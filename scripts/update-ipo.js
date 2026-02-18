/**
 * DART 공모정보 > 청약 달력(지분증권) (dsac008)
 * ✅ 핵심: 캘린더만으로는 "공모주(IPO) vs 유상증자"가 구분되지 않음
 *    -> 각 항목의 rcpNo로 "증권신고서(지분증권)" 원문(viewer.do)을 가져와서
 *       유상증자/주주배정 등 키워드가 있으면 제외.
 *
 * 사용 예:
 *  - IPO만:
 *      node scripts/update-ipo.js --start 2026-03-01 --end 2026-03-31 --mode ipo --out docs/data/ipo.json
 *  - 유상증자만 빼고(애매한 건 포함):
 *      node scripts/update-ipo.js --start 2026-03-01 --end 2026-03-31 --mode exrights --out docs/data/ipo.json
 *  - 전부(필터 없음):
 *      node scripts/update-ipo.js --start 2026-03-01 --end 2026-03-31 --mode all --out docs/data/ipo.json
 *
 * 참고:
 *  - DART 원문은 dsaf001에서 viewDoc(...) 파라미터를 뽑아 /report/viewer.do 로 접근 가능. (일반적으로 알려진 구조) 
 */

import fs from "fs";
import path from "path";
import iconv from "iconv-lite";
import * as cheerio from "cheerio";

const DART_CAL_URL = "https://dart.fss.or.kr/dsac008/main.do";
const DART_DSAF_URL = "https://dart.fss.or.kr/dsaf001/main.do";
const DART_VIEWER_BASE = "https://dart.fss.or.kr/report/viewer.do";

const EVENT_RE = /^(유|코|넥|기)\s*([^[]+?)\s*\[\s*(시작|종료)\s*\]\s*$/;
const EVENT_RE_LOOSE = /(유|코|넥|기)\s*([^[]+?)\s*\[\s*(시작|종료)\s*\]/g;

// ---- “유상증자” 판단 키워드 (있으면 rights 로 본다)
const RIGHTS_KEYWORDS = [
  "유상증자",
  "주주배정",
  "실권주",
  "신주인수권",
  "제3자배정",
  "제3자 배정",
  "일반공모(유상증자)",
  "주주우선공모",
];

// ---- “IPO(공모주/신규상장)” 판단 키워드 (있으면 ipo 로 본다)
const IPO_KEYWORDS = [
  "신규상장",
  "상장예정",
  "상장 예정",
  "코스닥시장 상장",
  "유가증권시장 상장",
  "상장심사",
  "예비상장",
  "대표주관회사",
  "공모가",
  "기관투자자 수요예측",
];

// ---------------- utils ----------------
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
function pad2(n) { return String(n).padStart(2, "0"); }
function toISODate(y, m, d) { return `${y}-${pad2(m)}-${pad2(d)}`; }
function normalizeText(s) {
  return (s || "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[++i] : true;
      args[k] = v;
    }
  }
  return args;
}
function kstTodayISO() {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Seoul",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return fmt.format(new Date());
}
function addDaysISO(iso, days) {
  const [y, m, d] = iso.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + days);
  return toISODate(dt.getUTCFullYear(), dt.getUTCMonth() + 1, dt.getUTCDate());
}
function monthsBetween(startISO, endISO) {
  const [sy, sm] = startISO.split("-").map(Number);
  const [ey, em] = endISO.split("-").map(Number);
  const cur = new Date(Date.UTC(sy, sm - 1, 1));
  const last = new Date(Date.UTC(ey, em - 1, 1));
  const out = [];
  while (cur <= last) {
    out.push({ y: cur.getUTCFullYear(), m: cur.getUTCMonth() + 1 });
    cur.setUTCMonth(cur.getUTCMonth() + 1);
  }
  return out;
}
function withinRange(dateISO, startISO, endISO) {
  return dateISO >= startISO && dateISO <= endISO;
}
function marketFromShort(short) {
  if (short === "유") return "KOSPI";
  if (short === "코") return "KOSDAQ";
  if (short === "넥") return "KONEX";
  if (short === "기") return "ETC";
  return "UNKNOWN";
}
function extractRcpNo(href) {
  const m = String(href || "").match(/rcpNo=(\d{14})/);
  return m ? m[1] : "";
}

// ---------------- charset decode ----------------
function extractCharset(contentType) {
  const ct = (contentType || "").toLowerCase();
  const m = ct.match(/charset\s*=\s*([a-z0-9_\-]+)/i);
  return m ? m[1].toLowerCase() : "";
}
function decodeByCharset(buffer, charset) {
  const cs = (charset || "").toLowerCase();
  if (cs.includes("euc-kr") || cs.includes("ks_c_5601") || cs.includes("ksc5601")) {
    return iconv.decode(buffer, "euc-kr");
  }
  return buffer.toString("utf-8");
}
function scoreForEvents(html) {
  const matches = (html || "").match(EVENT_RE_LOOSE);
  return matches ? matches.length : 0;
}
function pickBestDecodedHTML(buffer, contentType) {
  const headerCS = extractCharset(contentType);
  const primary = decodeByCharset(buffer, headerCS || "utf-8");
  const primaryScore = scoreForEvents(primary);

  const altCS =
    headerCS.includes("euc") || headerCS.includes("ksc") ? "utf-8" : "euc-kr";
  const alt = decodeByCharset(buffer, altCS);
  const altScore = scoreForEvents(alt);

  if (altScore > primaryScore) {
    return { html: alt, picked: altCS, score: altScore, altScore: primaryScore };
  }
  return { html: primary, picked: headerCS || "utf-8", score: primaryScore, altScore };
}

// ---------------- cookie jar (캘린더 월 이동 안정화) ----------------
function getSetCookieStrings(headers) {
  if (typeof headers.getSetCookie === "function") return headers.getSetCookie();
  const sc = headers.get("set-cookie");
  return sc ? [sc] : [];
}
function parseCookiePair(setCookieLine) {
  const first = (setCookieLine || "").split(";")[0]?.trim();
  if (!first) return null;
  const eq = first.indexOf("=");
  if (eq <= 0) return null;
  return { name: first.slice(0, eq).trim(), value: first.slice(eq + 1).trim() };
}
class CookieJar {
  constructor() { this.map = new Map(); }
  absorbFromResponse(res) {
    const arr = getSetCookieStrings(res.headers);
    for (const line of arr) {
      const kv = parseCookiePair(line);
      if (kv) this.map.set(kv.name, kv.value);
    }
  }
  headerValue() {
    if (this.map.size === 0) return "";
    return [...this.map.entries()].map(([k, v]) => `${k}=${v}`).join("; ");
  }
}

// ---------------- fetch calendar month ----------------
async function fetchCalendarMonthHTML(y, m) {
  const headers = {
    "User-Agent": "Mozilla/5.0 (compatible; ipo-calender-bot/1.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": DART_CAL_URL,
  };

  const jar = new CookieJar();

  // 1) bootstrap GET
  const res0 = await fetch(DART_CAL_URL, { method: "GET", headers });
  jar.absorbFromResponse(res0);
  const buf0 = Buffer.from(await res0.arrayBuffer());
  const ct0 = res0.headers.get("content-type") || "";
  const dec0 = pickBestDecodedHTML(buf0, ct0);

  // 2) month change POST
  const form = new URLSearchParams();
  form.set("selectYear", String(y));
  form.set("selectMonth", pad2(m));
  form.set("search", "검색");

  const cookie = jar.headerValue();
  const res1 = await fetch(DART_CAL_URL, {
    method: "POST",
    headers: {
      ...headers,
      "Content-Type": "application/x-www-form-urlencoded",
      ...(cookie ? { Cookie: cookie } : {}),
    },
    body: form.toString(),
  });

  const buf1 = Buffer.from(await res1.arrayBuffer());
  const ct1 = res1.headers.get("content-type") || "";
  const dec1 = pickBestDecodedHTML(buf1, ct1);

  return {
    html: dec1.html,
    fetch_info: {
      method: "POST",
      status: res1.status,
      content_type: ct1,
      bytes: buf1.length,
      decoded_charset: dec1.picked,
      event_score: dec1.score,
      cookie_names: [...jar.map.keys()],
      requested: { y, m },
      bootstrap: { status: res0.status, bytes: buf0.length, decoded_charset: dec0.picked, event_score: dec0.score },
    }
  };
}

// ---------------- parse day ----------------
function inferDayFromAnchor($, aEl) {
  let node = $(aEl);
  for (let up = 0; up < 6; up++) {
    node = node.parent();
    if (!node || node.length === 0) break;

    const cloned = node.clone();
    cloned.find("a").remove();
    const t = normalizeText(cloned.text());
    if (!t) continue;
    if (t.length > 160) continue;

    const nums = [...t.matchAll(/\b(\d{1,2})\b/g)]
      .map((m) => Number(m[1]))
      .filter((n) => n >= 1 && n <= 31);

    if (nums.length) return nums[0];
  }
  return null;
}

// ---------------- parse month events ----------------
function parseCalendarMonth(html, y, m) {
  const $ = cheerio.load(html);

  const allATexts = $("a")
    .toArray()
    .map((el) => normalizeText($(el).text()))
    .filter(Boolean);

  const matchedAnchors = $("a")
    .toArray()
    .filter((el) => EVENT_RE.test(normalizeText($(el).text())));

  const events = [];
  for (const a of matchedAnchors) {
    const raw = normalizeText($(a).text());
    const mm = raw.match(EVENT_RE);
    if (!mm) continue;

    const marketShort = mm[1];
    const corpName = normalizeText(mm[2]);
    const mark = mm[3];
    const href = $(a).attr("href") || "";

    const day = inferDayFromAnchor($, a);
    if (!day) continue;

    events.push({
      date: toISODate(y, m, day),
      market_short: marketShort,
      corp_name: corpName,
      mark,
      href,
    });
  }

  // month 단위 중복 제거
  const dedup = new Map();
  for (const e of events) {
    const k = `${e.date}||${e.market_short}||${e.corp_name}||${e.mark}||${e.href}`;
    dedup.set(k, e);
  }

  return {
    ok: true,
    anchors_total: allATexts.length,
    anchors_matched: matchedAnchors.length,
    events: [...dedup.values()],
    sample_matched_texts: matchedAnchors.slice(0, 15).map((a) => normalizeText($(a).text())),
  };
}

// ---------------- merge to items ----------------
function mergeEventsToItems(events) {
  const map = new Map();

  for (const e of events) {
    const key = `${e.market_short}||${e.corp_name}`;
    if (!map.has(key)) {
      map.set(key, {
        corp_name: e.corp_name,
        market_short: e.market_short,
        market: marketFromShort(e.market_short),
        sbd_start: null,
        sbd_end: null,
        hrefs: [],
      });
    }
    const it = map.get(key);
    if (e.href) it.hrefs.push(e.href);

    if (e.mark === "시작") {
      if (!it.sbd_start || e.date < it.sbd_start) it.sbd_start = e.date;
    } else if (e.mark === "종료") {
      if (!it.sbd_end || e.date > it.sbd_end) it.sbd_end = e.date;
    }
  }

  const items = [];
  for (const it of map.values()) {
    if (it.sbd_start && !it.sbd_end) it.sbd_end = it.sbd_start;
    if (!it.sbd_start && it.sbd_end) it.sbd_start = it.sbd_end;

    // 대표 href는 rcpNo 있는 걸 우선
    const href = it.hrefs.find((h) => /rcpNo=\d{14}/.test(h)) || it.hrefs.find(Boolean) || "";

    items.push({
      corp_name: it.corp_name,
      market_short: it.market_short,
      market: it.market,
      sbd_start: it.sbd_start,
      sbd_end: it.sbd_end,
      href,
      href_abs: href ? new URL(href, "https://dart.fss.or.kr").toString() : "",
    });
  }

  items.sort((a, b) => {
    if ((a.sbd_start || "") !== (b.sbd_start || "")) return (a.sbd_start || "").localeCompare(b.sbd_start || "");
    return (a.corp_name || "").localeCompare(b.corp_name || "");
  });

  return items;
}

// ---------------- classify (IPO vs Rights) by fetching filing text ----------------
function includesAny(text, keywords) {
  const t = text || "";
  return keywords.some((k) => t.includes(k));
}

/**
 * dsaf001(main.do?rcpNo=...) HTML에서
 * "증권신고서(지분증권)" 항목의 javascript:viewDoc(...) 파라미터를 추출
 */
function extractViewerParamsFromDsaf(html) {
  // 1) a[href^="javascript: viewDoc("] 중 텍스트에 '증권신고서(지분증권)'가 있는 걸 우선
  const $ = cheerio.load(html);
  const candidates = [];

  $("a").each((_, el) => {
    const href = String($(el).attr("href") || "");
    const text = normalizeText($(el).text());
    if (!href.includes("viewDoc(")) return;

    candidates.push({ href, text });
  });

  // 우선순위: 본문 '증권신고서(지분증권)' > 그 외 viewDoc
  const picked =
    candidates.find((c) => c.text.includes("증권신고서(지분증권)")) ||
    candidates.find((c) => c.text.includes("증권신고서")) ||
    candidates[0];

  if (!picked) return null;

  // viewDoc('rcpNo', 'dcmNo', 'eleId', 'offset', 'length', 'dtd')
  // 숫자/NULL/따옴표 섞이는 케이스 대응
  const m = picked.href.match(
    /viewDoc\(\s*'?(?<rcpNo>\d{14})'?\s*,\s*'?(?<dcmNo>\d+)'?\s*,\s*(?<eleId>null|\d+)?\s*,\s*(?<offset>null|\d+)?\s*,\s*(?<length>null|\d+)?\s*,\s*'?(?<dtd>[^'()\s]+)'?\s*\)/
  );
  if (!m || !m.groups) return null;

  const eleId = m.groups.eleId && m.groups.eleId !== "null" ? m.groups.eleId : "0";
  const offset = "0";
  const length = "0";
  const dtd = m.groups.dtd || "dart3.xsd";

  return {
    rcpNo: m.groups.rcpNo,
    dcmNo: m.groups.dcmNo,
    eleId,
    offset,
    length,
    dtd,
    picked_text: picked.text,
  };
}

async function fetchTextHTML(url, headers) {
  const res = await fetch(url, { method: "GET", headers });
  const buf = Buffer.from(await res.arrayBuffer());
  const ct = res.headers.get("content-type") || "";
  const html = decodeByCharset(buf, extractCharset(ct) || "utf-8");
  return { res, html, bytes: buf.length, content_type: ct };
}

async function classifyRcpNo(rcpNo) {
  const headers = {
    "User-Agent": "Mozilla/5.0 (compatible; ipo-calender-bot/1.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": DART_DSAF_URL,
  };

  // 1) dsaf001 (메타 페이지)
  const dsafUrl = `${DART_DSAF_URL}?rcpNo=${rcpNo}`;
  const dsaf = await fetchTextHTML(dsafUrl, headers);

  const params = extractViewerParamsFromDsaf(dsaf.html);
  if (!params) {
    return { type: "unknown", reason: "viewDoc params not found", dsaf_url: dsafUrl };
  }

  // 2) viewer 원문(HTML)
  const viewerUrl =
    `${DART_VIEWER_BASE}?rcpNo=${params.rcpNo}` +
    `&dcmNo=${params.dcmNo}` +
    `&eleId=${params.eleId}` +
    `&offset=${params.offset}` +
    `&length=${params.length}` +
    `&dtd=${encodeURIComponent(params.dtd)}`;

  const viewer = await fetchTextHTML(viewerUrl, headers);
  const text = normalizeText(cheerio.load(viewer.html).text());

  // 3) 분류
  const isRights = includesAny(text, RIGHTS_KEYWORDS);
  const isIpo = includesAny(text, IPO_KEYWORDS);

  if (isRights) return { type: "rights", reason: "matched rights keywords", viewer_url: viewerUrl, picked: params.picked_text };
  if (isIpo) return { type: "ipo", reason: "matched ipo keywords", viewer_url: viewerUrl, picked: params.picked_text };

  // 애매하면 unknown
  return { type: "unknown", reason: "no decisive keywords", viewer_url: viewerUrl, picked: params.picked_text };
}

// ---------------- main ----------------
async function main() {
  const args = parseArgs(process.argv);

  const start = typeof args.start === "string" ? args.start : kstTodayISO();
  const end = typeof args.end === "string" ? args.end : addDaysISO(kstTodayISO(), 45);
  const outPath = typeof args.out === "string" ? args.out : "docs/data/ipo.json";
  const mode = typeof args.mode === "string" ? String(args.mode).toLowerCase() : "ipo"; // ipo | exrights | all

  const months = monthsBetween(start, end);

  const allEvents = [];
  const debug = [];

  for (const { y, m } of months) {
    try {
      await sleep(900);

      const { html, fetch_info } = await fetchCalendarMonthHTML(y, m);
      const pm = parseCalendarMonth(html, y, m);

      debug.push({
        y, m,
        fetch: fetch_info,
        parse: {
          ok: pm.ok,
          anchors_total: pm.anchors_total,
          anchors_matched: pm.anchors_matched,
          events: pm.events.length,
          sample_matched_texts: pm.sample_matched_texts,
        },
      });

      allEvents.push(...pm.events);
    } catch (err) {
      debug.push({
        y, m,
        fetch: null,
        parse: { ok: false, reason: String(err?.message || err) },
      });
    }
  }

  // 1) 날짜 범위 필터
  const rangedEvents = allEvents.filter((e) => withinRange(e.date, start, end));

  // 2) 아이템 병합
  const merged = mergeEventsToItems(rangedEvents);

  // 3) rcpNo 기반 분류 + mode 필터
  const classified = [];
  const classify_debug = [];

  for (const it of merged) {
    const rcpNo = extractRcpNo(it.href);
    if (!rcpNo) {
      // rcpNo 없으면 애매: mode=all일 때만 살림
      if (mode === "all") classified.push({ ...it, offer_type: "unknown", offer_reason: "no rcpNo" });
      continue;
    }

    await sleep(400); // DART 부담 줄이기

    let cls;
    try {
      cls = await classifyRcpNo(rcpNo);
    } catch (e) {
      cls = { type: "unknown", reason: `classify error: ${String(e?.message || e)}` };
    }

    classify_debug.push({ corp_name: it.corp_name, rcpNo, ...cls });

    const itemOut = {
      ...it,
      rcpNo,
      offer_type: cls.type,
      offer_reason: cls.reason,
      viewer_url: cls.viewer_url || "",
    };

    if (mode === "all") {
      classified.push(itemOut);
    } else if (mode === "exrights") {
      if (cls.type !== "rights") classified.push(itemOut);
    } else {
      // mode === "ipo" (기본): IPO만
      if (cls.type === "ipo") classified.push(itemOut);
    }
  }

  const payload = {
    ok: true,
    source: "dart-dsac008(calendar) + classify-by-filing(viewer.do) + mode-filter",
    range: { start, end },
    mode,
    last_updated_kst: kstTodayISO(),
    count: classified.length,
    items: classified,
    _debug: debug,
    _classify_debug: classify_debug.slice(0, 80), // 너무 길어지는 거 방지
  };

  const absOut = path.resolve(outPath);
  fs.mkdirSync(path.dirname(absOut), { recursive: true });
  fs.writeFileSync(absOut, JSON.stringify(payload, null, 2), "utf-8");

  console.log("[OK] wrote:", outPath);
  console.log("[OK] months:", months.map((x) => `${x.y}-${pad2(x.m)}`).join(", "));
  console.log("[OK] mode:", mode);
  console.log("[OK] total events:", allEvents.length);
  console.log("[OK] ranged events:", rangedEvents.length);
  console.log("[OK] merged items:", merged.length);
  console.log("[OK] output items:", classified.length);
}

main().catch((e) => {
  console.error("[FATAL]", e);
  process.exit(1);
});
