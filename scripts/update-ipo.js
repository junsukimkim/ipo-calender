/**
 * DART 공모정보 > 청약 달력(지분증권) (dsac008) 스크래퍼
 *
 * ✅ IMPORTANT FIX
 * - dsac008은 GET 쿼리(selectYear/selectMonth)를 무시하고 "현재 달"을 주는 경우가 있음.
 * - 따라서 "쿠키 확보(GET) -> POST로 달 변경"을 기본으로 수행.
 *
 * 사용 예)
 *   node scripts/update-ipo.js --start 2026-03-01 --end 2026-03-31 --markets all --out docs/data/ipo.json
 *
 * markets:
 *   all (기본) / 유 / 코 / 넥 / 기 / 또는 "코,유" 같이 콤마
 */

import fs from "fs";
import path from "path";
import iconv from "iconv-lite";
import * as cheerio from "cheerio";

const DART_URL = "https://dart.fss.or.kr/dsac008/main.do";

// "코 아이씨에이치 [시작]" / "기케이뱅크[종료]" 둘 다 커버
const EVENT_RE = /^(유|코|넥|기)\s*([^[]+?)\s*\[\s*(시작|종료)\s*\]\s*$/;
const EVENT_RE_LOOSE = /(유|코|넥|기)\s*([^[]+?)\s*\[\s*(시작|종료)\s*\]/g;

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

// ---------------- cookie jar ----------------
function getSetCookieStrings(headers) {
  // Node 20+ (undici)에는 getSetCookie()가 있음
  if (typeof headers.getSetCookie === "function") {
    return headers.getSetCookie();
  }
  // fallback: 단일 set-cookie만 오는 경우
  const sc = headers.get("set-cookie");
  return sc ? [sc] : [];
}
function parseCookiePair(setCookieLine) {
  // "NAME=VALUE; Path=/; ..." 중 첫 부분만
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

// ---------------- fetch month ----------------
async function fetchMonthHTML(y, m) {
  const headers = {
    "User-Agent": "Mozilla/5.0 (compatible; ipo-calender-bot/1.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": DART_URL,
  };

  const jar = new CookieJar();

  // 1) GET으로 쿠키/세션 확보
  const res0 = await fetch(DART_URL, { method: "GET", headers });
  jar.absorbFromResponse(res0);
  const buf0 = Buffer.from(await res0.arrayBuffer());
  const ct0 = res0.headers.get("content-type") || "";
  const dec0 = pickBestDecodedHTML(buf0, ct0);

  // 2) POST로 달 변경
  const form = new URLSearchParams();
  form.set("selectYear", String(y));
  form.set("selectMonth", pad2(m));
  // (사이트에 따라 '검색' 버튼과 연동된 필드가 있을 수 있어 함께 넣어둠)
  form.set("search", "검색");

  const cookie = jar.headerValue();
  const res1 = await fetch(DART_URL, {
    method: "POST",
    headers: {
      ...headers,
      "Content-Type": "application/x-www-form-urlencoded",
      ...(cookie ? { Cookie: cookie } : {}),
    },
    body: form.toString(),
  });

  jar.absorbFromResponse(res1);

  const buf1 = Buffer.from(await res1.arrayBuffer());
  const ct1 = res1.headers.get("content-type") || "";
  const dec1 = pickBestDecodedHTML(buf1, ct1);

  return {
    html: dec1.html,
    fetch_info: {
      bootstrap: { status: res0.status, bytes: buf0.length, decoded_charset: dec0.picked, event_score: dec0.score },
      method: "POST",
      url: DART_URL,
      status: res1.status,
      content_type: ct1,
      bytes: buf1.length,
      decoded_charset: dec1.picked,
      event_score: dec1.score,
      cookie_names: [...jar.map.keys()],
      requested: { y, m },
    }
  };
}

// ---------------- parse month selection sanity ----------------
function detectSelectedYearMonth($) {
  // 가장 정석: name="selectYear"/"selectMonth"
  const y1 =
    $("select[name='selectYear'] option[selected]").attr("value") ||
    $("select[name='selectYear']").val();
  const m1 =
    $("select[name='selectMonth'] option[selected]").attr("value") ||
    $("select[name='selectMonth']").val();

  const y = y1 ? Number(String(y1).replace(/\D/g, "")) : null;
  const m = m1 ? Number(String(m1).replace(/\D/g, "")) : null;

  // fallback: 모든 select 훑어서 "년도/월" 후보 찾기
  if (y && m) return { y, m };

  let best = { y: null, m: null, score: 0 };
  $("select").each((_, el) => {
    const opts = $(el).find("option");
    const texts = opts.toArray().map(o => normalizeText($(o).text()));
    const hasYearLike = texts.some(t => t.includes("년"));
    const hasMonthLike = texts.some(t => t.includes("월"));
    const s = (hasYearLike ? 1 : 0) + (hasMonthLike ? 1 : 0);
    if (s > best.score) {
      // selected가 있으면 우선
      const sel = opts.filter((i, o) => $(o).attr("selected"));
      if (sel.length) {
        const tx = normalizeText($(sel[0]).text());
        const val = $(sel[0]).attr("value") || "";
        // year/month 둘 다는 못 잡아도 null로 둠
        if (tx.includes("년")) best.y = Number(tx.replace(/\D/g, "")) || best.y;
        if (tx.includes("월")) best.m = Number(tx.replace(/\D/g, "")) || best.m;
        if (val && val.length <= 4 && /^\d+$/.test(val)) {
          // month select일 가능성
          const n = Number(val);
          if (n >= 1 && n <= 12) best.m = n;
          if (n >= 2000) best.y = n;
        }
      }
      best.score = s;
    }
  });

  return { y: best.y, m: best.m };
}

// ---------------- parse day ----------------
function inferDayFromAnchor($, aEl) {
  // anchor 주변 부모에서 "1~31" 숫자 찾기 (가까운 영역 우선)
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
function parseMonth(html, y, m) {
  const $ = cheerio.load(html);

  const selected = detectSelectedYearMonth($);

  const allATexts = $("a")
    .toArray()
    .map((el) => normalizeText($(el).text()))
    .filter(Boolean);

  const eventish = allATexts
    .filter((t) => t.includes("시작") || t.includes("종료") || t.includes("["))
    .slice(0, 50);

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

  // ✅ 중복 방지(혹시 페이지가 같은 달을 반복 반환해도 피해 최소화)
  const dedup = new Map();
  for (const e of events) {
    const k = `${e.date}||${e.market_short}||${e.corp_name}||${e.mark}`;
    dedup.set(k, e);
  }

  return {
    ok: true,
    selected, // 서버가 실제로 표시한 년/월 (잡히면 여기로 검증 가능)
    anchors_total: allATexts.length,
    anchors_eventish: eventish.length,
    anchors_matched: matchedAnchors.length,
    events: [...dedup.values()],
    sample_eventish_texts: eventish,
  };
}

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

    items.push({
      corp_name: it.corp_name,
      market_short: it.market_short,
      market: it.market,
      sbd_start: it.sbd_start,
      sbd_end: it.sbd_end,
      href: it.hrefs.find(Boolean) || "",
    });
  }

  items.sort((a, b) => {
    if ((a.sbd_start || "") !== (b.sbd_start || "")) {
      return (a.sbd_start || "").localeCompare(b.sbd_start || "");
    }
    return (a.corp_name || "").localeCompare(b.corp_name || "");
  });

  return items;
}

function parseMarketsArg(s) {
  const v = String(s || "all").trim();
  if (!v || v === "all") return new Set(["유", "코", "넥", "기"]);
  const parts = v.split(",").map(x => x.trim()).filter(Boolean);
  const ok = new Set(["유", "코", "넥", "기"]);
  const out = new Set();
  for (const p of parts) if (ok.has(p)) out.add(p);
  return out.size ? out : new Set(["유", "코", "넥", "기"]);
}

// ---------------- main ----------------
async function main() {
  const args = parseArgs(process.argv);

  const start = typeof args.start === "string" ? args.start : kstTodayISO();
  const end = typeof args.end === "string" ? args.end : addDaysISO(kstTodayISO(), 45);
  const outPath = typeof args.out === "string" ? args.out : "docs/data/ipo.json";
  const markets = parseMarketsArg(args.markets);

  const months = monthsBetween(start, end);

  const allEvents = [];
  const debug = [];

  for (const { y, m } of months) {
    try {
      // 과도 요청 방지
      await sleep(900);

      const { html, fetch_info } = await fetchMonthHTML(y, m);
      const pm = parseMonth(html, y, m);

      // ✅ 서버가 실제로 어떤 달을 보여줬는지 기록 (pm.selected가 잡히면 검증에 도움)
      debug.push({
        y, m,
        fetch: fetch_info,
        parse: {
          ok: pm.ok,
          server_selected: pm.selected,
          anchors_total: pm.anchors_total,
          anchors_eventish: pm.anchors_eventish,
          anchors_matched: pm.anchors_matched,
          events: pm.events.length,
          sample_eventish_texts: pm.sample_eventish_texts.slice(0, 25),
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

  // 범위 필터 + market 필터
  const ranged = allEvents
    .filter((e) => withinRange(e.date, start, end))
    .filter((e) => markets.has(e.market_short));

  const merged = mergeEventsToItems(ranged);

  // href를 절대 URL로도 쓸 수 있게 하나 더 제공
  const items = merged.map(it => ({
    ...it,
    href_abs: it.href ? new URL(it.href, "https://dart.fss.or.kr").toString() : ""
  }));

  const payload = {
    ok: true,
    source: "dart-dsac008(calendar POST+cookie) + markets-filter",
    range: { start, end },
    markets: [...markets].join(","),
    last_updated_kst: kstTodayISO(),
    count: items.length,
    items,
    _debug: debug,
  };

  const absOut = path.resolve(outPath);
  fs.mkdirSync(path.dirname(absOut), { recursive: true });
  fs.writeFileSync(absOut, JSON.stringify(payload, null, 2), "utf-8");

  console.log("[OK] wrote:", outPath);
  console.log("[OK] months:", months.map((x) => `${x.y}-${pad2(x.m)}`).join(", "));
  console.log("[OK] markets:", [...markets].join(","));
  console.log("[OK] total events:", allEvents.length);
  console.log("[OK] ranged events:", ranged.length);
  console.log("[OK] items:", items.length);
}

main().catch((e) => {
  console.error("[FATAL]", e);
  process.exit(1);
});
