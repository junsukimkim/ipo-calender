/**
 * DART 공모정보 > 청약달력(지분증권) (dsac008) 스크래퍼
 * - 서버 응답 charset(UTF-8/EUC-KR)에 맞춰 확실히 디코딩
 * - start~end 범위의 모든 월을 순회
 * - a 태그 텍스트에서 "유/코/넥/기 + 회사명 + [시작/종료]" 패턴 추출
 * - 기본: '기'(기타법인)만 남김 (=상장 제외)
 */

import fs from "fs";
import path from "path";
import iconv from "iconv-lite";
import * as cheerio from "cheerio";

const DART_URL = "https://dart.fss.or.kr/dsac008/main.do";
const EVENT_RE = /^(유|코|넥|기)\s+(.+?)\s*\[(시작|종료)\]\s*$/;
const EVENT_RE_LOOSE = /(유|코|넥|기)\s+(.+?)\s*\[(시작|종료)\]/g;

// ---------------- utils ----------------
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function pad2(n) {
  return String(n).padStart(2, "0");
}
function toISODate(y, m, d) {
  return `${y}-${pad2(m)}-${pad2(d)}`;
}
function normalizeText(s) {
  return (s || "").replace(/\s+/g, " ").trim();
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

// ---------------- decode ----------------
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
  // 기본은 UTF-8
  return buffer.toString("utf-8");
}

function scoreForEvents(html) {
  // 디코딩이 제대로 됐으면 "기/코/시작/종료" 패턴이 많이 잡힘
  const s = html || "";
  const matches = s.match(EVENT_RE_LOOSE);
  return matches ? matches.length : 0;
}

function pickBestDecodedHTML(buffer, contentType) {
  const headerCS = extractCharset(contentType);
  const primary = decodeByCharset(buffer, headerCS || "utf-8");
  const primaryScore = scoreForEvents(primary);

  // 반대쪽도 같이 디코딩해보고 이벤트 패턴이 더 잘 잡히는 쪽 선택
  const altCS =
    headerCS.includes("euc") || headerCS.includes("ksc") ? "utf-8" : "euc-kr";
  const alt = decodeByCharset(buffer, altCS);
  const altScore = scoreForEvents(alt);

  if (altScore > primaryScore) {
    return { html: alt, picked: altCS, score: altScore, altScore: primaryScore };
  }
  return { html: primary, picked: headerCS || "utf-8", score: primaryScore, altScore };
}

function looksLikeCalendar(html) {
  const t = (html || "").replace(/\s+/g, "");
  // 요일 + "청약" 같은 텍스트가 있으면 캘린더 본문일 확률 높음
  const hasWeek = ["일", "월", "화", "수", "목", "금", "토"].every((d) => t.includes(d));
  const hasKey = t.includes("청약") || t.includes("공모정보") || t.includes("청약달력");
  return hasWeek && hasKey;
}

// ---------------- fetch ----------------
async function fetchMonthHTML(y, m) {
  const urlGet = `${DART_URL}?selectYear=${y}&selectMonth=${pad2(m)}`;

  const headers = {
    "User-Agent": "Mozilla/5.0 (compatible; ipo-calender-bot/1.0; +https://github.com/)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": DART_URL,
  };

  // 1) GET
  const resGet = await fetch(urlGet, { method: "GET", headers });
  const bufGet = Buffer.from(await resGet.arrayBuffer());
  const ctGet = resGet.headers.get("content-type") || "";
  const decGet = pickBestDecodedHTML(bufGet, ctGet);

  const infoGet = {
    method: "GET",
    url: urlGet,
    status: resGet.status,
    content_type: ctGet,
    bytes: bufGet.length,
    decoded_charset: decGet.picked,
    event_score: decGet.score,
  };

  if (looksLikeCalendar(decGet.html)) {
    return { html: decGet.html, fetch_info: infoGet };
  }

  // 2) POST fallback
  const form = new URLSearchParams();
  form.set("selectYear", String(y));
  form.set("selectMonth", pad2(m));

  const resPost = await fetch(DART_URL, {
    method: "POST",
    headers: { ...headers, "Content-Type": "application/x-www-form-urlencoded" },
    body: form.toString(),
  });

  const bufPost = Buffer.from(await resPost.arrayBuffer());
  const ctPost = resPost.headers.get("content-type") || "";
  const decPost = pickBestDecodedHTML(bufPost, ctPost);

  const infoPost = {
    method: "POST",
    url: DART_URL,
    status: resPost.status,
    content_type: ctPost,
    bytes: bufPost.length,
    decoded_charset: decPost.picked,
    event_score: decPost.score,
  };

  return { html: decPost.html, fetch_info: infoPost };
}

// ---------------- parse ----------------
function inferDayFromAnchor($, aEl) {
  // a 주변 부모에서 day(1~31) 추정
  let node = $(aEl);
  for (let up = 0; up < 10; up++) {
    node = node.parent();
    if (!node || node.length === 0) break;

    const cloned = node.clone();
    cloned.find("a").remove();
    const t = normalizeText(cloned.text());

    if (!t) continue;
    if (t.length > 240) continue;
    if (/\b\d{4}\b/.test(t)) continue; // 2026 같은 연도가 섞이면 너무 상위
    if (t.includes("년") || t.includes("월")) continue;

    const nums = [...t.matchAll(/\b(\d{1,2})\b/g)]
      .map((m) => Number(m[1]))
      .filter((n) => n >= 1 && n <= 31);

    if (nums.length) return nums[nums.length - 1];
  }
  return null;
}

function parseMonth(html, y, m) {
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
    const text = normalizeText($(a).text());
    const mm = text.match(EVENT_RE);
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

  // 디버깅용: 매칭이 0이면 a 텍스트 샘플을 남김
  const sample = matchedAnchors.length
    ? []
    : allATexts.slice(0, 25);

  return {
    ok: true,
    anchors_total: allATexts.length,
    anchors_matched: matchedAnchors.length,
    events,
    sample_a_texts: sample,
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

// ---------------- main ----------------
async function main() {
  const args = parseArgs(process.argv);

  const start = typeof args.start === "string" ? args.start : kstTodayISO();
  const end = typeof args.end === "string" ? args.end : addDaysISO(kstTodayISO(), 45);
  const outPath = typeof args.out === "string" ? args.out : "docs/data/ipo.json";

  const months = monthsBetween(start, end);

  const allEvents = [];
  const debug = [];

  for (const { y, m } of months) {
    try {
      await sleep(900);

      const { html, fetch_info } = await fetchMonthHTML(y, m);
      const pm = parseMonth(html, y, m);

      debug.push({
        y,
        m,
        fetch: fetch_info,
        parse: {
          ok: pm.ok,
          anchors_total: pm.anchors_total,
          anchors_matched: pm.anchors_matched,
          events: pm.events.length,
          sample_a_texts: pm.sample_a_texts,
        },
      });

      allEvents.push(...pm.events);
    } catch (err) {
      debug.push({
        y,
        m,
        fetch: null,
        parse: { ok: false, reason: String(err?.message || err) },
      });
    }
  }

  const ranged = allEvents.filter((e) => withinRange(e.date, start, end));
  const merged = mergeEventsToItems(ranged);

  // keep only '기'
  const kept = [];
  let excluded_listed = 0;
  for (const it of merged) {
    if (it.market_short === "기") kept.push(it);
    else excluded_listed++;
  }

  const payload = {
    ok: true,
    source: "dart-dsac008(calendar charset-aware) + kind-listed-filter(=keep only '기')",
    range: { start, end },
    last_updated_kst: kstTodayISO(),
    count: kept.length,
    excluded_listed,
    items: kept,
    _debug: debug,
  };

  const absOut = path.resolve(outPath);
  fs.mkdirSync(path.dirname(absOut), { recursive: true });
  fs.writeFileSync(absOut, JSON.stringify(payload, null, 2), "utf-8");

  console.log("[OK] wrote:", outPath);
  console.log("[OK] months:", months.map((x) => `${x.y}-${pad2(x.m)}`).join(", "));
  console.log("[OK] total events:", allEvents.length);
  console.log("[OK] ranged events:", ranged.length);
  console.log("[OK] kept:", kept.length, "excluded_listed:", excluded_listed);
}

main().catch((e) => {
  console.error("[FATAL]", e);
  process.exit(1);
});
