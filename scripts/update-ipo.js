/**
 * DART 공모정보 > 청약달력(지분증권) (dsac008) 스크래퍼
 * - EUC-KR 안전 디코딩 (utf-8 fallback)
 * - start~end 범위의 모든 월을 순회
 * - "이벤트 링크(a)"를 기준으로 파싱 (DART 달력은 table이 아닐 수 있음)
 * - 기본: '기'(기타법인)만 남김 (=상장 제외)
 *
 * 사용:
 *   node scripts/update-ipo.js --start 2026-02-18 --end 2026-04-04
 *   node scripts/update-ipo.js   (기본: 오늘(KST)~+45일)
 */

import fs from "fs";
import path from "path";
import iconv from "iconv-lite";
import * as cheerio from "cheerio";

const DART_URL = "https://dart.fss.or.kr/dsac008/main.do";

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
  return fmt.format(new Date()); // "YYYY-MM-DD"
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

// ---------------- fetch & decode ----------------
function looksLikeCalendar(html) {
  const t = (html || "").replace(/\s+/g, "");
  const hasWeek = ["일", "월", "화", "수", "목", "금", "토"].every((d) => t.includes(d));
  // "청약 달력"은 페이지에 항상 있는 편이라 힌트로 사용
  const hasTitle = t.includes("청약달력") || t.includes("청약달력(지분증권)") || t.includes("청약달력(지분증권)");
  return hasWeek && (hasTitle || t.includes("공모정보"));
}

function tryDecode(buffer) {
  const euckr = iconv.decode(buffer, "euc-kr");
  if (looksLikeCalendar(euckr)) return euckr;
  const utf8 = buffer.toString("utf-8");
  return utf8;
}

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
  let htmlGet = tryDecode(bufGet);

  const infoGet = {
    method: "GET",
    url: urlGet,
    status: resGet.status,
    content_type: resGet.headers.get("content-type") || "",
    bytes: bufGet.length,
  };

  if (looksLikeCalendar(htmlGet)) {
    return { html: htmlGet, fetch_info: infoGet };
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
  const htmlPost = tryDecode(bufPost);

  const infoPost = {
    method: "POST",
    url: DART_URL,
    status: resPost.status,
    content_type: resPost.headers.get("content-type") || "",
    bytes: bufPost.length,
  };

  // POST가 성공하면 그걸 사용
  if (looksLikeCalendar(htmlPost)) {
    return { html: htmlPost, fetch_info: infoPost };
  }

  // 둘 다 애매하면 GET 결과 반환 (디버깅 위해)
  return { html: htmlGet, fetch_info: infoGet };
}

// ---------------- parse (anchor-driven) ----------------
const EVENT_RE = /^(유|코|넥|기)\s+(.+?)\s*\[(시작|종료)\]\s*$/;

function inferDayFromAnchor($, aEl) {
  // a 태그 주변(부모/조상)에서 1~31 숫자를 찾아서 day로 추정
  // 너무 위로 올라가면 연도/월 등 숫자 섞이니, "짧고(<=220) 연도(4자리) 없는" 블록에서만 채택
  let node = $(aEl);

  for (let up = 0; up < 10; up++) {
    node = node.parent();
    if (!node || node.length === 0) break;

    const cloned = node.clone();
    // 이벤트 링크 텍스트는 제거하고 day 후보만 남겨서 숫자 추출
    cloned.find("a").remove();
    const t = normalizeText(cloned.text());

    if (!t) continue;
    if (t.length > 220) continue;
    if (/\b\d{4}\b/.test(t)) continue; // 2026 같은 게 있으면 너무 상위 블록일 가능성 큼
    if (t.includes("년") || t.includes("월")) continue;

    const nums = [...t.matchAll(/\b(\d{1,2})\b/g)]
      .map((m) => Number(m[1]))
      .filter((n) => n >= 1 && n <= 31);

    if (nums.length === 0) continue;

    // 같은 숫자가 여러 번 있을 수 있으니 마지막 것을 선택 (대개 "02" 같은 날짜가 뒤에 붙음)
    return nums[nums.length - 1];
  }

  return null;
}

function parseMonth(html, y, m) {
  const $ = cheerio.load(html);

  const anchors = $("a")
    .toArray()
    .filter((el) => EVENT_RE.test(normalizeText($(el).text())));

  const events = [];

  for (const a of anchors) {
    const text = normalizeText($(a).text());
    const mm = text.match(EVENT_RE);
    if (!mm) continue;

    const marketShort = mm[1];
    const corpName = normalizeText(mm[2]);
    const mark = mm[3]; // 시작/종료
    const href = $(a).attr("href") || "";

    const day = inferDayFromAnchor($, a);
    if (!day) continue;

    const dateISO = toISODate(y, m, day);

    events.push({
      date: dateISO,
      market_short: marketShort,
      corp_name: corpName,
      mark,
      href,
    });
  }

  return { ok: true, events, anchors: anchors.length };
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

    const href = it.hrefs.find(Boolean) || "";
    items.push({
      corp_name: it.corp_name,
      market_short: it.market_short,
      market: it.market,
      sbd_start: it.sbd_start,
      sbd_end: it.sbd_end,
      href,
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
        parse: { ok: pm.ok, anchors: pm.anchors, events: pm.events.length },
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

  // keep only '기' (기타법인)
  const kept = [];
  let excluded_listed = 0;

  for (const it of merged) {
    if (it.market_short === "기") kept.push(it);
    else excluded_listed++;
  }

  const payload = {
    ok: true,
    source: "dart-dsac008(calendar euc-kr) + kind-listed-filter(=keep only '기')",
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
