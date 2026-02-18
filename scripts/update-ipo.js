/**
 * DART 공모정보 > 청약달력(지분증권) (dsac008) 스크래퍼
 * - EUC-KR 안전 디코딩 (utf-8 fallback)
 * - start~end 범위의 모든 월을 순회해서 캘린더 파싱
 * - 기본: '기'(기타법인)만 남기고(=상장 제외), docs/data/ipo.json 생성
 *
 * 사용 예)
 *   node scripts/update-ipo.js --start 2026-02-18 --end 2026-03-31
 *   node scripts/update-ipo.js  (기본: 오늘(KST)~+45일)
 */

import fs from "fs";
import path from "path";
import iconv from "iconv-lite";
import * as cheerio from "cheerio";

const DART_URL = "https://dart.fss.or.kr/dsac008/main.do";

// ---------- util ----------
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function pad2(n) {
  return String(n).padStart(2, "0");
}

function toISODate(y, m, d) {
  return `${y}-${pad2(m)}-${pad2(d)}`;
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
  // Intl로 KST 기준 YYYY-MM-DD 만들기
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Seoul",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  });
  return fmt.format(new Date()); // "2026-02-18" 같은 형태
}

function addDaysISO(iso, days) {
  const [y, m, d] = iso.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + days);
  const yy = dt.getUTCFullYear();
  const mm = dt.getUTCMonth() + 1;
  const dd = dt.getUTCDate();
  return toISODate(yy, mm, dd);
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

function normalizeText(s) {
  return (s || "").replace(/\s+/g, " ").trim();
}

function marketFromShort(short) {
  if (short === "유") return "KOSPI";
  if (short === "코") return "KOSDAQ";
  if (short === "넥") return "KONEX";
  if (short === "기") return "ETC";
  return "UNKNOWN";
}

function withinRange(dateISO, startISO, endISO) {
  return dateISO >= startISO && dateISO <= endISO;
}

// ---------- fetch & decode ----------
async function fetchMonthHTML(y, m) {
  // 1) GET with query params
  const urlGet = `${DART_URL}?selectYear=${y}&selectMonth=${pad2(m)}`;

  const headers = {
    "User-Agent":
      "Mozilla/5.0 (compatible; ipo-calender-bot/1.0; +https://github.com/)",
    "Accept":
      "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.7,en;q=0.6",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache"
  };

  const resGet = await fetch(urlGet, { method: "GET", headers });
  const bufGet = Buffer.from(await resGet.arrayBuffer());
  let html = tryDecode(bufGet);

  // 2) fallback: POST form
  // (GET이 막히거나 파라미터명이 다른 경우 대비)
  if (!looksLikeCalendar(html)) {
    const form = new URLSearchParams();
    form.set("selectYear", String(y));
    form.set("selectMonth", pad2(m));

    const resPost = await fetch(DART_URL, {
      method: "POST",
      headers: {
        ...headers,
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body: form.toString()
    });

    const bufPost = Buffer.from(await resPost.arrayBuffer());
    const htmlPost = tryDecode(bufPost);
    if (looksLikeCalendar(htmlPost)) return htmlPost;

    // 마지막: GET 결과라도 반환 (디버깅용)
    return html;
  }

  return html;
}

function tryDecode(buffer) {
  // DART 페이지는 종종 EUC-KR
  // EUC-KR로 먼저 디코딩 → 달력 헤더(일/월/화...)가 안 보이면 UTF-8 fallback
  const euckr = iconv.decode(buffer, "euc-kr");
  if (looksLikeCalendar(euckr)) return euckr;

  const utf8 = buffer.toString("utf-8");
  return utf8;
}

function looksLikeCalendar(html) {
  // 달력 요일 헤더가 있으면 거의 확정
  const t = (html || "").replace(/\s+/g, "");
  return (
    t.includes(">일<") &&
    t.includes(">월<") &&
    t.includes(">화<") &&
    t.includes(">수<") &&
    t.includes(">목<") &&
    t.includes(">금<") &&
    t.includes(">토<")
  );
}

// ---------- parse ----------
function findCalendarTable($) {
  // 요일(th)이 있는 table을 찾아서 반환
  const tables = $("table").toArray();
  for (const el of tables) {
    const th = $(el)
      .find("th")
      .toArray()
      .map((x) => normalizeText($(x).text()));
    const joined = th.join("|");
    if (
      joined.includes("일") &&
      joined.includes("월") &&
      joined.includes("화") &&
      joined.includes("수") &&
      joined.includes("목") &&
      joined.includes("금") &&
      joined.includes("토")
    ) {
      return el;
    }
  }
  return null;
}

function parseMonth(html, y, m) {
  const $ = cheerio.load(html);
  const table = findCalendarTable($);
  if (!table) {
    return { ok: false, reason: "calendar table not found", events: [] };
  }

  const events = [];
  const tds = $(table).find("td").toArray();

  for (const td of tds) {
    const tdTextRaw = normalizeText($(td).text());
    if (!tdTextRaw) continue;

    // td의 첫 숫자를 day로 인식
    const dayMatch = tdTextRaw.match(/^(\d{1,2})\b/);
    if (!dayMatch) continue;

    const day = Number(dayMatch[1]);
    if (!(day >= 1 && day <= 31)) continue;

    const dateISO = toISODate(y, m, day);

    // td 안의 a 태그 텍스트를 전부 훑으면서 [시작]/[종료] 패턴을 수집
    const anchors = $(td).find("a").toArray();
    for (const a of anchors) {
      const text = normalizeText($(a).text());
      if (!text) continue;

      // 예: "기 케이뱅크 [시작]" / "코 라온피플 [종료]"
      const mm = text.match(/^(유|코|넥|기)\s+(.+?)\s*\[(시작|종료)\]\s*$/);
      if (!mm) continue;

      const marketShort = mm[1];
      const corpName = normalizeText(mm[2]);
      const kind = mm[3]; // 시작/종료
      const href = $(a).attr("href") || "";

      events.push({
        date: dateISO,
        market_short: marketShort,
        corp_name: corpName,
        mark: kind, // 시작/종료
        href
      });
    }
  }

  return { ok: true, events };
}

function mergeEventsToItems(events) {
  // corp + marketShort 단위로 start/end 합치기
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
        hrefs: []
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

  // start만 있고 end가 없으면 end=start로 보정
  const items = [];
  for (const it of map.values()) {
    if (it.sbd_start && !it.sbd_end) it.sbd_end = it.sbd_start;
    if (!it.sbd_start && it.sbd_end) it.sbd_start = it.sbd_end;

    // href는 너무 길어질 수 있으니 대표 1개만 남김
    const href = it.hrefs.find(Boolean) || "";
    items.push({
      corp_name: it.corp_name,
      market_short: it.market_short,
      market: it.market,
      sbd_start: it.sbd_start,
      sbd_end: it.sbd_end,
      href
    });
  }

  // 정렬: 시작일 → 회사명
  items.sort((a, b) => {
    if ((a.sbd_start || "") !== (b.sbd_start || "")) {
      return (a.sbd_start || "").localeCompare(b.sbd_start || "");
    }
    return (a.corp_name || "").localeCompare(b.corp_name || "");
  });

  return items;
}

// ---------- main ----------
async function main() {
  const args = parseArgs(process.argv);

  const start = typeof args.start === "string" ? args.start : kstTodayISO();
  const end =
    typeof args.end === "string" ? args.end : addDaysISO(kstTodayISO(), 45);

  const outPath =
    typeof args.out === "string" ? args.out : "docs/data/ipo.json";

  const months = monthsBetween(start, end);

  const allEvents = [];
  const debug = [];

  for (const { y, m } of months) {
    try {
      // 너무 빨리 때리면 차단/오류 가능성 ↑
      await sleep(800);

      const html = await fetchMonthHTML(y, m);

      const pm = parseMonth(html, y, m);
      if (!pm.ok) {
        debug.push({ y, m, ok: false, reason: pm.reason || "parse failed" });
        continue;
      }

      debug.push({ y, m, ok: true, events: pm.events.length });
      allEvents.push(...pm.events);
    } catch (err) {
      debug.push({
        y,
        m,
        ok: false,
        reason: String(err?.message || err)
      });
    }
  }

  // 기간 필터
  const ranged = allEvents.filter((e) => withinRange(e.date, start, end));

  // 병합
  const merged = mergeEventsToItems(ranged);

  // 상장 제외(= kind-listed-filter 역할을 최소 구현)
  // - DART 화면에서 유/코/넥은 보통 상장시장 구분이므로 제외하고 "기"만 남김
  const kept = [];
  let excluded_listed = 0;

  for (const it of merged) {
    if (it.market_short === "기") kept.push(it);
    else excluded_listed++;
  }

  // 결과 JSON
  const payload = {
    ok: true,
    source: "dart-dsac008(calendar euc-kr) + kind-listed-filter(=keep only '기')",
    range: { start, end },
    last_updated_kst: kstTodayISO(),
    count: kept.length,
    excluded_listed,
    items: kept,
    _debug: debug // 필요 없으면 지워도 됨 (프론트에서 무시)
  };

  // 디렉토리 보장
  const absOut = path.resolve(outPath);
  fs.mkdirSync(path.dirname(absOut), { recursive: true });

  fs.writeFileSync(absOut, JSON.stringify(payload, null, 2), "utf-8");

  console.log("[OK] wrote:", outPath);
  console.log("[OK] months:", months.map((x) => `${x.y}-${pad2(x.m)}`).join(", "));
  console.log("[OK] total events:", allEvents.length);
  console.log("[OK] ranged events:", ranged.length);
  console.log("[OK] items kept:", kept.length, "excluded_listed:", excluded_listed);

  // 만약 0이면, Actions 로그에서 _debug 를 보고 어디서 막혔는지 바로 확인 가능
}

main().catch((e) => {
  console.error("[FATAL]", e);
  process.exit(1);
});
