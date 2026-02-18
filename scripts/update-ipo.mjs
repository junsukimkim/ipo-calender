import fs from "fs";
import path from "path";
import iconv from "iconv-lite";
import * as cheerio from "cheerio";

const OUT_JSON = path.join(process.cwd(), "docs", "data", "ipo.json");
const META_JSON = path.join(process.cwd(), "docs", "data", "ipo_meta_manual.json");

// DART 청약 달력(지분증권)
const DART_URL = "https://dart.fss.or.kr/dsac008/main.do";
// KIND 상장법인목록 다운로드(EUC-KR)
const KIND_LIST_DL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download";

const UA = "Mozilla/5.0 (compatible; IPOCalendarBot/1.0)";
const FETCH_TIMEOUT_MS = 25000;
const DOC_FETCH_DELAY_MS = 350;

const pad2 = (n) => String(n).padStart(2, "0");

function ymdUTC(d) {
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// KST "오늘"을 UTC Date로 만든다(날짜 계산 안정)
function nowKST_asUTCDate() {
  const now = new Date();
  return new Date(now.getTime() + 9 * 60 * 60 * 1000);
}

function endOfNextMonth_KST_asUTCDate() {
  const k = nowKST_asUTCDate();
  const y = k.getUTCFullYear();
  const m = k.getUTCMonth();
  return new Date(Date.UTC(y, m + 2, 0, 23, 59, 59));
}

function monthPairsBetween(startUTC, endUTC) {
  const out = [];
  let y = startUTC.getUTCFullYear();
  let m = startUTC.getUTCMonth();
  const ey = endUTC.getUTCFullYear();
  const em = endUTC.getUTCMonth();

  while (y < ey || (y === ey && m <= em)) {
    out.push({ year: y, month: m + 1 });
    m++;
    if (m >= 12) {
      m = 0;
      y++;
    }
  }
  return out;
}

async function fetchBuffer(url, options = {}) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), FETCH_TIMEOUT_MS);

  try {
    const res = await fetch(url, {
      ...options,
      signal: controller.signal,
      headers: {
        "user-agent": UA,
        ...(options.headers || {}),
      },
    });

    if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText} for ${url}`);
    const ab = await res.arrayBuffer();
    return Buffer.from(ab);
  } finally {
    clearTimeout(t);
  }
}

function normalizeName(s) {
  return String(s || "").replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
}

async function loadMetaMap() {
  try {
    const raw = fs.readFileSync(META_JSON, "utf8");
    const obj = JSON.parse(raw);
    return obj && typeof obj === "object" ? obj : {};
  } catch {
    return {};
  }
}

async function loadListedCorpNameSet() {
  const buf = await fetchBuffer(KIND_LIST_DL);
  const html = iconv.decode(buf, "euc-kr");
  const $ = cheerio.load(html);

  const set = new Set();
  $("table tr").each((_, tr) => {
    const tds = $(tr).find("td");
    if (tds.length < 1) return;
    const name = normalizeName($(tds[0]).text());
    if (name) set.add(name);
  });

  if (set.size < 500) {
    throw new Error(`KIND listed set too small (${set.size}). Download may have failed.`);
  }
  return set;
}

function marketShortToMarket(ms) {
  if (ms === "코") return "KOSDAQ";
  if (ms === "유") return "KOSPI";
  return "ETC";
}

/**
 * ✅ 핵심 수정: DART 달력을 td(날짜 칸) 단위로 파싱
 * - 예전처럼 "2 02" 같은 토큰 규칙에 의존하지 않음
 * - 각 td에서 (코/유/기, 회사명, [시작]/[종료]) 패턴을 찾아 날짜를 부여
 */
function parseDartCalendar(html, year, month) {
  const $ = cheerio.load(html);

  // DART가 차단/오류 페이지를 주는 경우 빠르게 감지
  const bodyText = normalizeName($("body").text());
  if (!bodyText || bodyText.length < 50) return [];
  if (bodyText.includes("접근") && bodyText.includes("제한")) return [];
  if (bodyText.includes("점검") && bodyText.includes("서비스")) return [];

  const map = new Map();

  // td들 중 "날짜로 시작하는 칸"을 잡아냄
  $("td").each((_, td) => {
    const raw = normalizeName($(td).text());
    if (!raw) return;

    // 날짜 칸은 보통 "18 ..." 처럼 숫자로 시작
    const mDay = raw.match(/^\s*(\d{1,2})\b/);
    if (!mDay) return;

    const day = Number(mDay[1]);
    if (!(day >= 1 && day <= 31)) return;

    const date = `${year}-${pad2(month)}-${pad2(day)}`;

    // 링크/타이틀/alt까지 같이 합쳐서 패턴 매칭력을 올림
    const extras = [];
    $(td)
      .find("a, img")
      .each((__, el) => {
        const t = normalizeName($(el).text());
        const title = normalizeName($(el).attr("title"));
        const alt = normalizeName($(el).attr("alt"));
        if (t) extras.push(t);
        if (title) extras.push(title);
        if (alt) extras.push(alt);
      });

    const combined = normalizeName([raw, ...extras].join(" "));

    // 이벤트 패턴: 코/유/기 + 회사명 + [시작]/[종료]
    const re = /(코|유|기)\s+([^\[\]]+?)\s*\[(시작|종료)\]/g;
    let mm;
    while ((mm = re.exec(combined)) !== null) {
      const ms = mm[1];
      const name = normalizeName(mm[2]);
      const which = mm[3];

      if (!name) continue;

      const item = map.get(name) || {
        corp_name: name,
        market_short: ms,
        market: marketShortToMarket(ms),
        sbd_start: null,
        sbd_end: null,
        rcp_no: null,
      };

      if (which === "시작") item.sbd_start = date;
      if (which === "종료") item.sbd_end = date;

      map.set(name, item);
    }
  });

  // ✅ start/end가 하나만 잡힌 경우라도 0으로 떨어지는 걸 막기 위해 임시 보정
  // (실제 DART에선 보통 2일이지만, 파싱 누락이 있을 때라도 최소 표시되게)
  const out = [];
  for (const it of map.values()) {
    if (!it.sbd_start && it.sbd_end) it.sbd_start = it.sbd_end;
    if (!it.sbd_end && it.sbd_start) it.sbd_end = it.sbd_start;
    out.push(it);
  }

  return out;
}

async function fetchDartMonth(year, month) {
  const url = `${DART_URL}?selectYear=${year}&selectMonth=${pad2(month)}`;
  const buf = await fetchBuffer(url, { headers: { referer: "https://dart.fss.or.kr/" } });
  const html = buf.toString("utf8");
  return parseDartCalendar(html, year, month);
}

function withinRange(item, startUTC, endUTC) {
  const sDate = item.sbd_start || item.sbd_end;
  const eDate = item.sbd_end || item.sbd_start;
  if (!sDate || !eDate) return false;

  const s = new Date(sDate + "T00:00:00Z");
  const e = new Date(eDate + "T23:59:59Z");
  return e >= startUTC && s <= endUTC;
}

function uniqByCompanyKeepEarliest(items) {
  const by = new Map();
  for (const it of items) {
    const key = it.corp_name;
    const prev = by.get(key);
    if (!prev) by.set(key, it);
    else {
      if ((it.sbd_start || "9999") < (prev.sbd_start || "9999")) by.set(key, it);
    }
  }
  return [...by.values()];
}

/**
 * ✅ “유상증자/제3자배정” 등을 강하게 거르는 방식은
 * rcpNo가 필요해서(문서 원문 텍스트 확인) 지금은 안전하게 UNKNOWN 유지.
 * → 우선 DART 달력 파싱이 정상(0이 아님)으로 돌아가게 만든 뒤,
 *   그 다음 단계에서 네가 “2월에 성공한 규칙”을 그대로 반영해서 강화하는 게 안전함.
 */
async function main() {
  const start = nowKST_asUTCDate();
  const end = endOfNextMonth_KST_asUTCDate();

  const months = monthPairsBetween(
    new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), 1)),
    new Date(Date.UTC(end.getUTCFullYear(), end.getUTCMonth(), 1))
  );

  const metaMap = await loadMetaMap();
  const listedSet = await loadListedCorpNameSet();

  let all = [];

  for (const m of months) {
    const monthItems = await fetchDartMonth(m.year, m.month);
    console.log(`[DART] ${m.year}-${pad2(m.month)} parsed=${monthItems.length}`);
    all.push(...monthItems);
    await sleep(250);
  }

  // 기간 필터
  all = all.filter((it) => withinRange(it, start, end));

  // 이미 상장된 회사 제외(공모주 후보 추정)
  const before = all.length;
  all = all.filter((it) => !listedSet.has(it.corp_name));
  const excluded_listed = before - all.length;

  // 중복 정리
  all = uniqByCompanyKeepEarliest(all);

  // 메타 합치기
  const items = all
    .map((it) => {
      const meta = metaMap[it.corp_name] || {};
      return {
        ...it,
        brokers: meta.brokers || "",
        equalMin: meta.equalMin || "",
        note: meta.note || "",
      };
    })
    .sort((a, b) => (a.sbd_start || "").localeCompare(b.sbd_start || ""));

  const out = {
    ok: true,
    source: "dart-calendar + kind-listed-filter (stable-td-parse)",
    range: { start: ymdUTC(start), end: ymdUTC(end) },
    last_updated_kst: ymdUTC(nowKST_asUTCDate()),
    count: items.length,
    excluded_listed,
    excluded_non_ipo: 0,
    items,
  };

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(out, null, 2), "utf8");

  console.log(`Wrote ${items.length} items. excluded_listed=${excluded_listed}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
