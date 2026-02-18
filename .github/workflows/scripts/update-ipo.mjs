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

const pad2 = (n) => String(n).padStart(2, "0");

function ymdUTC(d) {
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
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
  // 다음달 말일(= 다다음달 0일)
  return new Date(Date.UTC(y, m + 2, 0, 23, 59, 59));
}

function monthPairsBetween(startUTC, endUTC) {
  const out = [];
  let y = startUTC.getUTCFullYear();
  let m = startUTC.getUTCMonth(); // 0~11
  const ey = endUTC.getUTCFullYear();
  const em = endUTC.getUTCMonth();

  while (y < ey || (y === ey && m <= em)) {
    out.push({ year: y, month: m + 1 });
    m++;
    if (m >= 12) { m = 0; y++; }
  }
  return out;
}

async function fetchBuffer(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: {
      "user-agent": UA,
      ...(options.headers || {})
    }
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText} for ${url}`);
  const ab = await res.arrayBuffer();
  return Buffer.from(ab);
}

function normalizeName(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
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

  // 너무 작으면(차단/오류) 실패로 간주
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

// DART 달력 텍스트에서 "코/유/기 + 회사명 + [시작]/[종료]" 패턴 파싱
function parseDartCalendar(html, year, month) {
  const $ = cheerio.load(html);
  const text = $("body").text().replace(/\u00a0/g, " ");
  const tokens = text.split(/\s+/).map(t => t.trim()).filter(Boolean);

  let curDay = null;
  const map = new Map(); // corp_name -> item

  const isDayNum = (t) => /^\d{1,2}$/.test(t);
  const isDay2 = (t) => /^\d{2}$/.test(t);

  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i];

    // day marker: "2" 다음에 "02" 같은 패턴
    if (isDayNum(t) && tokens[i + 1] && isDay2(tokens[i + 1])) {
      const d = Number(t);
      if (Number(tokens[i + 1]) === d && d >= 1 && d <= 31) {
        curDay = d;
        i += 1;
        continue;
      }
    }

    if (t === "코" || t === "유" || t === "기") {
      const ms = t;
      const nameParts = [];
      let j = i + 1;

      while (j < tokens.length) {
        const tok = tokens[j];
        if (tok === "[시작]" || tok === "[종료]") break;
        // 다음 날짜 구간 들어가면 중단
        if (isDayNum(tok) && tokens[j + 1] && isDay2(tokens[j + 1])) break;
        nameParts.push(tok);
        j++;
      }

      const which = tokens[j];
      if ((which === "[시작]" || which === "[종료]") && curDay != null && nameParts.length) {
        const name = normalizeName(nameParts.join(" "));
        const item = map.get(name) || {
          corp_name: name,
          market_short: ms,
          market: marketShortToMarket(ms),
          sbd_start: null,
          sbd_end: null
        };
        const date = `${year}-${pad2(month)}-${pad2(curDay)}`;
        if (which === "[시작]") item.sbd_start = date;
        if (which === "[종료]") item.sbd_end = date;
        map.set(name, item);
      }

      i = j;
    }
  }

  return [...map.values()];
}

async function fetchDartMonth(year, month) {
  const url = `${DART_URL}?selectYear=${year}&selectMonth=${pad2(month)}`;
  const buf = await fetchBuffer(url);
  const html = buf.toString("utf8");
  return parseDartCalendar(html, year, month);
}

function withinRange(item, startUTC, endUTC) {
  if (!item.sbd_start || !item.sbd_end) return false;
  const s = new Date(item.sbd_start + "T00:00:00Z");
  const e = new Date(item.sbd_end + "T23:59:59Z");
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
    all.push(...monthItems);
  }

  // 기간 필터
  all = all.filter(it => withinRange(it, start, end));

  // 공모주(신규상장 후보) 추정: 이미 상장된 회사 제외
  const before = all.length;
  all = all.filter(it => !listedSet.has(it.corp_name));
  const excluded_listed = before - all.length;

  // 같은 회사 중복 정리
  all = uniqByCompanyKeepEarliest(all);

  // 메타(증권사/균등금액) 합치기
  const items = all
    .map(it => {
      const meta = metaMap[it.corp_name] || {};
      return {
        ...it,
        brokers: meta.brokers || "",
        equalMin: meta.equalMin || "",
        note: meta.note || ""
      };
    })
    .sort((a, b) => (a.sbd_start || "").localeCompare(b.sbd_start || ""));

  const out = {
    ok: true,
    source: "dart-calendar + kind-listed-filter",
    range: { start: ymdUTC(start), end: ymdUTC(end) },
    last_updated_kst: ymdUTC(nowKST_asUTCDate()),
    count: items.length,
    excluded_listed,
    items
  };

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(out, null, 2), "utf8");

  console.log(`Wrote ${items.length} items. excluded_listed=${excluded_listed}`);
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
