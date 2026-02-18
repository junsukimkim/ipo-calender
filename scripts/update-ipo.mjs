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

const pad2 = (n) => String(n).padStart(2, "0");

function ymdUTC(d) {
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

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
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "ko-KR,ko;q=0.9,en;q=0.7",
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

// HTML 인코딩(utf8/euc-kr) 자동 감지
function decodeHtml(buf) {
  const head = buf.slice(0, 3000).toString("ascii").toLowerCase();
  if (head.includes("charset=euc-kr") || head.includes("charset=\"euc-kr\"") || head.includes("euckr")) {
    return iconv.decode(buf, "euc-kr");
  }
  return buf.toString("utf8");
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
 * ✅ DART 달력 파싱(강화판)
 * - 태그 구조(td/li/div) 상관없이 body 텍스트를 "줄"로 읽음
 * - 날짜는 "02" 같은 2자리 줄에서 잡고
 * - 이벤트는 "코/유/기 + 회사명 + [시작]/[종료]"를 정규식으로 뽑음
 * - 추가로 a[href*='rcpNo=']에서 rcp_no도 붙여봄(있으면)
 */
function parseDartCalendar(html, year, month) {
  const $ = cheerio.load(html);

  // (선택) 링크 텍스트 -> rcpNo 매핑 (있으면 저장)
  const linkMap = new Map();
  $("a[href*='dsaf001/main.do?rcpNo=']").each((_, a) => {
    const href = $(a).attr("href") || "";
    const m = href.match(/rcpNo=(\d{14})/);
    if (!m) return;
    const t = normalizeName($(a).text());
    if (t) linkMap.set(t, m[1]);
  });

  const bodyText = $("body").text().replace(/\u00a0/g, " ");
  // 줄 단위로 쪼개서(여기서 성공/실패가 갈림)
  const lines = bodyText
    .split(/\r?\n/)
    .map((l) => normalizeName(l))
    .filter(Boolean);

  let curDay = null;
  const map = new Map();

  // 이벤트는 한 줄에 여러 개가 붙기도 하므로 g 플래그 필수
  const reEvt = /(코|유|기)\s+(.+?)\s+\[(시작|종료)\]/g;

  for (const line of lines) {
    // 날짜 라인: "02", "23" 같은 두 자리만 인정(잡음 최소화)
    const md = line.match(/^(\d{2})$/);
    if (md) {
      const d = Number(md[1]);
      if (d >= 1 && d <= 31) curDay = d;
      continue;
    }

    if (curDay == null) continue;

    let mm;
    while ((mm = reEvt.exec(line)) !== null) {
      const ms = mm[1];
      const name = normalizeName(mm[2]);
      const which = mm[3]; // 시작/종료
      if (!name) continue;

      const item = map.get(name) || {
        corp_name: name,
        market_short: ms,
        market: marketShortToMarket(ms),
        sbd_start: null,
        sbd_end: null,
        rcp_no: null,
      };

      const date = `${year}-${pad2(month)}-${pad2(curDay)}`;
      if (which === "시작") item.sbd_start = date;
      if (which === "종료") item.sbd_end = date;

      // 링크 텍스트는 대개 "기 케이뱅크 [시작]" 형태라서 그 키로 찾아봄
      const key = normalizeName(`${ms} ${name} [${which}]`);
      const rcpNo = linkMap.get(key) || null;
      if (rcpNo) item.rcp_no = rcpNo;

      map.set(name, item);
    }
  }

  // start/end 하나만 잡혀도 0으로 떨어지는 걸 막기 위한 보정
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
  const html = decodeHtml(buf);
  const items = parseDartCalendar(html, year, month);

  // 디버그(로그에 찍힘): 여기 숫자가 0이면 “가져온 HTML에 이벤트가 없거나 파싱 실패”
  console.log(`[DART] ${year}-${pad2(month)} parsed=${items.length}`);
  if (items.length === 0) {
    const snippet = normalizeName(
      cheerio.load(html)("body").text().replace(/\u00a0/g, " ").slice(0, 220)
    );
    console.log(`[DART] ${year}-${pad2(month)} body_snippet="${snippet}"`);
  }

  return items;
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
    source: "dart-calendar + kind-listed-filter (line-parse)",
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
