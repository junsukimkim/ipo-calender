import fs from "fs";
import path from "path";
import iconv from "iconv-lite";
import * as cheerio from "cheerio";
import { chromium } from "playwright";

const OUT_JSON = path.join(process.cwd(), "docs", "data", "ipo.json");
const META_JSON = path.join(process.cwd(), "docs", "data", "ipo_meta_manual.json");

// DART 청약 달력(지분증권)
const DART_URL = "https://dart.fss.or.kr/dsac008/main.do";
// KIND 상장법인목록 다운로드(EUC-KR)
const KIND_LIST_DL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download";

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

const FETCH_TIMEOUT_MS = 25000;

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

// 줄 안에 유상증자/배정 키워드가 보이면 제외(가벼운 휴리스틱)
function looksLikeRightsIssue(line) {
  return /(유상\s*증자|주주\s*배정|제\s*3\s*자\s*배정|신주\s*인수권|신주\s*배정|구주주)/.test(line);
}

/**
 * ✅ Playwright로 “실제 브라우저”에서 달력을 열고 innerText를 가져온다
 * (fetch로는 ‘잠시만 기다려주세요’ 페이지만 와서 0건이 됨)
 */
async function getDartRenderedTextAndHtml(page, year, month) {
  const url = `${DART_URL}?selectYear=${year}&selectMonth=${pad2(month)}`;
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });

  // 달력 로딩을 기다림(시작/종료가 보이면 성공)
  try {
    await page.waitForFunction(
      () =>
        document.body &&
        (document.body.innerText.includes("[시작]") || document.body.innerText.includes("[종료]")),
      { timeout: 15000 }
    );
  } catch {
    // 그래도 진행(아래 스니펫 로그로 원인 확인 가능)
  }

  // 약간의 추가 안정화
  await page.waitForTimeout(800);

  const html = await page.content();
  const text = await page.evaluate(() => (document.body ? document.body.innerText : ""));
  return { html, text };
}

/**
 * ✅ 렌더된 텍스트 기준 파싱: "날짜 -> 코/유/기 회사명 [시작/종료]"
 */
function parseDartCalendarFromTextAndHtml(text, html, year, month) {
  const $ = cheerio.load(html);

  // 링크텍스트 -> rcpNo 매핑(있으면)
  const linkMap = new Map();
  $("a[href*='dsaf001/main.do?rcpNo=']").each((_, a) => {
    const href = $(a).attr("href") || "";
    const m = href.match(/rcpNo=(\d{14})/);
    if (!m) return;
    const key = normalizeName($(a).text()).replace(/\s+/g, "");
    if (key) linkMap.set(key, m[1]);
  });

  const lines = String(text || "")
    .split(/\r?\n/)
    .map((l) => normalizeName(l))
    .filter(Boolean);

  let curDay = null;
  const map = new Map();

  // 이벤트 패턴(한 줄에 여러 개 가능)
  const reEvt = /(코|유|기)\s+(.+?)\s+\[(시작|종료)\]/g;

  for (const line of lines) {
    // 날짜줄: 1~31 (한 자리/두 자리 둘 다 허용)
    const md = line.match(/^(\d{1,2})$/);
    if (md) {
      const d = Number(md[1]);
      if (d >= 1 && d <= 31) curDay = d;
      continue;
    }
    if (curDay == null) continue;

    // 유상증자류 키워드 포함 줄은 통째로 제외(달력에 같이 섞이는 케이스 대응)
    if (looksLikeRightsIssue(line)) continue;

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

      // 링크 텍스트도 보통 "기케이뱅크[시작]" 형태라서 공백 제거 키로 매칭
      const k1 = normalizeName(`${ms}${name}[${which}]`).replace(/\s+/g, "");
      const k2 = normalizeName(`${ms} ${name} [${which}]`).replace(/\s+/g, "");
      const rcpNo = linkMap.get(k1) || linkMap.get(k2) || null;
      if (rcpNo) item.rcp_no = rcpNo;

      map.set(name, item);
    }
  }

  // start/end 하나만 있어도 보정
  const out = [];
  for (const it of map.values()) {
    if (!it.sbd_start && it.sbd_end) it.sbd_start = it.sbd_end;
    if (!it.sbd_end && it.sbd_start) it.sbd_end = it.sbd_start;
    out.push(it);
  }
  return out;
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

  // ✅ Playwright 브라우저 1번만 띄워서 월별 재사용
  const browser = await chromium.launch({
    headless: true,
    args: ["--disable-blink-features=AutomationControlled"],
  });
  const context = await browser.newContext({
    userAgent: UA,
    locale: "ko-KR",
    timezoneId: "Asia/Seoul",
  });
  const page = await context.newPage();

  let all = [];
  for (const m of months) {
    const { html, text } = await getDartRenderedTextAndHtml(page, m.year, m.month);
    const monthItems = parseDartCalendarFromTextAndHtml(text, html, m.year, m.month);

    console.log(`[DART] ${m.year}-${pad2(m.month)} parsed=${monthItems.length}`);
    if (monthItems.length === 0) {
      const snip = normalizeName(String(text || "").slice(0, 180));
      console.log(`[DART] ${m.year}-${pad2(m.month)} text_snippet="${snip}"`);
    }

    all.push(...monthItems);
  }

  await browser.close();

  // 기간 필터
  all = all.filter((it) => withinRange(it, start, end));

  // 이미 상장된 회사 제외(공모주 후보)
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
    source: "dart-calendar (playwright) + kind-listed-filter + rights-keyword-filter",
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
