import fs from "fs";
import path from "path";
import iconv from "iconv-lite";
import * as cheerio from "cheerio";
import { chromium } from "playwright";

const OUT_JSON = path.join(process.cwd(), "docs", "data", "ipo.json");
const META_JSON = path.join(process.cwd(), "docs", "data", "ipo_meta_manual.json");

const DART_URL = "https://dart.fss.or.kr/dsac008/main.do";
const KIND_LIST_DL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download";

const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

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

function normalizeName(s) {
  return String(s || "").replace(/\u00a0/g, " ").replace(/\s+/g, " ").trim();
}

async function fetchBuffer(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: {
      "user-agent": UA,
      "accept-language": "ko-KR,ko;q=0.9,en;q=0.7",
      ...(options.headers || {}),
    },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText} for ${url}`);
  const ab = await res.arrayBuffer();
  return Buffer.from(ab);
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

// (유상증자 등) 제외용 키워드 — 필요하면 여기 더 추가 가능
function isNonIPOText(s) {
  const t = String(s || "");
  return /(유상\s*증자|주주\s*배정|제\s*3\s*자\s*배정|신주\s*인수권|신주\s*배정|구주주)/.test(t);
}

/**
 * ✅ 핵심: 달력 “표(td)”에서 직접 뽑는다
 * - td 텍스트에 [시작]/[종료]가 포함된 셀만 후보
 * - 그 td 안에서 (1) 날짜 1~31 (2) "코/유/기 회사명 [시작/종료]"를 추출
 */
function parseDartCalendarByTd(html, year, month) {
  const $ = cheerio.load(html);
  const map = new Map();

  // 링크 텍스트 -> rcpNo (있으면)
  const linkMap = new Map();
  $("a[href*='dsaf001/main.do?rcpNo=']").each((_, a) => {
    const href = $(a).attr("href") || "";
    const m = href.match(/rcpNo=(\d{14})/);
    if (!m) return;
    const key = normalizeName($(a).text()).replace(/\s+/g, "");
    if (key) linkMap.set(key, m[1]);
  });

  const reEvt = /(코|유|기)\s+(.+?)\s+\[(시작|종료)\]/g;

  const tds = $("td").toArray();
  for (const td of tds) {
    const raw = $(td).text();
    if (!raw.includes("[시작]") && !raw.includes("[종료]")) continue;

    const cellText = normalizeName(raw);
    if (!cellText) continue;
    if (isNonIPOText(cellText)) continue;

    // 날짜(1~31) 찾기: 셀 안에서 제일 먼저 나오는 1~31 숫자를 day로 사용
    let day = null;
    const tokens = cellText.split(/\s+/);
    for (const tok of tokens) {
      if (/^\d{1,2}$/.test(tok)) {
        const d = Number(tok);
        if (d >= 1 && d <= 31) {
          day = d;
          break;
        }
      }
    }
    if (!day) {
      // 혹시 "2코 ..."처럼 붙은 케이스
      const mDay = cellText.match(/^(\d{1,2})/);
      if (mDay) {
        const d = Number(mDay[1]);
        if (d >= 1 && d <= 31) day = d;
      }
    }
    if (!day) continue;

    let mm;
    while ((mm = reEvt.exec(cellText)) !== null) {
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

      const date = `${year}-${pad2(month)}-${pad2(day)}`;
      if (which === "시작") item.sbd_start = date;
      if (which === "종료") item.sbd_end = date;

      const k1 = normalizeName(`${ms}${name}[${which}]`).replace(/\s+/g, "");
      const k2 = normalizeName(`${ms} ${name} [${which}]`).replace(/\s+/g, "");
      const rcpNo = linkMap.get(k1) || linkMap.get(k2) || null;
      if (rcpNo) item.rcp_no = rcpNo;

      map.set(name, item);
    }
  }

  // start/end 하나만 있으면 보정
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

async function fetchDartMonthHtml(page, year, month) {
  const url = `${DART_URL}?selectYear=${year}&selectMonth=${pad2(month)}`;
  await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });

  // 달력이 JS로 늦게 그려지는 케이스 대비: 조금 기다렸다가 HTML 뽑기
  try {
    await page.waitForTimeout(1500);
    await page.waitForLoadState("networkidle", { timeout: 15000 });
  } catch {
    // 무시
  }

  const finalUrl = page.url();
  const html = await page.content();
  const hasMarker = html.includes("[시작]") || html.includes("[종료]");

  console.log(`[DART] ${year}-${pad2(month)} url=${finalUrl}`);
  console.log(`[DART] ${year}-${pad2(month)} html_has_marker=${hasMarker}`);

  if (!hasMarker) {
    // 디버그용으로 body 텍스트 앞부분을 더 길게 찍어준다
    const bodyText = await page.evaluate(() => (document.body ? document.body.innerText : ""));
    console.log(
      `[DART] ${year}-${pad2(month)} text_snippet="${normalizeName(bodyText).slice(0, 400)}"`
    );
  }

  return html;
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
    const html = await fetchDartMonthHtml(page, m.year, m.month);
    const monthItems = parseDartCalendarByTd(html, m.year, m.month);
    console.log(`[DART] ${m.year}-${pad2(m.month)} parsed=${monthItems.length}`);
    all.push(...monthItems);
  }

  await browser.close();

  // 기간 필터
  all = all.filter((it) => withinRange(it, start, end));

  // 상장회사 제외
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
    source: "dart-calendar(td-parse) + kind-listed-filter",
    range: { start: ymdUTC(start), end: ymdUTC(end) },
    last_updated_kst: ymdUTC(nowKST_asUTCDate()),
    count: items.length,
    excluded_listed,
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
