import fs from "fs";
import path from "path";
import iconv from "iconv-lite";
import * as cheerio from "cheerio";

/**
 * 결과 파일/메타 파일 경로
 * - ipo.json: 자동 생성 결과
 * - ipo_meta_manual.json: 수동 메타(증권사/균등/메모) - 선택
 */
const OUT_JSON = path.join(process.cwd(), "docs", "data", "ipo.json");
const META_JSON = path.join(process.cwd(), "docs", "data", "ipo_meta_manual.json");

// DART 청약 달력(지분증권)
const DART_URL = "https://dart.fss.or.kr/dsac008/main.do";
// KIND 상장법인목록 다운로드(EUC-KR)
const KIND_LIST_DL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download";

const UA = "Mozilla/5.0 (compatible; IPOCalendarBot/1.0)";
const FETCH_TIMEOUT_MS = 25000;
const DOC_FETCH_DELAY_MS = 400; // DART 문서 조회 간 딜레이(차단 방지)

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

/**
 * DART 달력 파싱
 * - "코/유/기 + 회사명 + [시작]/[종료]" 패턴으로 sbd_start/sbd_end 만들고
 * - 가능하면 rcpNo(공시 접수번호)도 함께 붙임(문서 확인/유상증자 구분용)
 */
function parseDartCalendar(html, year, month) {
  const $ = cheerio.load(html);

  // (중요) 달력 안 링크 텍스트 -> rcpNo 매핑
  // a 태그가 dsaf001/main.do?rcpNo=... 로 연결되는 경우가 많음
  const linkMap = new Map();
  $("a[href*='dsaf001/main.do?rcpNo=']").each((_, a) => {
    const href = $(a).attr("href") || "";
    const m = href.match(/rcpNo=(\d{14})/);
    if (!m) return;
    const key = normalizeName($(a).text());
    if (key) linkMap.set(key, m[1]);
  });

  const text = $("body").text().replace(/\u00a0/g, " ");
  const tokens = text.split(/\s+/).map((t) => t.trim()).filter(Boolean);

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
          sbd_end: null,
          rcp_no: null,
        };

        const date = `${year}-${pad2(month)}-${pad2(curDay)}`;
        if (which === "[시작]") item.sbd_start = date;
        if (which === "[종료]") item.sbd_end = date;

        // 링크텍스트는 보통 "코 회사명 [시작]" 같이 붙어 있는 경우가 많아서 이 형태로도 찾아봄
        const linkKey1 = normalizeName(`${ms} ${name} ${which}`);
        const linkKey2 = normalizeName(`${name} $
