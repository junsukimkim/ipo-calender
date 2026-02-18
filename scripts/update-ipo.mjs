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
        const linkKey2 = normalizeName(`${name} ${which}`);
        const rcpNo = linkMap.get(linkKey1) || linkMap.get(linkKey2) || null;
        if (rcpNo) item.rcp_no = rcpNo;

        map.set(name, item);
      }

      i = j;
    }
  }

  return [...map.values()];
}

async function fetchDartMonth(year, month) {
  const url = `${DART_URL}?selectYear=${year}&selectMonth=${pad2(month)}`;
  const buf = await fetchBuffer(url, { headers: { referer: "https://dart.fss.or.kr/" } });
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

/** --------- (추가) 유상증자/제3자배정 등 걸러내기: rcpNo 문서 텍스트 기반 --------- */

function stripTags(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function extractDcmNoFromDsaf(html) {
  const patterns = [
    /viewDoc\(\s*'?\d{14}'?\s*,\s*'?(?<dcm>\d+)'?\s*,/i,
    /dcmNo\s*=\s*["'](?<dcm>\d+)["']/i,
    /dcmNo=(?<dcm>\d+)/i,
    /dcm_no=(?<dcm>\d+)/i,
  ];
  for (const re of patterns) {
    const m = html.match(re);
    if (m?.groups?.dcm) return m.groups.dcm;
  }
  return null;
}

async function fetchFilingText(rcpNo) {
  const dsafUrl = `https://dart.fss.or.kr/dsaf001/main.do?rcpNo=${rcpNo}`;
  const dsafHtml = (await fetchBuffer(dsafUrl, { headers: { referer: "https://dart.fss.or.kr/" } })).toString("utf8");
  const dcmNo = extractDcmNoFromDsaf(dsafHtml);
  if (!dcmNo) return "";

  const viewerUrl = `https://dart.fss.or.kr/report/viewer.do?rcpNo=${rcpNo}&dcmNo=${dcmNo}&eleId=0&offset=0&length=0&dtd=HTML`;
  const viewerHtml = (await fetchBuffer(viewerUrl, { headers: { referer: dsafUrl } })).toString("utf8");
  return stripTags(viewerHtml);
}

function classifyOfferingByText(text) {
  const t = text || "";

  // “유상증자/배정/신주인수권” 계열이 나오면 거의 확실히 IPO가 아니라서 제외 신호로 사용
  const rightsSignals = [
    /유상\s*증자\s*결정/,
    /주주\s*배정/,
    /제\s*3\s*자\s*배정/,
    /신주\s*인수권/,
    /신주\s*배정/,
    /구주주/,
    /발행\s*가액/,
    /청약\s*증거금.*유상/i,
  ];

  // IPO/공모주 쪽 신호(있으면 참고)
  const ipoSignals = [
    /신규\s*상장/,
    /상장\s*예정/,
    /기업\s*공개/,
    /수요\s*예측/,
    /공모\s*주/,
    /공모\s*청약/,
    /코스닥.*상장/,
    /유가증권.*상장/,
  ];

  const rightsScore = rightsSignals.reduce((acc, re) => acc + (re.test(t) ? 1 : 0), 0);
  const ipoScore = ipoSignals.reduce((acc, re) => acc + (re.test(t) ? 1 : 0), 0);

  // 제외는 “유상증자 신호가 있는 경우만” 강하게 적용(오탐 최소화)
  if (rightsScore >= 1) return "RIGHTS_OR_CAPITAL_INCREASE";
  if (ipoScore >= 1) return "IPO";
  return "UNKNOWN";
}

async function filterIpoOnly(items) {
  const cache = new Map(); // rcpNo -> type
  let excluded_non_ipo = 0;

  const kept = [];
  for (const it of items) {
    if (!it.rcp_no) {
      // rcpNo 없으면 문서 확인이 어려우니 UNKNOWN으로 유지(제외하지 않음)
      kept.push({ ...it, offer_type: "UNKNOWN" });
      continue;
    }

    try {
      if (!cache.has(it.rcp_no)) {
        // 문서 조회 간 딜레이
        await sleep(DOC_FETCH_DELAY_MS);
        const text = await fetchFilingText(it.rcp_no);
        const type = classifyOfferingByText(text);
        cache.set(it.rcp_no, type);
      }

      const offer_type = cache.get(it.rcp_no);
      if (offer_type === "RIGHTS_OR_CAPITAL_INCREASE") {
        excluded_non_ipo++;
        continue;
      }

      kept.push({ ...it, offer_type });
    } catch {
      // 문서 확인 실패 시엔 제외하지 않고 UNKNOWN 처리 (차단/네트워크 이슈 대비)
      kept.push({ ...it, offer_type: "UNKNOWN" });
    }
  }

  return { kept, excluded_non_ipo };
}

/** --------- main --------- */

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
    await sleep(250); // 월별 조회도 약간 쉬어가기
  }

  // 기간 필터
  all = all.filter((it) => withinRange(it, start, end));

  // 이미 상장된 회사 제외(공모주 후보 추정)
  const before = all.length;
  all = all.filter((it) => !listedSet.has(it.corp_name));
  const excluded_listed = before - all.length;

  // 같은 회사 중복 정리
  all = uniqByCompanyKeepEarliest(all);

  // (추가) 유상증자/배정류 제외(문서 텍스트 기반)
  const r1 = await filterIpoOnly(all);
  all = r1.kept;
  const excluded_non_ipo = r1.excluded_non_ipo;

  // 메타(증권사/균등금액) 합치기
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
    source: "dart-calendar + kind-listed-filter + filing-text-filter",
    range: { start: ymdUTC(start), end: ymdUTC(end) },
    last_updated_kst: ymdUTC(nowKST_asUTCDate()),
    count: items.length,
    excluded_listed,
    excluded_non_ipo,
    items,
  };

  fs.mkdirSync(path.dirname(OUT_JSON), { recursive: true });
  fs.writeFileSync(OUT_JSON, JSON.stringify(out, null, 2), "utf8");

  console.log(`Wrote ${items.length} items. excluded_listed=${excluded_listed} excluded_non_ipo=${excluded_non_ipo}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
