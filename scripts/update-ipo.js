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
    form.set("selectYe
