const DATA_URL = "./data/ipo.json";
const SHORTCUT_NAME = "공모주 미리알림 추가";

const $ = (id) => document.getElementById(id);

$("shortcut-name-view").textContent = SHORTCUT_NAME;

function escapeIcs(s) {
  return String(s || "")
    .replace(/\\/g, "\\\\")
    .replace(/\n/g, "\\n")
    .replace(/,/g, "\\,")
    .replace(/;/g, "\\;");
}

function addDays(ymdStr, days) {
  const [y, m, d] = ymdStr.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + days);
  const yy = dt.getUTCFullYear();
  const mm = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(dt.getUTCDate()).padStart(2, "0");
  return `${yy}-${mm}-${dd}`;
}

function dtIcsAllDay(ymdStr) {
  return ymdStr.replaceAll("-", "");
}

function buildICS(items) {
  const lines = [];
  lines.push("BEGIN:VCALENDAR");
  lines.push("VERSION:2.0");
  lines.push("PRODID:-//IPO Calendar//KR//EN");
  lines.push("CALSCALE:GREGORIAN");
  lines.push(`X-WR-CALNAME:${escapeIcs("공모주 청약")}`);

  const now = new Date();
  const dtstamp =
    `${now.getUTCFullYear()}${String(now.getUTCMonth()+1).padStart(2,"0")}${String(now.getUTCDate()).padStart(2,"0")}T` +
    `${String(now.getUTCHours()).padStart(2,"0")}${String(now.getUTCMinutes()).padStart(2,"0")}${String(now.getUTCSeconds()).padStart(2,"0")}Z`;

  for (const it of items) {
    const start = dtIcsAllDay(it.sbd_start);
    const endExclusive = dtIcsAllDay(addDays(it.sbd_end, 1));

    const desc = [
      `청약: ${it.sbd_start} ~ ${it.sbd_end}`,
      it.brokers ? `증권사: ${it.brokers}` : `증권사: (미입력)`,
      it.equalMin ? `균등 최소금액: ${it.equalMin}` : `균등 최소금액: (미입력)`,
      it.note ? `메모: ${it.note}` : "",
      "※ 최종 일정/증권사는 반드시 공시 확인"
    ].filter(Boolean).join("\n");

    lines.push("BEGIN:VEVENT");
    lines.push(`UID:${escapeIcs(it.corp_name)}-${escapeIcs(it.sbd_start)}@ipo`);
    lines.push(`DTSTAMP:${dtstamp}`);
    lines.push(`DTSTART;VALUE=DATE:${start}`);
    lines.push(`DTEND;VALUE=DATE:${endExclusive}`);
    lines.push(`SUMMARY:${escapeIcs(`[공모주] ${it.corp_name} 청약`)}`);
    lines.push(`DESCRIPTION:${escapeIcs(desc)}`);
    lines.push("END:VEVENT");
  }

  lines.push("END:VCALENDAR");
  return lines.join("\r\n");
}

function buildRemindersText(items) {
  return items.map(it => {
    const p = [];
    p.push(`[공모주] ${it.corp_name}`);
    p.push(`청약 ${it.sbd_start}~${it.sbd_end}`);
    if (it.brokers) p.push(`증권사 ${it.brokers}`);
    if (it.equalMin) p.push(`균등 ${it.equalMin}`);
    return p.join(" | ");
  }).join("\n");
}

function download(filename, content, mime = "text/plain;charset=utf-8") {
  const blob = new Blob([content], { type: mime });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(a.href);
}

function render(items, meta) {
  $("range").textContent = meta.range?.start && meta.range?.end ? `${meta.range.start} ~ ${meta.range.end}` : "-";
  $("updated").textContent = meta.last_updated_kst || "-";
  $("count").textContent = String(items.length);

  const list = $("list");
  list.innerHTML = "";
  $("empty").style.display = items.length ? "none" : "block";

  for (const it of items) {
    const div = document.createElement("div");
    div.className = "item";

    const title = document.createElement("div");
    title.className = "item-title";
    title.textContent = it.corp_name;

    const line1 = document.createElement("div");
    line1.className = "muted";
    line1.textContent = `청약: ${it.sbd_start} ~ ${it.sbd_end}`;

    const line2 = document.createElement("div");
    line2.className = "muted";
    line2.textContent = `증권사: ${it.brokers || "(미입력)"} · 균등: ${it.equalMin || "(미입력)"}`;

    div.appendChild(title);
    div.appendChild(line1);
    div.appendChild(line2);

    list.appendChild(div);
  }
}

async function loadData() {
  const url = `${DATA_URL}?t=${Date.now()}`;
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) throw new Error(`데이터 로드 실패: HTTP ${res.status}`);
  const data = await res.json();
  if (!data.ok || !Array.isArray(data.items)) throw new Error("데이터 형식이 이상함");
  return data;
}

let lastData = null;

async function reload() {
  $("reload").disabled = true;
  try {
    const data = await loadData();
    lastData = data;
    render(data.items, data);
  } catch (e) {
    alert(e.message || String(e));
  } finally {
    $("reload").disabled = false;
  }
}

$("reload").addEventListener("click", reload);

$("dl-ics").addEventListener("click", () => {
  if (!lastData) return alert("먼저 데이터 새로고침을 눌러줘!");
  const ics = buildICS(lastData.items);
  download("ipo.ics", ics, "text/calendar;charset=utf-8");
});

$("dl-rem").addEventListener("click", () => {
  if (!lastData) return alert("먼저 데이터 새로고침을 눌러줘!");
  const txt = buildRemindersText(lastData.items);
  download("ipo_reminders.txt", txt);
});

$("run-shortcut").addEventListener("click", () => {
  if (!lastData) return alert("먼저 데이터 새로고침을 눌러줘!");
  const txt = buildRemindersText(lastData.items);

  const name = encodeURIComponent(SHORTCUT_NAME);
  const text = encodeURIComponent(txt);

  // iOS 단축어 실행 URL
  const url = `shortcuts://run-shortcut?name=${name}&input=text&text=${text}`;
  window.location.href = url;
});

reload();
