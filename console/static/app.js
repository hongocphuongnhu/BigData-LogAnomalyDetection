// Trung tam giam sat: doc /api/infra moi 5 giay va ve the Ha tang.
// Dung textContent, khong ghep chuoi HTML tu du lieu API (ten index, ten app co the la bat ky).
"use strict";

const POLL_MS = 5000;
const numberVi = new Intl.NumberFormat("vi-VN");
const decimalVi = new Intl.NumberFormat("vi-VN", { maximumFractionDigits: 1 });
const timeVi = new Intl.DateTimeFormat("vi-VN", { hour: "2-digit", minute: "2-digit", second: "2-digit" });

const APP_NAMES = {
  stream_detect: "Phát hiện bất thường thời gian thực",
  http_access_ingest: "Làm sạch log mô phỏng",
  nasa_ingest: "Làm sạch log NASA",
};
const INDEX_NAMES = {
  "logs-sim": "Log mô phỏng (báo điện tử)",
  "logs-nasa": "Log NASA thật (1995)",
  "log-features": "Đặc trưng theo cửa sổ 5 phút",
  "log-alerts": "Cảnh báo bất thường",
  "alert-history": "Lịch sử luật cảnh báo Kibana",
};
const TIERS = { raw: "Thô (raw)", bronze: "Đồng (bronze)", silver: "Bạc — đã làm sạch", gold: "Vàng — đặc trưng, điểm" };
const ES_STATUS = { green: "Ổn định", yellow: "Chạy, thiếu bản sao", red: "Lỗi dữ liệu" };

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key === "class") node.className = value;
    else node.setAttribute(key, value);
  }
  for (const child of children) {
    if (child != null) node.append(child instanceof Node ? child : String(child));
  }
  return node;
}

function bytesVi(bytes) {
  if (bytes == null) return "–";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) { value /= 1024; unit += 1; }
  return `${unit ? decimalVi.format(value) : numberVi.format(value)} ${units[unit]}`;
}

function durationVi(seconds) {
  if (seconds < 60) return `${seconds} giây`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes} phút`;
  return `${Math.floor(minutes / 60)} giờ ${minutes % 60} phút`;
}

function metric(value, label) {
  return el("div", { class: "metric" }, el("div", { class: "value" }, value), el("div", { class: "label" }, label));
}

function link(href, text = "Mở giao diện gốc") {
  return el("a", { class: "open-link", href, target: "_blank", rel: "noopener" }, `${text} ↗`);
}

function setCard(service, state, statusText, body, footer) {
  const card = document.querySelector(`.card[data-service="${service}"]`);
  card.dataset.state = state;
  const status = card.querySelector(".status");
  status.className = `status status-${state}`;
  status.textContent = statusText;
  card.querySelector(".metrics").replaceChildren(...body);
  card.querySelector("footer").replaceChildren(...(footer ? [footer] : []));
}

function downBody(result, hint) {
  return [el("p", { class: "hint" }, `${result.error}. `, hint)];
}

const START_HINT = () => el("span", {}, "Thử ", el("code", {}, "docker compose up -d"), ".");

function renderSpark(spark, links) {
  if (!spark.ok) return setCard("spark", "down", "Mất kết nối", downBody(spark, START_HINT()), link(links.spark));
  setCard("spark", spark.workers > 0 ? "ok" : "down", spark.workers > 0 ? "Đang chạy" : "Không có máy worker", [
    metric(numberVi.format(spark.workers), "máy worker"),
    metric(`${spark.cores_used}/${spark.cores}`, "lõi CPU đang dùng"),
    metric(bytesVi(spark.memory_mb * 1024 * 1024), "bộ nhớ của cụm"),
  ], link(links.spark));
}

function renderSparkTask(spark, sparkApp, links) {
  if (!spark.ok) {
    return setCard("spark_task", "down", "Mất kết nối", [el("p", { class: "empty" }, "Không đọc được vì cụm Spark không phản hồi.")]);
  }
  if (!spark.apps.length) {
    return setCard("spark_task", "idle", "Chưa có tác vụ", [
      el("p", { class: "empty" }, "Cụm đang rảnh. Bộ phát hiện sẽ được bật ở mục 2."),
    ]);
  }
  const items = spark.apps.map((app) => el("li", {},
    el("span", {}, APP_NAMES[app.name] || app.name, el("br"), el("span", { class: "sub" }, `${app.name} · chạy ${durationVi(app.duration_s)}`)),
    el("span", { class: "num" }, `${app.cores} lõi`)));
  const waiting = spark.apps.some((app) => app.state !== "RUNNING" || app.cores === 0);
  setCard("spark_task", waiting ? "pending" : "ok", waiting ? "Đang chờ lõi CPU" : `${spark.apps.length} tác vụ đang chạy`,
    [el("ul", { class: "list" }, ...items)], sparkApp.ok && sparkApp.running ? link(links.spark_app) : null);
}

function renderHdfs(hdfs, links) {
  if (!hdfs.ok) return setCard("hdfs", "down", "Mất kết nối", downBody(hdfs, START_HINT()), link(links.hdfs));
  const tiers = Object.entries(TIERS).map(([key, label]) => {
    const tier = hdfs.lake[key];
    return el("li", {}, el("span", {}, label), tier
      ? el("span", { class: "num" }, `${bytesVi(tier.bytes)} · ${numberVi.format(tier.files)} file`)
      : el("span", { class: "sub" }, "chưa có"));
  });
  setCard("hdfs", hdfs.live_nodes > 0 ? "ok" : "down", hdfs.live_nodes > 0 ? "Đang chạy" : "Không có DataNode", [
    metric(numberVi.format(hdfs.live_nodes), "máy lưu dữ liệu"),
    metric(numberVi.format(hdfs.blocks), "block dữ liệu"),
    metric(bytesVi(hdfs.capacity_used), "đã dùng"),
    el("ul", { class: "list", style: "grid-column: 1 / -1" }, ...tiers),
  ], link(links.hdfs_browse, "Xem thư mục /lake"));
}

function renderEs(es, links) {
  if (!es.ok) return setCard("es", "down", "Mất kết nối", downBody(es, START_HINT()), link(links.es));
  const entries = Object.entries(es.indices);
  const total = entries.reduce((sum, [, index]) => sum + index.docs, 0);
  const rows = entries.map(([name, index]) => el("li", {},
    INDEX_NAMES[name] ? el("span", {}, INDEX_NAMES[name], el("br"), el("span", { class: "sub" }, name)) : el("span", {}, name),
    el("span", { class: "num" }, numberVi.format(index.docs))));
  const healthy = es.status !== "red";
  setCard("es", healthy ? "ok" : "down", ES_STATUS[es.status] || es.status, [
    metric(numberVi.format(total), "bản ghi"),
    metric(numberVi.format(entries.length), "chỉ mục"),
    entries.length ? el("ul", { class: "list", style: "grid-column: 1 / -1" }, ...rows)
      : el("p", { class: "empty", style: "grid-column: 1 / -1" }, "Chưa có chỉ mục nào. Chạy scripts/run_all.py để nạp dữ liệu."),
  ], link(links.es));
}

function renderKibana(kibana, links) {
  if (!kibana.ok) return setCard("kibana", "down", "Mất kết nối", downBody(kibana, START_HINT()), link(links.kibana));
  const ready = kibana.level === "available";
  setCard("kibana", ready ? "ok" : "pending", ready ? "Sẵn sàng" : "Đang khởi động", [
    metric(kibana.version || "–", "phiên bản"),
  ], link(links.kibana));
}

async function refresh() {
  const banner = document.getElementById("server-banner");
  try {
    const response = await fetch("/api/infra", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    banner.hidden = true;
    renderSpark(data.spark, data.links);
    renderSparkTask(data.spark, data.spark_app, data.links);
    renderHdfs(data.hdfs, data.links);
    renderEs(data.es, data.links);
    renderKibana(data.kibana, data.links);
    document.getElementById("updated").textContent = `Cập nhật lúc ${timeVi.format(new Date())}`;
  } catch (error) {
    banner.hidden = false;
    document.getElementById("updated").textContent = "Không cập nhật được";
  }
}

// --- muc 3: canh bao ---------------------------------------------------------
const ALERT_POLL_MS = 3000;
const dateTimeVi = new Intl.DateTimeFormat("vi-VN", { hour: "2-digit", minute: "2-digit", day: "2-digit", month: "2-digit", year: "numeric" });
const SOURCE_NOTES = {
  stream: "Cảnh báo do bộ phát hiện thời gian thực sinh ra, mới nhất ở trên. Chạy một kịch bản ở mục 2 để thấy cảnh báo mới xuất hiện.",
  sim: "Kết quả chấm điểm một ngày log mô phỏng của trang báo, xếp theo điểm, nặng nhất ở trên.",
  nasa: "Log thật của máy chủ NASA tháng 7/1995, không có nhãn. Đây là những cửa sổ lạ nhất mà model tìm ra.",
};
const EMPTY_NOTES = {
  stream: "Chưa có cảnh báo thời gian thực nào. Bật bộ phát hiện và chạy một kịch bản ở mục 2.",
  sim: "Chưa có cảnh báo của lô mô phỏng. Chạy scripts/run_all.py --batch --serve để nạp.",
  nasa: "Chưa có cảnh báo NASA. Chạy scripts/run_all.py --batch --serve để nạp.",
};
let alertSource = "stream";
let alertLabel = "";
let alertsFirstLoad = true;
const seenAlerts = {}; // theo "nguon|nhan": null = chua tai lan nao (khong to sang ca danh sach dau)
const LABEL_ORDER = ["ddos", "scan", "brute_force", "sqli", "normal"];

function renderLabelChips(byLabel, total) {
  const box = document.getElementById("alerts-labels");
  box.hidden = !byLabel.length;
  if (!byLabel.length) return box.replaceChildren();
  const sorted = [...byLabel].sort((a, b) => LABEL_ORDER.indexOf(a.label) - LABEL_ORDER.indexOf(b.label));
  const chip = (label, text, count, extraClass = "") => {
    const button = el("button", { type: "button", "aria-pressed": String(alertLabel === label), class: extraClass },
      text, el("span", { class: "num" }, numberVi.format(count)));
    button.addEventListener("click", () => selectAlertLabel(label));
    return button;
  };
  box.replaceChildren(
    el("span", { class: "chips-title" }, "Theo nhãn mô phỏng:"),
    chip("", "Tất cả", sorted.reduce((sum, item) => sum + item.count, 0) || total),
    ...sorted.map((item) => item.label === "normal"
      ? chip("normal", "Bình thường (báo nhầm)", item.count, "is-normal")
      : chip(item.label, item.label_vi, item.count)));
}

function alertItem(alert, isNew) {
  const ratio = alert.ratio ?? 0;
  const bar = el("div", { class: "bar", role: "img", "aria-label": alert.score_vi || "Không có điểm" },
    el("span", { class: "fill", style: `width:${Math.min(ratio / 3, 1) * 100}%` }),
    el("span", { class: "mark", style: "left:33.33%", title: "Ngưỡng" }));
  const when = alert.window_start ? `Khung 5 phút từ ${dateTimeVi.format(new Date(alert.window_start))}` : "";
  return el("li", { class: `alert sev-${alert.severity || "low"}${isNew ? " is-new" : ""}` },
    el("div", { class: "alert-side" },
      el("span", { class: "sev" }, `Mức ${alert.severity_vi.toLowerCase()}`),
      el("span", { class: "ip" }, alert.ip || "IP không rõ"),
      el("span", { class: "when" }, when)),
    el("div", { class: "alert-main" },
      el("p", { class: "headline" }, alert.headline),
      alert.context.length ? el("ul", { class: "context" }, ...alert.context.map((line) => el("li", {}, line))) : null,
      alert.score_vi ? el("div", { class: "scoreline" }, bar, el("span", { class: "score-text" }, `${alert.score_vi} · ${alert.ratio_vi}`)) : null,
      alert.ground_truth_vi ? el("p", { class: "truth" }, "Nhãn mô phỏng: ", el("b", {}, alert.ground_truth_vi),
        el("span", {}, " — chỉ để đối chiếu, model không dùng nhãn này")) : null));
}

function alertMessage(text, isError = false) {
  return el("li", { class: `alerts-empty${isError ? " is-error" : ""}` }, text);
}

async function refreshAlerts() {
  const source = alertSource;
  const label = alertLabel;
  const key = `${source}|${label}`;
  const list = document.getElementById("alerts-list");
  try {
    const query = new URLSearchParams({ source, limit: "30", ...(label ? { label } : {}) });
    const response = await fetch(`/api/alerts?${query}`, { cache: "no-store" });
    const data = await response.json();
    if (source !== alertSource || label !== alertLabel) return; // nguoi xem da doi tab/nhan trong luc cho
    if (!data.ok) return list.replaceChildren(alertMessage(`Không đọc được cảnh báo: ${data.error}.`, true));
    for (const button of document.querySelectorAll(".tabs button")) {
      const count = data.by_source[button.dataset.source] || 0;
      button.querySelector(".count").textContent = numberVi.format(count);
    }
    // Lan dau mo trang: chua co canh bao real-time thi mo san tab lo mo phong cho co noi dung.
    if (alertsFirstLoad && source === "stream" && !data.total && data.by_source.sim) {
      alertsFirstLoad = false;
      return selectAlertSource("sim");
    }
    alertsFirstLoad = false;
    renderLabelChips(data.by_label || [], data.total);
    if (data.missing_index) return list.replaceChildren(alertMessage("Chưa có chỉ mục log-alerts. Chạy scripts/run_all.py --serve để nạp dữ liệu."));
    if (!data.items.length) return list.replaceChildren(alertMessage(EMPTY_NOTES[source]));
    const seen = seenAlerts[key];
    list.replaceChildren(...data.items.map((alert) => alertItem(alert, seen != null && !seen.has(alert.doc_id))));
    seenAlerts[key] = new Set([...(seen || []), ...data.items.map((alert) => alert.doc_id)]);
  } catch (error) {
    list.replaceChildren(alertMessage("Không đọc được cảnh báo: mất kết nối tới server trung tâm.", true));
  }
}

function selectAlertLabel(label) {
  alertLabel = label;
  document.getElementById("alerts-list").replaceChildren(alertMessage("Đang tải cảnh báo…"));
  refreshAlerts();
}

function selectAlertSource(source) {
  alertSource = source;
  alertLabel = "";
  for (const button of document.querySelectorAll(".tabs button")) {
    button.setAttribute("aria-selected", String(button.dataset.source === source));
  }
  document.getElementById("alerts-note").textContent = SOURCE_NOTES[source];
  document.getElementById("alerts-list").replaceChildren(alertMessage("Đang tải cảnh báo…"));
  refreshAlerts();
}

for (const button of document.querySelectorAll(".tabs button")) {
  button.addEventListener("click", () => selectAlertSource(button.dataset.source));
}

// --- sang/toi: luu lua chon trong localStorage (co the bi chan -> bo qua) ---
const THEMES = ["auto", "light", "dark"];
const THEME_LABELS = { auto: "tự động", light: "sáng", dark: "tối" };

function applyTheme(theme) {
  if (theme === "auto") delete document.documentElement.dataset.theme;
  else document.documentElement.dataset.theme = theme;
  document.getElementById("theme-toggle").textContent = `Giao diện: ${THEME_LABELS[theme]}`;
}

function initTheme() {
  let theme = "auto";
  try { theme = localStorage.getItem("console-theme") || "auto"; } catch (_) { /* bo qua */ }
  if (!THEMES.includes(theme)) theme = "auto";
  applyTheme(theme);
  document.getElementById("theme-toggle").addEventListener("click", () => {
    theme = THEMES[(THEMES.indexOf(theme) + 1) % THEMES.length];
    applyTheme(theme);
    try { localStorage.setItem("console-theme", theme); } catch (_) { /* bo qua */ }
  });
}

initTheme();
refresh();
setInterval(refresh, POLL_MS);
selectAlertSource("stream");
setInterval(refreshAlerts, ALERT_POLL_MS);
