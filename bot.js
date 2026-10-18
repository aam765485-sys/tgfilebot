const http = require("http");
const fs = require("fs");
const path = require("path");
const { Readable } = require("stream");
const { pipeline } = require("stream/promises");

// ═══ تنظیمات از Environment Variables ═══
const TOKEN = process.env.BOT_TOKEN;
const BASE_URL = process.env.BASE_URL;
const BOTAPI_URL = process.env.BOTAPI_URL;
const API = `${BOTAPI_URL}/bot${TOKEN}`;
const FILE_URL = `${BOTAPI_URL}/file/bot${TOKEN}/`;
const FILES_DIR = "/var/tgfiles";
const PORT = parseInt(process.env.PORT || "8080");
const ADMIN_ID = process.env.ADMIN_ID || "0";
// ═══════════════════════════════════════════

fs.mkdirSync(FILES_DIR, { recursive: true });
const INDEX_FILE = path.join(FILES_DIR, "index.json");
let fileIndex = fs.existsSync(INDEX_FILE) ? JSON.parse(fs.readFileSync(INDEX_FILE, "utf8")) : {};
const saveIndex = () => fs.writeFileSync(INDEX_FILE, JSON.stringify(fileIndex));

const buckets = new Map();
function checkRate(id) {
  const now = Date.now();
  const e = buckets.get(id) || { count: 0, reset: now + 60000 };
  if (now > e.reset) { e.count = 0; e.reset = now + 60000; }
  e.count++; buckets.set(id, e);
  if (buckets.size > 5000) buckets.clear();
  return e.count <= 15;
}

async function tg(method, body) {
  const res = await fetch(`${API}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return res.json();
}
const sendMessage = (chatId, text, extra = {}) => tg("sendMessage", { chat_id: chatId, text, ...extra });

let stats = { uploads: 0, downloads: 0, errors: 0 };

async function handleMessage(msg) {
  const chatId = msg.chat.id;
  if (!checkRate(chatId)) return sendMessage(chatId, "⛔ زیادی سریع! یک دقیقه صبر کن.");

  if (msg.text?.startsWith("/start")) {
    return sendMessage(chatId,
      "🏢 <b>بات لینک‌ساز فایل</b>\n\nفایل بفرست (تا ۲ گیگابایت) → لینک مستقیم و دائمی بگیر.\n✍️ با کپشن بفرست تا اسم فایل رو خودت تعیین کنی.",
      { parse_mode: "HTML" });
  }
  if (msg.text === "/stats" && String(chatId) === ADMIN_ID) {
    return sendMessage(chatId, `📊 Stats:\n⬆️ آپلود: ${stats.uploads}\n⬇️ دانلود: ${stats.downloads}\n❌ خطا: ${stats.errors}`);
  }

  const file = msg.document || msg.video || msg.audio || msg.voice || (msg.photo ? msg.photo[msg.photo.length - 1] : null);
  if (!file) return;

  const sizeGB = ((file.file_size || 0) / 1073741824).toFixed(2);
  await sendMessage(chatId, `⏳ در حال پردازش (${sizeGB} GB)...`);

  try {
    const gf = await tg("getFile", { file_id: file.file_id });
    if (!gf.ok) throw new Error("getFile failed");

    const rel = gf.result.file_path.replace(/^\/+/, "");
    const dl = await fetch(FILE_URL + rel);
    if (!dl.ok || !dl.body) throw new Error("دانلود فایل ناموفق");

    const fileName = msg.document?.file_name || (msg.caption ? sanitize(msg.caption) : null) || guessName(file, msg);
    const key = `${Date.now()}-${file.file_unique_id}`;
    const dest = path.join(FILES_DIR, key);

    await pipeline(Readable.fromWeb(dl.body), fs.createWriteStream(dest));

    fileIndex[key] = { name: fileName, size: file.file_size || 0 };
    saveIndex();
    stats.uploads++;

    await sendMessage(chatId,
      `✅ <b>لینک ساخته شد!</b>\n\n📁 ${escapeHtml(fileName)}\n📦 ${sizeGB} GB\n\n🔗 <code>${BASE_URL}/dl/${key}</code>`,
      { parse_mode: "HTML", disable_web_page_preview: true });
  } catch (e) {
    stats.errors++;
    await sendMessage(chatId, "❌ خطا: " + e.message);
  }
}

// ═══ سرور دانلود (Range → resume) ═══
const MIME = { jpg: "image/jpeg", jpeg: "image/jpeg", png: "image/png", gif: "image/gif", mp4: "video/mp4", mkv: "video/x-matroska", mp3: "audio/mpeg", pdf: "application/pdf", zip: "application/zip", rar: "application/vnd.rar", ogg: "audio/ogg", webm: "video/webm", apk: "application/vnd.android.package-archive" };

http.createServer((req, res) => {
  const url = new URL(req.url, "http://x");
  if (!url.pathname.startsWith("/dl/")) { res.end("✅ Bot running"); return; }
  const key = decodeURIComponent(url.pathname.slice(4)).replace(/[^\w\-.]/g, "");
  const filePath = path.join(FILES_DIR, key);
  if (!fileIndex[key] || !fs.existsSync(filePath)) { res.writeHead(404); res.end("Not Found"); return; }

  const meta = fileIndex[key];
  const ext = (meta.name.split(".").pop() || "").toLowerCase();
  const size = fs.statSync(filePath).size;
  const headers = {
    "Content-Type": MIME[ext] || "application/octet-stream",
    "Content-Disposition": `attachment; filename*=UTF-8''${encodeURIComponent(meta.name)}`,
    "Accept-Ranges": "bytes",
  };
  const m = req.headers.range ? /bytes=(\d+)-(\d*)/.exec(req.headers.range) : null;
  if (m) {
    const start = parseInt(m[1]);
    const end = m[2] ? Math.min(parseInt(m[2]), size - 1) : size - 1;
    res.writeHead(206, { ...headers, "Content-Range": `bytes ${start}-${end}/${size}`, "Content-Length": end - start + 1 });
    fs.createReadStream(filePath, { start, end }).pipe(res);
  } else {
    res.writeHead(200, { ...headers, "Content-Length": size });
    fs.createReadStream(filePath).pipe(res);
  }
  stats.downloads++;
}).listen(PORT, () => console.log(`✅ Server on port ${PORT}`));

// ═══ دریافت پیام‌ها ═══
let offset = 0;
async function poll() {
  try {
    const res = await fetch(`${API}/getUpdates`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ offset, timeout: 30, allowed_updates: ["message"] }),
    });
    const data = await res.json();
    if (data.ok && data.result) for (const u of data.result) {
      offset = u.update_id + 1;
      if (u.message) handleMessage(u.message).catch(() => {});
    }
  } catch (e) {}
  setTimeout(poll, 1000);
}
poll();

function sanitize(n) { return String(n).replace(/[\/\\:*?"<>|]/g, "_").trim().slice(0, 80) || "file"; }
function guessName(file, msg) {
  const ext = { photo: ".jpg", video: ".mp4", audio: ".mp3", voice: ".ogg" };
  for (const [k, e] of Object.entries(ext)) if (msg[k]) return `${k}-${file.file_unique_id}${e}`;
  return `file-${file.file_unique_id}`;
}
function escapeHtml(s) { return String(s).replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])); }
