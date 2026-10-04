// api/stream.js - versi Vercel Serverless Function dari Finder.js
// Dipanggil oleh halaman: /api/stream?url=<link tiktok>
const cache = new Map();
const API = 'https://www.tikwm.com/api/';
const UA = 'Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/120 Mobile Safari/537.36';
const MAX_PAGES = 12;     // 12 x 50 = sampai 600 komentar
const MAX_REPLY = 40;     // buka balasan maksimal 40 komentar
const GAP = 1100;         // jeda minimal antar request (batas gratis tikwm)

const sleep = ms => new Promise(r => setTimeout(r, ms));
let last = 0;

async function tw(endpoint, params) {
  const u = new URL(API + endpoint);
  for (const [k, v] of Object.entries(params)) u.searchParams.set(k, v);
  let msg = '';
  for (let i = 0; i < 3; i++) {
    const wait = GAP - (Date.now() - last);
    if (wait > 0) await sleep(wait);
    last = Date.now();
    try {
      const r = await fetch(u, { headers: { 'User-Agent': UA } });
      const j = await r.json();
      if (j && j.code === 0) return j.data;
      msg = (j && j.msg) || 'respon tidak valid';
    } catch (e) { msg = e.message; }
  }
  throw new Error(msg);
}

const fmt = n => {
  n = Number(n) || 0;
  if (n >= 1e6) return (n / 1e6).toFixed(1).replace(/\.0$/, '') + ' M';
  if (n >= 1e3) return (n / 1e3).toFixed(1).replace(/\.0$/, '') + ' K';
  return String(n);
};
const mapC = c => ({ user: '@' + (c.user && c.user.unique_id || ''), text: (c.text || '').replace(/\s+/g, ' ') });

// ---------- Jalur cepat: langsung ke TikTok, paralel ----------
const TT = 'https://www.tiktok.com/api/comment/list/';
const TTR = 'https://www.tiktok.com/api/comment/list/reply/';
const BASE = { aid: 1988, app_language: 'id', app_name: 'tiktok_web', device_platform: 'web_pc', count: 50 };

async function ttGet(base, params) {
  const u = new URL(base);
  for (const [k, v] of Object.entries({ ...BASE, ...params })) u.searchParams.set(k, v);
  try {
    const r = await fetch(u, { headers: { 'User-Agent': UA, Referer: 'https://www.tiktok.com/', Accept: 'application/json' } });
    return JSON.parse(await r.text());
  } catch (e) { return null; }
}

async function resolveId(url) {
  try {
    const r = await fetch(url, { redirect: 'follow', headers: { 'User-Agent': UA } });
    const m = (r.url || '').match(/\/video\/(\d+)/) || (await r.text()).match(/\/video\/(\d+)/);
    return m && m[1];
  } catch (e) { return null; }
}

async function runDirect(id, info, first, emit) {
  const total = info.comment_count || first.total || 50;
  const pages = Math.min(MAX_PAGES, Math.max(1, Math.ceil(total / 50)));
  const raw = [...first.comments];
  let n = raw.length, nReply = 0;
  emit({ type: 'comments', sumber: 'langsung', list: raw.map(mapD), dibaca: n, balasan: 0, total });

  await Promise.all(Array.from({ length: pages - 1 }, (_, i) =>
    ttGet(TT, { aweme_id: id, cursor: (i + 1) * 50 }).then(j => {
      const list = (j && j.comments) || [];
      raw.push(...list); n += list.length;
      emit({ type: 'comments', sumber: 'langsung', list: list.map(mapD), dibaca: n, balasan: nReply, total });
    })));

  const me = info.author && info.author.unique_id;
  const cand = pickReplies(raw, me);
  await Promise.all(cand.map(c =>
    ttGet(TTR, { item_id: id, comment_id: c.cid, cursor: 0 }).then(j => {
      const list = (j && j.comments) || [];
      nReply += list.length;
      emit({ type: 'comments', sumber: 'langsung', list: list.map(mapR), dibaca: n, balasan: nReply, total });
    })));
  emit({ type: 'done', sumber: 'langsung', dibaca: n, balasan: nReply, total, error: [] });
}
const KW = /preset|link|xml|minta|req|mana|dong|donk|pls|plis|share|bagi|kasih|dm|kirim|spill|drop/i;
const hasReply = c => (c.reply_total || c.reply_comment_total || c.reply_count || 0) > 0;
function pickReplies(raw, me) {
  let c = raw.filter(hasReply);
  if (!raw.some(x => 'reply_total' in x || 'reply_comment_total' in x || 'reply_count' in x)) c = raw.slice(); // field tidak dikenal
  const rank = x => ((x.user && x.user.unique_id) === me ? 4 : 0) + (KW.test(x.text || '') ? 2 : 0) + (hasReply(x) ? 1 : 0);
  return c.sort((a, b) => rank(b) - rank(a)).slice(0, MAX_REPLY);
}
const mapR = c => ({ ...mapC(c), reply: true });
const mapD = c => ({ user: '@' + (c.user && c.user.unique_id || ''), text: (c.text || '').replace(/\s+/g, ' ') });

// ---------- Jalur cadangan: tikwm (1 request/detik) ----------
// Balasan dibuka langsung per halaman, dan berhenti lebih awal kalau link pemilik sudah ketemu.
async function runTikwm(info, emit, stop) {
  const me = info.author && info.author.unique_id;
  const errs = [];
  let n = 0, nReply = 0, nOpen = 0, cursor = 0;
  const out = () => ({ dibaca: n, balasan: nReply, total: info.comment_count });

  for (let p = 0; p < MAX_PAGES && !stop(); p++) {
    let page;
    try { page = await tw('comment/list/', { url: info.id, count: 50, cursor }); }
    catch (e) { errs.push('komentar: ' + e.message); break; }
    const list = page.comments || [];
    n += list.length;
    emit({ type: 'comments', sumber: 'tikwm', list: list.map(mapC), ...out() });

    for (const c of pickReplies(list, me).slice(0, 4)) {
      if (nOpen >= MAX_REPLY || stop()) break;
      nOpen++;
      try {
        const rp = await tw('comment/reply/', { video_id: info.id, comment_id: c.id, count: 50, cursor: 0 });
        const rl = rp.comments || [];
        nReply += rl.length;
        emit({ type: 'comments', sumber: 'tikwm', list: rl.map(mapR), ...out() });
      } catch (e) { errs.push('balasan: ' + e.message); }
    }
    if (!page.hasMore) break;
    cursor = page.cursor;
  }
  emit({ type: 'done', sumber: 'tikwm', awal: stop(), ...out(), error: errs.slice(0, 2) });
}

async function run(url, emit, early) {
  const ev = [];
  let hit = false, extra = 0;
  const wrap = o => {
    ev.push(o); emit(o);
    if (o.type !== 'comments') return;
    if (hit) extra++;
    else if (early && extract(ev).presets.some(p => p.byOwner)) hit = true;
  };
  const stop = () => early && hit && extra >= 6;   // beri 6 request tambahan setelah link pertama ketemu

  const idP = resolveId(url);
  const infoP = tw('', { url, hd: 1 });
  const id = await idP;
  const firstP = id ? ttGet(TT, { aweme_id: id, cursor: 0 }) : Promise.resolve(null);
  const [info, first] = await Promise.all([infoP, firstP]);
  wrap({ type: 'info', caption: info.title || '', account: '@' + (info.author && info.author.unique_id || ''),
         views: fmt(info.play_count), likes: fmt(info.digg_count), comments: info.comment_count });
  if (first && Array.isArray(first.comments) && first.comments.length) return runDirect(id, info, first, wrap);
  return runTikwm(info, wrap, stop);
}

// ---------- Ekstraksi link preset di server (untuk bot) ----------
const HOSTS = [[/alightcreative\.com|alight\.link/i, 'SHARE'], [/drive\.google\.com|docs\.google\.com/i, 'DRIVE'], [/mediafire\.com/i, 'MEDIAFIRE'], [/mega\.nz/i, 'MEGA'], [/t\.me\//i, 'TELEGRAM'], [/pixeldrain\.com|gofile\.io|dropbox\.com|terabox\.com|sfile\.mobi/i, 'FILE'], [/bit\.ly|s\.id|linktr\.ee|trakteer\.id|saweria\.co/i, 'LINK']];
const LINK = /(?:https?:\/\/)?(?:www\.)?(?:alightcreative\.com|alight\.link|drive\.google\.com|docs\.google\.com|mediafire\.com|mega\.nz|t\.me|bit\.ly|s\.id|linktr\.ee|pixeldrain\.com|gofile\.io|dropbox\.com|terabox\.com|sfile\.mobi|trakteer\.id|saweria\.co)\/?[^\s]*/gi;

function extract(ev) {
  const info = ev.find(e => e.type === 'info') || {};
  const list = ev.filter(e => e.type === 'comments').flatMap(e => e.list);
  const cr = ((info.caption || '').match(/CR\s*[:：]?\s*(@[\w.]+)/i) || [])[1] || '';
  const out = [], seen = new Set();
  list.concat([{ user: '', text: info.caption || '' }]).forEach(c => {
    const found = new Set((c.text.match(LINK) || []).concat(c.text.match(/https?:\/\/[^\s]+/gi) || []));
    found.forEach(u => {
      u = u.replace(/[.,)!]+$/, '');
      if (!/^https?:\/\//i.test(u)) u = 'https://' + u;
      const h = HOSTS.find(([r]) => r.test(u)) || (/tiktok\.com|tiktokcdn/i.test(u) ? null : [0, 'LINK']);
      if (!h || seen.has(u)) return;
      seen.add(u);
      const ratio = (c.text.match(/\b(9:16|16:9|1:1|4:5)\b/) || [])[1] || '9:16';
      out.push({ url: u, type: h[1], ratio, by: c.user, reply: !!c.reply, byOwner: !!c.user && c.user === info.account, credit: cr });
    });
  });
  out.sort((a, b) => b.byOwner - a.byOwner);
  return { account: info.account || '', caption: info.caption || '', views: info.views || '', likes: info.likes || '',
           comments: info.comments || 0, presets: out };
}

// Jalankan pencarian (pakai cache 10 menit), kirim tiap event ke onEmit
async function getEvents(url, onEmit, early = true) {
  const key = url + (early ? '|e' : '|f');
  const hit = cache.get(key);
  if (hit && Date.now() - hit.t < 10 * 60 * 1000) { hit.ev.forEach(onEmit); return; }
  const ev = [];
  const emit = o => { ev.push(o); onEmit(o); };
  try { await run(url, emit, early); cache.set(key, { t: Date.now(), ev }); }
  catch (e) { emit({ type: 'error', message: e.message }); }
}

const okUrl = url => /^https?:\/\/((www|vt|vm|m)\.)?tiktok\.com\//i.test(url);

module.exports = async (req, res) => {
  const u = new URL(req.url, 'http://x');
  const url = u.searchParams.get('url') || '';
  if (!okUrl(url)) {
    res.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8' });
    return res.end(JSON.stringify({ error: 'Link TikTok tidak valid' }));
  }
  res.writeHead(200, {
    'Content-Type': 'application/x-ndjson; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    'X-Accel-Buffering': 'no',
    'Access-Control-Allow-Origin': '*'
  });
  await getEvents(url, o => res.write(JSON.stringify(o) + '\n'), u.searchParams.get('full') !== '1');
  res.end();
};
