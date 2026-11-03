// worker/guard.js — v44: устойчивые лимиты на «дорогие» запросы.
//
// До v44 все лимиты Worker жили в обычных Map одного изолята: перезапуск или
// другой изолят начинал счёт с нуля, так что от целенаправленного флуда они
// почти не защищали. Guard — Durable Object, по одному на IP (idFromName
// 'ip:<адрес>'), плюс один общий ('global') для дневного бюджета TURN.
// Durable Object живёт в одном экземпляре на весь мир, а счётчики лежат в
// его хранилище — переживают перезапуски.
//
// Через Guard идут только редкие запросы: создание чат-комнаты, кодов и
// ссылок, регистрация номера, бэкап, выдача TURN, загрузка файлов. Звонки,
// сообщения и поллинг сюда не ходят — квота бесплатного тарифа (100 000
// запросов к Durable Objects в сутки) не расходуется на обычную работу.
// Лимиты щедрые: за одним IP мобильного оператора (CGNAT) могут быть сотни
// людей.

export const GUARD_RULES = {
  turn: { limit: 60, windowMs: 60_000 },
  turnDay: { limit: 20_000, windowMs: 86_400_000 }, // общий бюджет TURN в сутки
  chatInit: { limit: 120, windowMs: 3_600_000 },
  pairCreate: { limit: 200, windowMs: 3_600_000 },
  pairAlias: { limit: 60, windowMs: 3_600_000 },
  pairWord: { limit: 60, windowMs: 3_600_000 },
  backupPut: { limit: 60, windowMs: 3_600_000 },
  inboxRegister: { limit: 200, windowMs: 3_600_000 },
  chatFile: { limit: 3000, windowMs: 3_600_000 },
};

export class Guard {
  constructor(state) {
    this.state = state;
    this.c = null; // { rule: { n, reset } }
  }

  async fetch(req) {
    let body = null;
    try {
      body = await req.json();
    } catch {}
    const names = Array.isArray(body?.rules) ? body.rules.filter((r) => GUARD_RULES[r]) : [];
    if (!names.length) return Response.json({ ok: true });
    if (!this.c) this.c = (await this.state.storage.get('c')) || {};
    const now = Date.now();
    for (const [k, v] of Object.entries(this.c)) if (!v || now >= v.reset) delete this.c[k];
    for (const r of names) {
      const e = this.c[r];
      if (e && e.n >= GUARD_RULES[r].limit) {
        return Response.json({ ok: false, rule: r, retryAfter: Math.ceil((e.reset - now) / 1000) }, { status: 429 });
      }
    }
    for (const r of names) {
      const e = this.c[r] || (this.c[r] = { n: 0, reset: now + GUARD_RULES[r].windowMs });
      e.n++;
    }
    await this.state.storage.put('c', this.c);
    return Response.json({ ok: true });
  }
}

// Проверка из Worker. Без привязки GUARD (старый wrangler.toml) и при сбое
// самого Guard запрос пропускается: лимиты — защита квоты, а не доступа,
// и не должны ломать звонки.
export async function guardAllows(env, key, rules) {
  if (!env.GUARD || !rules.length) return { ok: true };
  try {
    const stub = env.GUARD.get(env.GUARD.idFromName(key));
    const r = await stub.fetch('https://guard/check', { method: 'POST', body: JSON.stringify({ rules }) });
    if (r.status === 429) return { ok: false, ...(await r.json().catch(() => ({}))) };
  } catch {}
  return { ok: true };
}

// Какое правило соответствует запросу (null — запрос не ограничивается Guard).
export function guardRuleFor(method, pathname) {
  if (method === 'POST' && /^\/chat\/[a-f0-9]{64}\/init$/.test(pathname)) return 'chatInit';
  if (method === 'POST' && /^\/chat\/[a-f0-9]{64}\/file$/.test(pathname)) return 'chatFile';
  if (method === 'POST' && /^\/inbox\/(?:chatdial-)?[a-f0-9]{16,64}\/register$/.test(pathname)) return 'inboxRegister';
  if (method === 'POST' && pathname === '/pair/create') return 'pairCreate';
  if (method === 'POST' && pathname === '/pair/alias') return 'pairAlias';
  if (method === 'POST' && pathname === '/pair/word') return 'pairWord';
  if (method === 'PUT' && pathname === '/pair/backup') return 'backupPut';
  return null;
}

// Предельный размер тела по Content-Length — до чтения тела. Значения с
// запасом над проверками внутри объектов (файл 512 КБ base64, бэкап 64 КБ,
// пакет сигнала 180 000 символов).
export function bodyLimitFor(pathname) {
  if (/^\/chat\/[a-f0-9]{64}\/file$/.test(pathname)) return 768 * 1024;
  if (/^\/chat\//.test(pathname)) return 384 * 1024;
  if (pathname.startsWith('/room/')) return 256 * 1024;
  if (pathname === '/pair/backup') return 128 * 1024;
  return 64 * 1024;
}
