// v44: устойчивые лимиты Worker (worker/guard.js).
import test from 'node:test';
import assert from 'node:assert/strict';
import { Guard, GUARD_RULES, guardAllows, guardRuleFor, bodyLimitFor } from '../worker/guard.js';

const mkState = (store = new Map()) => ({ storage: { get: async (k) => structuredClone(store.get(k)), put: async (k, v) => void store.set(k, structuredClone(v)) }, store });
const check = (g, rules) => g.fetch(new Request('https://guard/check', { method: 'POST', body: JSON.stringify({ rules }) }));

test('Guard: пропускает до лимита, дальше 429 с retryAfter', async () => {
  const g = new Guard(mkState());
  const { limit } = GUARD_RULES.pairAlias;
  for (let i = 0; i < limit; i++) assert.equal((await check(g, ['pairAlias'])).status, 200);
  const r = await check(g, ['pairAlias']);
  assert.equal(r.status, 429);
  const j = await r.json();
  assert.equal(j.rule, 'pairAlias');
  assert.ok(j.retryAfter > 0 && j.retryAfter <= 3600);
  // другое правило того же IP не затронуто
  assert.equal((await check(g, ['pairCreate'])).status, 200);
});

test('Guard: счётчик переживает перезапуск объекта (лежит в хранилище)', async () => {
  const st = mkState();
  let g = new Guard(st);
  for (let i = 0; i < GUARD_RULES.backupPut.limit; i++) await check(g, ['backupPut']);
  g = new Guard(st); // «перезапуск»: память пуста, хранилище то же
  assert.equal((await check(g, ['backupPut'])).status, 429);
});

test('Guard: окно истекает — счёт с нуля; неизвестные правила игнорируются', async () => {
  const st = mkState();
  const g = new Guard(st);
  for (let i = 0; i < GUARD_RULES.pairWord.limit; i++) await check(g, ['pairWord']);
  assert.equal((await check(g, ['pairWord'])).status, 429);
  g.c.pairWord.reset = Date.now() - 1;
  assert.equal((await check(g, ['pairWord'])).status, 200);
  assert.equal((await check(g, ['нет-такого'])).status, 200);
});

test('guardAllows: без привязки GUARD и при сбое Guard — пропускает', async () => {
  assert.deepEqual(await guardAllows({}, 'ip:1', ['turn']), { ok: true });
  const broken = { GUARD: { idFromName: () => 'x', get: () => ({ fetch: async () => { throw new Error('down'); } }) } };
  assert.deepEqual(await guardAllows(broken, 'ip:1', ['turn']), { ok: true });
});

test('guardRuleFor и bodyLimitFor: только «дорогие» запросы, частые — без Guard', () => {
  const room = 'a'.repeat(64);
  assert.equal(guardRuleFor('POST', `/chat/${room}/init`), 'chatInit');
  assert.equal(guardRuleFor('POST', `/chat/${room}/file`), 'chatFile');
  assert.equal(guardRuleFor('POST', `/chat/${room}/msg`), null);
  assert.equal(guardRuleFor('GET', `/chat/${room}/fetch`), null);
  assert.equal(guardRuleFor('POST', '/inbox/' + 'b'.repeat(32) + '/register'), 'inboxRegister');
  assert.equal(guardRuleFor('POST', '/inbox/' + 'b'.repeat(32) + '/ring'), null);
  assert.equal(guardRuleFor('POST', '/room/' + 'c'.repeat(32)), null);
  assert.equal(guardRuleFor('PUT', '/pair/backup'), 'backupPut');
  assert.equal(guardRuleFor('POST', '/pair/claim'), null);
  // пределы тела не ниже проверок внутри объектов
  assert.ok(bodyLimitFor(`/chat/${room}/file`) > 512 * 1024 + 1024);
  assert.ok(bodyLimitFor('/room/' + 'c'.repeat(32)) > 180_000);
  assert.ok(bodyLimitFor('/pair/backup') > 64 * 1024 + 1024);
});
