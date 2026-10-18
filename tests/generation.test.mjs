import test from 'node:test';
import assert from 'node:assert/strict';
import { createGenerationQueue } from '../src/generation.ts';

const music = { kind: 'music', prompt: 'Rock techno trailer with a decisive ending', duration_ms: 67367 };
const audio = () => new Response(Buffer.from('ID3-audio'), { headers: { 'Content-Type': 'audio/mpeg' } });
async function settle(queue, id) {
  for (let i = 0; i < 30; i++) {
    await new Promise(resolve => setImmediate(resolve));
    const job = queue.get(id);
    if (job.status !== 'running') return job;
  }
  throw new Error('Job did not settle');
}
function setup(request = async () => audio(), maxBytes = 1000) {
  const calls = [], cached = [];
  const queue = createGenerationQueue({
    request: async (...args) => { calls.push(args); return request(...args); },
    limited: async operation => operation(), maxBytes,
    cache: (bytes, model) => { cached.push({ bytes, model }); return 'cached-audio'; },
  });
  return { queue, calls, cached };
}
test('music uses correct endpoint, exact duration and guaranteed instrumental mode', async () => {
  const { queue, calls, cached } = setup();
  const job = queue.start(music);
  assert.equal(job.status, 'running');
  assert.equal((await settle(queue, job.job_id)).status, 'ready');
  assert.equal(calls[0][0], '/v1/music?output_format=mp3_44100_128');
  assert.deepEqual(JSON.parse(calls[0][1].body), { prompt: music.prompt, music_length_ms: 67367, force_instrumental: true, model_id: 'music_v1' });
  assert.equal(calls[0][2], 600000);
  assert.equal(cached[0].bytes.toString(), 'ID3-audio');
});
test('SFX forwards duration, loop and prompt influence to the sound model', async () => {
  const { queue, calls } = setup();
  const job = queue.start({ kind: 'sfx', prompt: 'Electric light trail whoosh', duration_seconds: 1.25, loop: false, prompt_influence: 0.6 });
  await settle(queue, job.job_id);
  assert.equal(calls[0][0], '/v1/sound-generation?output_format=mp3_44100_128');
  assert.deepEqual(JSON.parse(calls[0][1].body), { text: 'Electric light trail whoosh', duration_seconds: 1.25, prompt_influence: 0.6, loop: false, model_id: 'eleven_text_to_sound_v2' });
});
test('retries and polling do not create additional paid requests', async () => {
  const { queue, calls } = setup();
  const job = queue.start(music);
  queue.start(music, job.job_id);
  await settle(queue, job.job_id);
  queue.start(music, job.job_id);
  queue.get(job.job_id);
  assert.equal(calls.length, 1);
  assert.throws(() => queue.start({ ...music, duration_ms: 5000 }, job.job_id), /different/);
});
test('provider failure becomes failed job and is not automatically retried', async () => {
  const { queue, calls, cached } = setup(async () => { throw new Error('ElevenLabs request failed (403): permission missing'); });
  const job = queue.start(music);
  const result = await settle(queue, job.job_id);
  assert.equal(result.status, 'failed');
  assert.match(result.error, /403/);
  queue.start(music, job.job_id);
  assert.equal(calls.length, 1);
  assert.equal(cached.length, 0);
});
for (const [name, response, limit] of [
  ['non-audio response', () => new Response('{}', { headers: { 'Content-Type': 'application/json' } }), 100],
  ['empty response', () => new Response('', { headers: { 'Content-Type': 'audio/mpeg' } }), 100],
  ['oversized streamed response', audio, 2],
  ['oversized declared response', () => new Response('abc', { headers: { 'Content-Type': 'audio/mpeg', 'Content-Length': '1000' } }), 2],
]) test(`rejects ${name} without caching`, async () => {
  const { queue, cached } = setup(response, limit);
  const job = queue.start(music);
  assert.equal((await settle(queue, job.job_id)).status, 'failed');
  assert.equal(cached.length, 0);
});
test('concurrency guard covers reading the whole response', async () => {
  let active = 0, finish;
  const queue = createGenerationQueue({
    limited: async fn => { if (active) throw new Error('busy'); active++; try { return await fn(); } finally { active--; } },
    request: async () => new Response(new ReadableStream({ start(c) { finish = () => { c.enqueue(new Uint8Array([1])); c.close(); }; } }), { headers: { 'Content-Type': 'audio/mpeg' } }),
    cache: () => 'audio', maxBytes: 100,
  });
  const first = queue.start(music);
  await new Promise(resolve => setImmediate(resolve));
  const second = queue.start(music);
  assert.equal((await settle(queue, second.job_id)).status, 'failed');
  assert.equal(active, 1);
  finish();
  assert.equal((await settle(queue, first.job_id)).status, 'ready');
  assert.equal(active, 0);
});
