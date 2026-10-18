import { randomUUID } from "node:crypto";

export type GenerationInput =
  | { kind: "music"; prompt: string; duration_ms: number }
  | { kind: "sfx"; prompt: string; duration_seconds: number; prompt_influence: number; loop: boolean };
type Job = { job_id: string; status: "running" | "ready" | "failed"; kind: "music" | "sfx"; audio_id?: string; error?: string };
type Options = {
  request: (path: string, init: RequestInit, timeoutMs: number) => Promise<Response>;
  limited: <T>(operation: () => Promise<T>) => Promise<T>;
  cache: (bytes: Buffer, model: string) => string;
  maxBytes: number;
};

// No filesystem, credentials, or user-provided URLs are exposed through these jobs.
export function createGenerationQueue(options: Options) {
  const jobs = new Map<string, { job: Job; fingerprint: string; expires: number }>();
  const ttl = 30 * 60_000;
  function prune() {
    for (const [id, entry] of jobs) {
      if (entry.job.status !== "running" && entry.expires <= Date.now()) jobs.delete(id);
    }
  }
  async function generate(input: GenerationInput): Promise<string> {
    const music = input.kind === "music";
    const model = music ? "music_v1" : "eleven_text_to_sound_v2";
    const body = input.kind === "music"
      ? { prompt: input.prompt, music_length_ms: input.duration_ms, force_instrumental: true, model_id: model }
      : { text: input.prompt, duration_seconds: input.duration_seconds, prompt_influence: input.prompt_influence, loop: input.loop, model_id: model };
    const path = `${music ? "/v1/music" : "/v1/sound-generation"}?output_format=mp3_44100_128`;
    return options.limited(async () => {
      const response = await options.request(path, {
        method: "POST", headers: { "Content-Type": "application/json", Accept: "audio/mpeg" }, body: JSON.stringify(body),
      }, music ? 600_000 : 120_000);
      if (!response.ok) throw new Error(`Audio generation failed (HTTP ${response.status}).`);
      if (!(response.headers.get("content-type") ?? "").toLowerCase().startsWith("audio/")) {
        throw new Error("ElevenLabs did not return audio.");
      }
      if (Number(response.headers.get("content-length") ?? 0) > options.maxBytes) {
        await response.body?.cancel();
        throw new Error("Generated audio exceeds the configured size limit.");
      }
      const reader = response.body?.getReader();
      if (!reader) throw new Error("ElevenLabs returned no audio.");
      const chunks: Uint8Array[] = [];
      let size = 0;
      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          size += value.byteLength;
          if (size > options.maxBytes) {
            await reader.cancel();
            throw new Error("Generated audio exceeds the configured size limit.");
          }
          chunks.push(value);
        }
      } finally { reader.releaseLock(); }
      if (!size) throw new Error("ElevenLabs returned empty audio.");
      return options.cache(Buffer.concat(chunks, size), model);
    });
  }
  return {
    start(input: GenerationInput, requestId: string = randomUUID()): Job {
      prune();
      const fingerprint = JSON.stringify(input);
      const existing = jobs.get(requestId);
      if (existing) {
        if (existing.fingerprint !== fingerprint) throw new Error("request_id was already used for different generation settings.");
        return { ...existing.job };
      }
      if (jobs.size >= 100) throw new Error("Generation job storage is full. Try again later.");
      const job: Job = { job_id: requestId, status: "running", kind: input.kind };
      const entry = { job, fingerprint, expires: Date.now() + ttl };
      jobs.set(requestId, entry);
      void generate(input).then(audioId => {
        job.audio_id = audioId;
        job.status = "ready";
        entry.expires = Date.now() + ttl;
      }).catch(error => {
        job.status = "failed";
        job.error = error instanceof Error ? error.message.slice(0, 600) : "Audio generation failed.";
        entry.expires = Date.now() + ttl;
      });
      return { ...job };
    },
    get(id: string): Job {
      prune();
      const entry = jobs.get(id);
      if (!entry) throw new Error("Generation job not found or expired. Do not automatically regenerate paid audio.");
      return { ...entry.job };
    },
  };
}
