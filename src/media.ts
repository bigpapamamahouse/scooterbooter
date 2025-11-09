let BASE = (import.meta.env.VITE_MEDIA_BASE_URL || "").trim();
if (BASE && !/^https?:\/\//i.test(BASE)) BASE = "https://" + BASE;
BASE = BASE.replace(/\/+$/, ""); // strip trailing slashes

const BUCKET = (import.meta as any).env?.VITE_MEDIA_BUCKET as string | undefined;
const REGION = (import.meta as any).env?.VITE_AWS_REGION || "us-east-1";

export function mediaUrlFromKey(key?: string | null): string | undefined {
  if (!key) return;
  const safeKey = encodeURI(key);

  if (BASE) return `${BASE}/${safeKey}`;
  if (BUCKET) {
    return BUCKET.includes(".")
      ? `https://s3.${REGION}.amazonaws.com/${BUCKET}/${safeKey}`
      : `https://${BUCKET}.s3.${REGION}.amazonaws.com/${safeKey}`;
  }
  console.warn("MEDIA URL missing config", { BASE, BUCKET, REGION, key });
  return;
}