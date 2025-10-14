import { CONFIG } from "./config";

export type PostItem = {
  id: string;
  username: string;
  text: string;
  imageKey?: string | null;
  createdAt: number;
};

const auth = (t: string) => ({ Authorization: `Bearer ${t}` });

export async function me(token: string) {
  const r = await fetch(`${CONFIG.apiUrl}/me`, { headers: auth(token) });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function claimUsername(token: string, handle: string) {
  const r = await fetch(`${CONFIG.apiUrl}/username`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...auth(token) },
    body: JSON.stringify({ handle }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function getFeed(token: string): Promise<{ items: PostItem[] }> {
  const r = await fetch(`${CONFIG.apiUrl}/feed`, { headers: auth(token) });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function getUser(token: string, handle: string) {
  const r = await fetch(`${CONFIG.apiUrl}/u/${encodeURIComponent(handle)}`, {
    headers: auth(token),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function follow(token: string, handle: string) {
  const r = await fetch(`${CONFIG.apiUrl}/follow`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...auth(token) },
    body: JSON.stringify({ handle }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function unfollow(token: string, handle: string) {
  const r = await fetch(`${CONFIG.apiUrl}/unfollow`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...auth(token) },
    body: JSON.stringify({ handle }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function listFollowers(token: string, handle: string, cursor?: string) {
  const url = new URL(`${CONFIG.apiUrl}/u/${encodeURIComponent(handle)}/followers`);
  if (cursor) url.searchParams.set("cursor", cursor);
  const r = await fetch(url, { headers: auth(token) });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function listFollowing(token: string, handle: string, cursor?: string) {
  const url = new URL(`${CONFIG.apiUrl}/u/${encodeURIComponent(handle)}/following`);
  if (cursor) url.searchParams.set("cursor", cursor);
  const r = await fetch(url, { headers: auth(token) });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function searchUsers(token: string, q: string, cursor?: string) {
  const url = new URL(`${CONFIG.apiUrl}/search`);
  if (q) url.searchParams.set("q", q);
  if (cursor) url.searchParams.set("cursor", cursor);
  const r = await fetch(url.toString(), { headers: auth(token) });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function getUploadUrl(token: string, contentType: string) {
  const r = await fetch(`${CONFIG.apiUrl}/upload-url`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...auth(token) },
    body: JSON.stringify({ contentType }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function createPost(token: string, text: string, imageKey?: string) {
  const r = await fetch(`${CONFIG.apiUrl}/posts`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...auth(token) },
    body: JSON.stringify({ text, imageKey }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function createInvite(token: string, uses: number) {
  const r = await fetch(`${CONFIG.apiUrl}/invites`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...auth(token) },
    body: JSON.stringify({ uses }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export function s3PublicUrl(key?: string | null) {
  if (!key) return null;
  // your media bucket is public via CloudFront/S3; if not, swap to your distribution.
  return `https://${import.meta.env.VITE_MEDIA_BUCKET || ''}.s3.amazonaws.com/${key}`;
}

export async function setAvatar(token: string, key: string) {
  const r = await fetch(`${CONFIG.apiUrl}/me/avatar`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ key }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

// --- COMMENTS ---
export async function listComments(token: string, postId: string, cursor?: string) {
  const url = new URL(`${CONFIG.apiUrl}/comments/${encodeURIComponent(postId)}`);
  if (cursor) url.searchParams.set('cursor', cursor);
  const r = await fetch(url.toString(), { headers: { Authorization: `Bearer ${token}` } });
  if (!r.ok) throw new Error(await r.text());
  return r.json(); // { items, nextCursor }
}
export async function addComment(token: string, postId: string, text: string) {
  const r = await fetch(`${CONFIG.apiUrl}/comments/${encodeURIComponent(postId)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ text }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json(); // { id, ... }
}

// --- REACTIONS ---
export async function getReactions(token: string, postId: string) {
  const r = await fetch(`${CONFIG.apiUrl}/reactions/${encodeURIComponent(postId)}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json(); // { counts: { "👍":2, ... }, my: ["👍"] }
}
export async function toggleReaction(token: string, postId: string, emoji: string) {
  const r = await fetch(`${CONFIG.apiUrl}/reactions/${encodeURIComponent(postId)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ emoji }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json(); // { ok: true }
}