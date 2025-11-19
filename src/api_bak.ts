import { CONFIG } from "./config";


// --- token helper (auto-pull from Cognito localStorage) ---
function __getIdTokenFromLocalStorage(): string | null {
  try {
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i)!;
      if (k && k.endsWith('.idToken')) {
        const v = localStorage.getItem(k);
        if (v && v.split('.').length === 3) return v;
      }
    }
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i)!;
      if (k && k.includes('idToken')) {
        const v = localStorage.getItem(k);
        if (v && v.split('.').length === 3) return v;
      }
    }
    return null;
  } catch { return null; }
}

export type PostItem = {
  id: string;
  username: string;
  text: string;
  imageKey?: string | null;
  createdAt: number;
};

const auth = (t?: string) => ({ Authorization: `Bearer ${t ?? ''}` });

export async function me(token?: string) {
  const tok = token
    ?? localStorage.getItem('idToken.manual')
    ?? __getIdTokenFromLocalStorage()
    ?? undefined;

  if (!tok) {
    throw new Error('NO_ID_TOKEN_AVAILABLE');
  }

  const r = await fetch(`${CONFIG.apiUrl}/me`, { headers: auth(tok) });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function updateMe(token: string, data: { fullName?: string | null }) {
  // Try PATCH first
  let r = await fetch(`${CONFIG.apiUrl}/me`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify(data),
  });
  if (r.status === 404) {
    // Some gateways might not have PATCH wired; fall back to POST /me
    r = await fetch(`${CONFIG.apiUrl}/me`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify(data),
    });
  }
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

export async function getReactionsWho(token: string, postId: string) {
  const url = new URL(`${CONFIG.apiUrl}/reactions/${encodeURIComponent(postId)}`);
  url.searchParams.set('who', '1');
  const r = await fetch(url.toString(), { headers: { Authorization: `Bearer ${token}` } });
  if (!r.ok) throw new Error(await r.text());
  return r.json() as Promise<{ counts: Record<string, number>, my: string[], who: Record<string, {userId:string,handle:string|null,avatarKey:string|null}[]> }>;
}

// --- AVATAR UPLOAD URL ---
export async function getAvatarUploadUrl(token: string, contentType: string) {
  const r = await fetch(`${CONFIG.apiUrl}/avatar-url`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({ contentType }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json() as Promise<{ url: string; key: string }>;
}


export async function updatePost(
  token: string,
  id: string,
  data: { text?: string; imageKey?: string; deleteImage?: boolean }
) {
  const r = await fetch(`${CONFIG.apiUrl}/posts/${encodeURIComponent(id)}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json", ...auth(token) },
    body: JSON.stringify(data),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}



export async function getPost(token: string, id: string) {
  const r = await fetch(`${CONFIG.apiUrl}/posts/${encodeURIComponent(id)}`, {
    headers: { ...auth(token) },
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}
export async function deletePost(token: string, id: string) {
  const r = await fetch(`${CONFIG.apiUrl}/posts/${encodeURIComponent(id)}`, {
    method: "DELETE",
    headers: { ...auth(token) },
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function updateComment(token: string, postId: string, id: string, text: string) {
  const r = await fetch(`${CONFIG.apiUrl}/comments/${encodeURIComponent(postId)}`, {
    method: 'PATCH',
    headers: { "Content-Type": "application/json", ...auth(token) },
    body: JSON.stringify({ id, text }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function deleteComment(token: string, postId: string, id: string) {
  const r = await fetch(`${CONFIG.apiUrl}/comments/${encodeURIComponent(postId)}`, {
    method: 'DELETE',
    headers: { "Content-Type": "application/json", ...auth(token) },
    body: JSON.stringify({ id }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}
