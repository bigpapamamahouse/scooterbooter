import React, { useEffect, useRef, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { Bell, Check, X } from "lucide-react";
import { mediaUrlFromKey } from "./media";

/** Optional: pass your own api() to avoid global 401 redirects.
 *   api(path: string, opts?: RequestInit & { redirectOn401?: boolean })
 * If not provided, we'll fall back to fetch(CONFIG.API_BASE_URL + path).
 */
type API = (path: string, opts?: RequestInit & { redirectOn401?: boolean }) => Promise<Response>;

type NotificationItem = {
  id: string;
  type: "reaction" | "comment" | "mention" | "follow_request" | "follow_accept" | string;
  fromUserId?: string;
  postId?: string | null;
  message?: string;
  fromHandle?: string | null;
  avatarKey?: string | null;
  userUrl?: string;
  postUrl?: string;
  read?: boolean;
  createdAt: number;
};

interface Props {
  api?: API;
  apiBaseUrl?: string;
  token?: string | null;
}

async function defaultApi(apiBaseUrl: string, token?: string | null, path = "", opts: RequestInit = {}) {
  const url = `${apiBaseUrl}${path.startsWith("/") ? path : `/${path}`}`;
  const res = await fetch(url, {
    ...opts,
    headers: {
      ...(opts.headers || {}),
      Authorization: token ? `Bearer ${token}` : "",
      "Content-Type": "application/json",
      "X-Ignore-Auth-Redirect": "1", // signal to your app to not force-redirect on 401 for background calls
    },
    credentials: "include",
  });
  return res;
}

export default function NotificationsDropdown({ api, apiBaseUrl, token }: Props) {
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [items, setItems] = useState<NotificationItem[]>([]);
  const [unread, setUnread] = useState(0);
  const ref = useRef<HTMLDivElement | null>(null);

  const base = apiBaseUrl || (window as any).CONFIG?.API_BASE_URL || "";
  async function call(path: string, opts?: RequestInit) {
    if (api) return api(path, { ...(opts || {}), redirectOn401: false });
    return defaultApi(base, token, path, opts);
  }

  async function load(markRead = false) {
    setLoading(true);
    try {
      const res = await call(`/notifications${markRead ? "?markRead=1" : ""}`);
      const ct = res.headers.get("content-type") || "";
      if (!ct.includes("application/json")) {
        // Avoid hard crash if API returned HTML (e.g., redirect to login page)
        const text = await res.text();
        console.warn("Non-JSON notifications response:", res.status, text.slice(0, 120));
        return;
      }
      const data = await res.json();
      const list: NotificationItem[] = Array.isArray(data.items) ? data.items : [];
      setItems(list);
      setUnread(list.filter((n) => !n.read).length);
    } catch (e) {
      console.error("notifications fetch failed", e);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load(false);
    const t = setInterval(() => load(false), 30000);
    return () => clearInterval(t);
  }, []);

  const toggle = async () => {
    const next = !open;
    setOpen(next);
    if (next) {
      await load(true);
      setUnread(0);
    }
  };

  async function acceptFollow(n: NotificationItem) {
    try {
      await call("/follow-accept", {
        method: "POST",
        body: JSON.stringify({ fromUserId: n.fromUserId }),
      });
      setItems((prev) => prev.filter((x) => x.id !== n.id));
    } catch (e) {
      console.error("accept failed", e);
    }
  }
  function declineFollow(n: NotificationItem) {
    setItems((prev) => prev.filter((x) => x.id !== n.id));
  }

  useEffect(() => {
    const onDocClick = (ev: MouseEvent) => {
      if (ref.current && !ref.current.contains(ev.target as Node)) setOpen(false);
    };
    document.addEventListener("mousedown", onDocClick);
    return () => document.removeEventListener("mousedown", onDocClick);
  }, []);

  return (
    <div className="relative" ref={ref}>
      <button
        type="button"
        onClick={toggle}
        className="relative inline-flex items-center gap-2 rounded-2xl border px-3 py-1.5 text-sm shadow-sm hover:bg-gray-50"
      >
        <Bell className="h-4 w-4" />
        <span>Notifications</span>
        {unread > 0 && (
          <span className="absolute -top-1 -right-1 rounded-full bg-red-500 px-1.5 py-0.5 text-xs font-semibold text-white">
            {unread}
          </span>
        )}
      </button>

      <AnimatePresence>
        {open && (
          <motion.div
            initial={{ opacity: 0, y: -8 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -8 }}
            transition={{ duration: 0.15 }}
            className="absolute right-1/2 z-50 mt-3 w-96 translate-x-1/2 rounded-2xl border border-gray-200 bg-white shadow-xl"
          >
            <div className="flex items-center justify-between px-3 py-2">
              <div className="text-sm font-semibold">Notifications</div>
              {loading && (
                <div
                  aria-label="loading"
                  className="h-4 w-4 animate-spin rounded-full border-2 border-gray-300 border-t-transparent"
                />
              )}
            </div>
            <div className="max-h-96 overflow-y-auto">
              {items.length === 0 ? (
                <div className="px-4 py-8 text-center text-sm text-gray-500">
                  You’re all caught up 🎉
                </div>
              ) : (
                items.map((n) => (
                  <div
                    key={n.id}
                    className="flex items-start gap-2 border-t px-3 py-2 text-sm first:border-t-0"
                  >
                    {(() => { const u = mediaUrlFromKey(n.avatarKey); return u ? (
  <img src={u} alt="" className="mt-0.5 h-7 w-7 flex-none rounded-full object-cover" />
) : (
  <div className="mt-1 h-2.5 w-2.5 flex-none rounded-full bg-indigo-500" />
) })()}
                    <div className="min-w-0 flex-1">
                      <div className="truncate text-gray-800">
                        {n.userUrl ? (
  <a href={n.userUrl} className="font-medium hover:underline">@{n.fromHandle || (n.fromUserId || '').slice(0, 8)}</a>
) : (
  <span className="font-medium">@{n.fromHandle || (n.fromUserId || '').slice(0, 8)}</span>
)}{" "}
                        {n.postUrl ? (
  <a href={n.postUrl} className="hover:underline">{n.message || "sent you a notification"}</a>
) : (
  (n.message || "sent you a notification")
)}
                      </div>
                      <div className="mt-0.5 text-xs text-gray-400">
                        {new Date(n.createdAt).toLocaleString()}
                      </div>
                    </div>
                    {n.type === "follow_request" && (
                      <div className="flex gap-1">
                        <button
                          onClick={() => acceptFollow(n)}
                          className="rounded-full bg-green-500 p-1 text-white hover:bg-green-600"
                          title="Accept"
                        >
                          <Check className="h-4 w-4" />
                        </button>
                        <button
                          onClick={() => declineFollow(n)}
                          className="rounded-full bg-red-500 p-1 text-white hover:bg-red-600"
                          title="Decline"
                        >
                          <X className="h-4 w-4" />
                        </button>
                      </div>
                    )}
                  </div>
                ))
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
