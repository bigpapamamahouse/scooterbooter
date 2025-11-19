import { confirm, confirmSignup, login, login as legacyLogin, resend, resend as legacyResend, signUp, signUp as legacySignUp, forgotPassword, confirmForgotPassword, changePassword } from './auth';
// main.tsx (patched with global NotificationsDropdown placement + robust JSON handling)

function readIdTokenSync(): string {
  try {
    const manual = localStorage.getItem('idToken.manual');
    if (manual && manual.split('.').length === 3) return manual;
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i)!;
      if (k && (k.endsWith('.idToken') || k.includes('idToken'))) {
        const v = localStorage.getItem(k);
        if (v && v.split('.').length === 3) return v;
      }
    }
  } catch {}
  return '';
}

async function waitForIdToken(timeoutMs = 3000): Promise<string> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const manual = localStorage.getItem('idToken.manual');
    if (manual && manual.split('.').length === 3) return manual;
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i)!;
      if (k && k.endsWith('.idToken')) {
        const v = localStorage.getItem(k);
        if (v && v.split('.').length === 3) return v;
      }
      if (k && k.includes('idToken')) {
        const v = localStorage.getItem(k);
        if (v && v.split('.').length === 3) return v;
      }
    }
    await new Promise(r => setTimeout(r, 50));
  }
  throw new Error('ID_TOKEN_NOT_AVAILABLE');
}

import React from 'react'
import ReactDOM from 'react-dom/client'
import {createBrowserRouter, RouterProvider, Link, Outlet, useNavigate, useParams, useLocation, Navigate} from 'react-router-dom'
import './index.css'
import { confirm, login as legacyLogin, resend as legacyResend, signUp as legacySignUp } from './auth'
import { CONFIG } from './config'
import { me, claimUsername, getFeed, getUser, follow, unfollow,
         listFollowers, listFollowing, searchUsers, getUploadUrl, createPost,
         createInvite, listComments, addComment, getReactions, toggleReaction,
         getReactionsWho, updatePost, getPost, deletePost, updateComment,
         deleteComment, updateMe } from './api'


// Fallback for legacy references
function Gate(){ return <Navigate to="/signup" replace /> }

import { mediaUrlFromKey } from './media'

import { PhotoIcon, KeyIcon, EllipsisVerticalIcon } from '@heroicons/react/24/outline'

/* ----------------------------- Small UI bits ----------------------------- */
function Card(p:any){ return <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-5">{p.children}</div> }

function logoutAndReload(){
  try{
    Object.keys(localStorage).forEach(k => {
      if (/cognito|LastAuthUser|idToken|accessToken|refreshToken/i.test(k)) {
        try { localStorage.removeItem(k) } catch {}
      }
    });
  } finally {
    window.location.assign('/');
  }
}


/* ----------------------- Notifications Dropdown (patched) ----------------------- */
function NotificationsDropdown(){
  const [open, setOpen] = React.useState(false);
  const [items, setItems] = React.useState<any[]>([]);
  const [unread, setUnread] = React.useState<number>(0);
  const [loading, setLoading] = React.useState<boolean>(false);
  const dropdownRef = React.useRef<HTMLDivElement|null>(null);

  async function fetchNotifications(markRead = false){
    setLoading(true);
    try {
      // Token can be late; try sync and fallback to quick wait
      let token = readIdTokenSync();
      if (!token) {
        try { token = await waitForIdToken(1000); } catch {}
      }
      if (!token) { setLoading(false); return; }

      const API_BASE =
        (CONFIG as any).API_BASE_URL ||
        (CONFIG as any).apiUrl ||
        (window as any).CONFIG?.API_BASE_URL ||
        (window as any).CONFIG?.apiUrl ||
        (import.meta as any).env?.VITE_API_URL ||
        '';

      const url = API_BASE + '/notifications' + (markRead ? '?markRead=1' : '');
      const r = await fetch(url, {
        headers: {
          Authorization: 'Bearer ' + token,
          'X-Ignore-Auth-Redirect': '1'
        },
        credentials: 'include'
      });

      const ct = r.headers.get('content-type') || '';
      if (!ct.includes('application/json')) {
        // Most likely got HTML (login page) from a proxy. Avoid crashing.
        console.warn('Notifications returned non-JSON', r.status);
        console.warn('Tip: set CONFIG.API_BASE_URL (or CONFIG.apiUrl) to your API Gateway URL, not the SPA origin.');
        setItems([]);
        setUnread(0);
        return;
      }

      const data = await r.json();
      const arr = Array.isArray(data.items) ? data.items : [];
      setItems(arr);
      setUnread(arr.filter((n:any)=> !n.read).length);
    } catch(e) {
      console.error('notifications fetch failed', e);
    } finally {
      setLoading(false);
    }
  }

  React.useEffect(()=>{
    fetchNotifications(false);
    const id = setInterval(()=>fetchNotifications(false), 30000);
    return ()=>clearInterval(id);
  }, []);

  React.useEffect(()=>{
    function onDoc(e: MouseEvent){
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    }
    document.addEventListener('mousedown', onDoc);
    return ()=>document.removeEventListener('mousedown', onDoc);
  }, []);

  async function toggleOpen(){
    const next = !open;
    setOpen(next);
    if (next) {
      await fetchNotifications(true);
      setUnread(0);
    }
  }

  async function acceptFollow(fromUserId: string, id: string){
    try {
      let token = readIdTokenSync();
      if (!token) {
        try { token = await waitForIdToken(1000); } catch {}
      }
      if (!token) return;
      const API_BASE =
        (CONFIG as any).API_BASE_URL ||
        (CONFIG as any).apiUrl ||
        (window as any).CONFIG?.API_BASE_URL ||
        (window as any).CONFIG?.apiUrl ||
        (import.meta as any).env?.VITE_API_URL ||
        '';

      const url = API_BASE + '/follow-accept';
      await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token, 'X-Ignore-Auth-Redirect': '1' },
        body: JSON.stringify({ fromUserId })
      });
      setItems(prev => prev.filter(n => n.id !== id));
    } catch(e){
      console.error('accept follow failed', e);
    }
  }
  async function declineFollow(fromUserId: string, id: string){
    try {
      let token = readIdTokenSync();
      if (!token) {
        try { token = await waitForIdToken(1000); } catch {}
      }
      if (!token) return;
      const API_BASE =
        (CONFIG as any).API_BASE_URL ||
        (CONFIG as any).apiUrl ||
        (window as any).CONFIG?.API_BASE_URL ||
        (window as any).CONFIG?.apiUrl ||
        (import.meta as any).env?.VITE_API_URL ||
        '';

      const url = API_BASE + '/follow-decline';
      const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token, 'X-Ignore-Auth-Redirect': '1' },
        body: JSON.stringify({ fromUserId })
      });
      if (!res.ok) {
        const txt = await res.text().catch(()=>'');
        console.error('decline follow failed', res.status, txt);
        alert('Decline failed: ' + res.status + ' ' + (txt||''));
        return;
      }
      setItems(prev => prev.filter(n => n.id !== id));
    } catch(e){
      console.error('decline follow failed', e);
      alert(String(e));
    }
  }

  return (
    <div className="relative" ref={dropdownRef}>
      <button onClick={toggleOpen} type="button" className="relative inline-flex items-center gap-2 text-sm px-3 py-1.5 border rounded-lg hover:bg-gray-50">
        <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
          <path d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V4a2 2 0 10-4 0v1.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9"/>
        </svg>
        <span>Notifications</span>
        {unread > 0 && (
          <span className="absolute -top-1 -right-1 bg-red-500 text-white text-xs rounded-full px-1.5 py-0.5">{unread}</span>
        )}
      </button>

      
        {open && (
          <div
            initial={{ opacity: 0, y: -8 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -8 }}
            transition={{ duration: 0.15 }}
            className="absolute left-1/2 -translate-x-1/2 mt-2 w-96 max-w-[90vw] bg-white rounded-2xl shadow-lg border border-gray-200 z-50"
          >
            <div className="p-2 max-h-96 overflow-y-auto">
              {loading && (
                <div className="flex items-center justify-center py-3">
                  <div className="h-5 w-5 animate-spin rounded-full border-2 border-gray-300 border-t-gray-600"></div>
                </div>
              )}
              {!loading && items.length === 0 && (
                <div className="text-center text-gray-500 py-4 text-sm">No notifications</div>
              )}
              {!loading && items.map((n:any)=>{
                const avatar = mediaUrlFromKey(n.avatarKey || null);
                const handle = n.fromHandle || (n.fromUserId || '').slice(0,8);
                const userEl = n.userUrl
                  ? (<a href={n.userUrl} className="font-semibold hover:underline">@{handle}</a>)
                  : (<span className="font-semibold">@{handle}</span>);
                const msgEl = n.postUrl
                  ? (<a href={n.postUrl} className="hover:underline">{n.message || 'sent you a notification'}</a>)
                  : (<span>{n.message || 'sent you a notification'}</span>);
                return (
                  <div key={n.id} className="p-2 border-b border-gray-100 text-sm flex items-start gap-2">
                    {avatar ? (
                      <img src={avatar} alt="" className="h-7 w-7 rounded-full object-cover mt-0.5" />
                    ) : (
                      <div className="mt-1 h-2.5 w-2.5 rounded-full bg-indigo-500" />
                    )}
                    <div className="flex-1 min-w-0">
                      <p className="text-gray-800">
                        {userEl} {msgEl}
                      </p>
                      <p className="text-gray-400 text-xs mt-1">{new Date(n.createdAt || Date.now()).toLocaleString()}</p>
                    </div>
                    {n.type === 'follow_request' && (
                      <div className="flex gap-1">
                        <button onClick={(e)=>{ e.preventDefault(); e.stopPropagation(); acceptFollow(n.fromUserId, n.id); }} type="button" className="bg-green-500 text-white px-2 py-1 rounded-lg text-xs hover:bg-green-600">Accept</button>
                        <button type="button" onClick={(e)=>{ e.preventDefault(); e.stopPropagation(); declineFollow(n.fromUserId, n.id); }} className="bg-red-500 text-white px-2 py-1 rounded-lg text-xs hover:bg-red-600">Decline</button>
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
        )}
      
    </div>
  )
}

/* --------------------------- Layout (header) --------------------------- */
function Layout(){
  return (
    <div>
      <div className="sticky top-0 z-10 bg-white/80 backdrop-blur border-b border-gray-200">
        <div className="max-w-4xl mx-auto px-4 py-3 flex items-center gap-4">
          <Link to="/" className="flex items-center">
            <img
              src="https://scooterbooter-public.s3.us-east-1.amazonaws.com/scoot.png"
              alt=""
              className="h-7 w-auto object-contain select-none"
              draggable={false}
            />
            <span className="sr-only">Scooter Booter</span>
          </Link>

          <form action="/search" className="ml-auto hidden md:block">
            <input name="q" placeholder="Search users" className="border rounded-lg px-3 py-1.5 text-sm w-64"/>
          </form>
          <Link to="/search" className="md:hidden ml-auto text-sm text-gray-600">Search</Link>

          {/* NEW: global notifications button lives in the header, not inside Log out */}
          {(console.log('Header renders NotificationsDropdown'), (window as any).__SB_HEADER_MARK='has-nd', null) || <NotificationsDropdown/>}

          <Link to="/settings" className="hidden md:inline-flex items-center text-sm px-3 py-1.5 border rounded-lg hover:bg-gray-50">Settings</Link>

          {/* Keep Log out as its own button so clicking Notifications doesn’t trigger logout */}
          <button
              className="hidden md:inline-flex items-center text-sm px-3 py-1.5 border rounded-lg hover:bg-gray-50"
            onClick={logoutAndReload}
            aria-label="Log out"
            type="button"
          >
            Log out
          </button>
        </div>
      </div>
      <div className="max-w-4xl mx-auto px-4 py-6"><Outlet/></div>
    </div>
  )
}

/* ---------------------- Reactions + Comments UI ---------------------- */

function ReactionsRow({ token, postId }:{token:string; postId:string}) {
  const EMOJIS = ['👍','❤️','😂','🔥','👏'];
  const [counts, setCounts] = React.useState<Record<string,number>>({});
  const [mine, setMine] = React.useState<string[]>([]);
  const [loading, setLoading] = React.useState(false);

  // NEW: who + hover state
  const [who, setWho] = React.useState<Record<string, {userId:string,handle:string|null,avatarKey:string|null}[]>>({});
  const [hovered, setHovered] = React.useState<string | null>(null);

  const load = React.useCallback(async ()=>{
    if(!token) return;
    try {
      const r = await getReactions(token, postId);
      setCounts(r.counts || {});
      setMine(r.my || []);
    } catch {}
  }, [token, postId]);

  React.useEffect(()=>{ load() }, [load]);

  const ensureWhoLoaded = async ()=>{
    if (!token) return;
    if (Object.keys(who).length) return; // cached for this post row
    try {
      const r = await getReactionsWho(token, postId);
      setWho(r.who || {});
    } catch {}
  };

  const onToggle = async (emoji:string)=>{
    if (loading) return;
    setLoading(true);
    try {
      await toggleReaction(token, postId, emoji);
      const has = mine.includes(emoji);
      setMine(m => has ? m.filter(x=>x!==emoji) : [...m, emoji]);
      setCounts(c => ({...c, [emoji]: Math.max(0, (c[emoji]||0) + (has?-1:1))}));
      await load();
    } finally { setLoading(false) }
  };

  return (
    <div className="flex gap-2 relative">
      {EMOJIS.map(e => {
        const active = mine.includes(e);
        const cnt = counts[e] || 0;
        const showPopover = hovered === e && cnt > 0 && (who[e]?.length || 0) > 0;
        return (
          <div
            key={e}
            className="relative"
            onMouseEnter={async () => { if (cnt > 0) { setHovered(e); await ensureWhoLoaded(); } }}
            onMouseLeave={() => setHovered(h => (h === e ? null : h))}
          >
            <button
              onClick={()=>onToggle(e)}
              className={`px-2 py-1 rounded-full border text-sm leading-none ${active ? 'bg-indigo-50 border-indigo-200' : 'hover:bg-gray-50'}`}
              title={active ? `Remove ${e}` : `React ${e}`}
              type="button"
            >
              <span className="mr-1">{e}</span>
              {cnt > 0 && <span className="text-gray-600">{cnt}</span>}
            </button>

            {showPopover && (
              <div className="absolute z-20 mt-1 w-64 max-h-60 overflow-auto rounded-lg border bg-white shadow p-2">
                <div className="text-xs font-semibold mb-1">{e} reactions</div>
                <ul className="space-y-1">
                  {who[e]!.map(u => (
                    <li key={`${e}-${u.userId}`} className="flex items-center gap-2">
                      <img
                        src={mediaUrlFromKey(u.avatarKey) || 'https://placehold.co/20x20?text=👤'}
                        className="w-5 h-5 rounded-full border object-cover"
                        alt=""
                        draggable={false}
                      />
                      <a className="text-sm hover:underline" href={`/u/${u.handle ?? u.userId}`}>
                        @{u.handle ?? 'unknown'}
                      </a>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}


function CommentsBlock({ token, postId, my }:{token:string; postId:string; my?: any}) {
  const [open, setOpen] = React.useState(false);
  const [items, setItems] = React.useState<any[]>([]);
  const [cursor, setCursor] = React.useState<string|undefined>();
  const [text, setText] = React.useState('');
  const [loading, setLoading] = React.useState(false);

  // edit state
  const [editingId, setEditingId] = React.useState<string | null>(null);
  const [editText, setEditText] = React.useState<string>('');

  const load = async (c?:string)=>{
    if(!token || !open) return;
    setLoading(true);
    try{
      const r = await listComments(token, postId, c);
      setItems(prev => c ? [...prev, ...(r.items||[])] : (r.items||[]));
      setCursor(r.nextCursor);
    } finally { setLoading(false) }
  }

  React.useEffect(()=>{ if(open) load(undefined) }, [open, token, postId])

  const submit = async ()=>{
    if(!text.trim()) return;
    const t = text.trim();
    setText('');
    try{
      await addComment(token, postId, t);
      await load(undefined);
    }catch(e:any){ alert(e.message||String(e)) }
  }

  const saveEdit = async (id:string)=>{
    const t = editText.trim();
    if(!t) return;
    try {
      await updateComment(token, postId, id, t);
      setEditingId(null);
      setEditText('');
      await load(undefined);
    } catch(e:any){ alert(e.message||String(e)) }
  };

  const removeComment = async (id:string)=>{
    if(!window.confirm('Delete this comment?')) return;
    try {
      await deleteComment(token, postId, id);
      await load(undefined);
    } catch(e:any){ alert(e.message||String(e)) }
  };

  const isMine = (c:any)=>{
    const myId = my?.userId ?? null;
    const myHandle = (my?.handle ?? my?.username ?? '').toLowerCase();
    const cId = c.userId ?? null;
    const cHandle = (c.userHandle ?? '').toLowerCase();
    return (myId && cId && myId === cId) || (myHandle && cHandle && myHandle === cHandle);
  };

  return (
    <div className="mt-3">
      <button className="text-sm text-gray-600 hover:text-gray-900" onClick={()=>setOpen(o=>!o)}>
        {open ? 'Hide comments' : 'Show comments'}
      </button>
      {open && (
        <div className="mt-2 space-y-3">
          <div className="flex gap-2">
            <input
              className="flex-1 border rounded-lg px-3 py-2 text-sm"
              placeholder="Write a comment…"
              value={text}
              onChange={e=>setText(e.target.value)}
              onKeyDown={e=>{ if(e.key==='Enter' && !e.shiftKey){ e.preventDefault(); submit(); } }}
            />
            <button className="px-3 py-2 rounded-lg border text-sm" onClick={submit}>Send</button>
          </div>

          <div className="space-y-2">
            {items.map(c => {
              const mine = isMine(c);
              const isEditing = editingId === c.id;
              return (
                <div key={c.id} className="text-sm">
                  <div className="flex items-start gap-2">
                    <div className="flex-1">
                      <a href={`/u/${c.userHandle}`} className="font-medium hover:underline">@{c.userHandle}</a>
                      <span className="text-gray-500 ml-2">{new Date(c.createdAt).toLocaleString()}</span>
                      {!isEditing && <div className="whitespace-pre-wrap mt-0.5">{c.text}</div>}
                      {isEditing && (
                        <div className="mt-1 space-y-1">
                          <textarea className="w-full border rounded-lg px-3 py-2 text-sm" rows={2} value={editText} onChange={e=>setEditText(e.target.value)} />
                          <div className="flex gap-2">
                            <button className="px-2 py-1.5 text-sm rounded-lg border" onClick={()=>{ setEditingId(null); setEditText(''); }}>Cancel</button>
                            <button className="px-2 py-1.5 text-sm rounded-lg border bg-black text-white" onClick={()=>saveEdit(c.id)}>Save</button>
                          </div>
                        </div>
                      )}
                    </div>
                    {mine && !isEditing && (
                      <PostActionsMenu
                        onEdit={()=>{ setEditingId(c.id); setEditText(c.text || ''); }}
                        onDelete={()=>removeComment(c.id)}
                      />
                    )}
                  </div>
                </div>
              );
            })}
            {items.length===0 && !loading && <div className="text-sm text-gray-500">No comments yet.</div>}
            {cursor && (
              <div>
                <button className="text-sm text-gray-600 hover:text-gray-900" onClick={()=>load(cursor)} disabled={loading}>
                  {loading ? 'Loading…' : 'Load more'}
                </button>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}



/* ---------------------- Post actions menu (3 dots) ---------------------- */
function PostActionsMenu({ onEdit, onDelete }:{ onEdit:()=>void; onDelete:()=>void }){
  const [open, setOpen] = React.useState(false);
  const ref = React.useRef<HTMLDivElement>(null);

  React.useEffect(()=>{
    const onClick = (e: MouseEvent) => {
      if (!ref.current) return;
      if (!ref.current.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false); };
    document.addEventListener('click', onClick);
    document.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('click', onClick);
      document.removeEventListener('keydown', onKey);
    };
  },[]);

  return (
    <div ref={ref} className="relative">
      <button
        aria-label="Post options"
        onClick={(e)=>{ e.stopPropagation(); setOpen(o=>!o) }}
        className="p-1 rounded-full hover:bg-gray-100"
        type="button"
      >
        <EllipsisVerticalIcon className="w-5 h-5 text-gray-600" />
      </button>
      {open && (
        <div className="absolute right-0 mt-2 w-36 bg-white border border-gray-200 rounded-lg shadow-lg py-1 z-20">
          <button className="w-full text-left px-3 py-1.5 text-sm hover:bg-gray-50" onClick={()=>{ setOpen(false); onEdit(); }}>Edit</button>
          <button className="w-full text-left px-3 py-1.5 text-sm text-rose-600 hover:bg-rose-50" onClick={()=>{ setOpen(false); onDelete(); }}>Delete</button>
        </div>
      )}
    </div>
  );
}

function Feed(){
  const [token,setToken]=React.useState<string>(readIdTokenSync())
  const [editingId,setEditingId]=React.useState<string|null>(null);
  const [editText,setEditText]=React.useState('');
  const [editFile,setEditFile]=React.useState<File|null>(null);
  const [meData,setMeData]=React.useState<any|null>(null);
  const [items,setItems]=React.useState<any[]>([])
  const [text,setText]=React.useState('')
  const [file,setFile]=React.useState<File|null>(null)

  React.useEffect(()=>{ const id = Object.keys(localStorage).find(k=>k.includes('idToken')); if(id){ setToken(localStorage.getItem(id)||'') } },[])
  React.useEffect(()=>{ (async()=>{ if(!token) return; const f=await getFeed(token); setItems(f.items);
        try { const m = await me(token); setMeData(m) } catch {} })() },[token])

  if(!token) return <Gate/>

  return (
    <div className="grid md:grid-cols-3 gap-4">
      <div className="md:col-span-2 space-y-6">
        <Card>
          <h2 className="text-lg font-semibold mb-3">Create post</h2>
          <textarea className="w-full border rounded-xl p-3 mb-3" rows={3} placeholder="What's up?" value={text} onChange={e=>setText(e.target.value)}/>
          <div className="flex items-center justify-between">
            <label className="inline-flex items-center gap-2 cursor-pointer">
              <PhotoIcon className="w-5 h-5 text-gray-600"/>
              <input type="file" className="hidden" accept="image/*" onChange={e=>setFile(e.target.files?.[0]||null)}/>
              <span className="text-sm text-gray-700">{file?file.name:'Add a photo'}</span>
            </label>
            <button className="bg-indigo-600 text-white rounded-lg px-4 py-2" onClick={async()=>{ try{
                let imageKey: string | undefined
                if(file){
                  const id = Object.keys(localStorage).find(k=>k.includes('idToken'))!
                  const tok = localStorage.getItem(id)!
                  const { url, key } = await getUploadUrl(tok, file.type)
                  await fetch(url,{ method:'PUT', body:file, headers:{ 'Content-Type': file.type } })
                  imageKey = key
                }
                const id2 = Object.keys(localStorage).find(k=>k.includes('idToken'))!
                const tok2 = localStorage.getItem(id2)!
                await createPost(tok2, text, imageKey)
                setText(''); setFile(null)
                const f=await getFeed(tok2); setItems(f.items)
              } catch(e:any){ alert(e.message||String(e)) }
            }}>Post</button>
          </div>
        </Card>
        <div className="space-y-3">
          {items.map((it: any) => (
            <Card key={it.id} id={`post-${it.id}`}>
              <div className="flex items-center gap-2 mb-1">
                <img
                  src={mediaUrlFromKey(it.avatarKey) || 'https://placehold.co/32x32?text=👤'}
                  className="w-8 h-8 rounded-full object-cover border"
                  alt=""
                  draggable={false}
                />
                <a
                  className="font-semibold hover:underline"
                  href={`/u/${(it.handle || it.username || String(it.userId || '')).replace(/[^a-z0-9_-]/gi, '')}`}
                >
                  @{(it.handle || it.username || String(it.userId || '')).replace(/[^a-z0-9_-]/gi, '').slice(0, 32)}
                </a>
                <div className="ml-auto text-xs text-gray-500">
                  {new Date(it.createdAt).toLocaleString()}
                </div>
                {(meData && it.userId === meData.userId) && (
                  <PostActionsMenu
                    onEdit={() => { setEditingId(it.id); setEditText(it.text || ''); setEditFile(null); }}
                    onDelete={async () => {
                      if (!window.confirm('Delete this post?')) return;
                      const id = Object.keys(localStorage).find(k => k.includes('idToken'))!;
                      const tok = localStorage.getItem(id)!;
                      await deletePost(tok, it.id);
                      const f = await getFeed(tok); setItems(f.items);
                    }}
                  />
                )}
              </div>

              {it.imageKey && (
                <img
                  src={mediaUrlFromKey(it.imageKey)}
                  alt=""
                  className="rounded-lg w-full object-cover border"
                />
              )}

              {editingId === it.id ? (
                <div className="mt-2">
                  <textarea
                    className="w-full border rounded-lg p-2"
                    value={editText}
                    onChange={e => setEditText(e.target.value)}
                  />
                  <div className="flex gap-2 mt-2">
                    <button
                      className="px-3 py-1.5 rounded-lg border"
                      onClick={() => { setEditingId(null); setEditText(''); setEditFile(null); }}
                    >
                      Cancel
                    </button>
                    <button
                      className="px-3 py-1.5 rounded-lg bg-indigo-600 text-white"
                      onClick={async () => {
                        const id = Object.keys(localStorage).find(k => k.includes('idToken'))!;
                        const tok = localStorage.getItem(id)!;
                        await updatePost(tok, it.id, editText, undefined);
                        setEditingId(null); setEditText('');
                        const f = await getFeed(tok); setItems(f.items);
                      }}
                    >
                      Save
                    </button>
                  </div>
                </div>
              ) : (
                <p className="whitespace-pre-wrap break-words mt-2">{it.text}</p>
              )}

              <ReactionsRow token={token} postId={it.id} />
              <CommentsBlock token={token} postId={it.id} my={meData} />
            </Card>
          ))}
          {items.length === 0 && (
            <div className="text-center text-gray-500">No posts from people you follow yet.</div>
          )}
        </div>
        </div>    </div>
  )
}

/* ----------------------------- Admin ----------------------------- */
function Admin(){
  const [token,setToken]=React.useState('')
  const [uses,setUses]=React.useState(5)
  const [code,setCode]=React.useState<string|undefined>()

  React.useEffect(()=>{ const id=Object.keys(localStorage).find(k=>k.includes('idToken')); if(id){ setToken(localStorage.getItem(id)||'') } },[])
  if(!token) return null

  return (
    <Card>
      <h3 className="font-semibold mb-2 flex items-center gap-2"><KeyIcon className="w-5 h-5 text-amber-600"/>Admin</h3>
      <div className="text-sm text-gray-600 mb-2">Create invite codes</div>
      <div className="flex items-center gap-2">
        <label className="text-sm">Uses:</label>
        <input className="w-20 border rounded-lg px-2 py-1" type="number" min="1" max="100" value={uses} onChange={e=>setUses(parseInt(e.target.value||'1'))}/>
        <button className="ml-auto px-3 py-2 rounded-lg border" onClick={async()=>{
          try{ const r=await createInvite(token, uses); setCode(r.code) }catch(e:any){ alert(e.message||String(e)) }
        }}>Generate</button>
      </div>
      {code && <div className="mt-3 text-sm">New invite code: <code className="bg-gray-100 px-1.5 py-0.5 rounded">{code}</code></div>}
    </Card>
  )
}

/* ---------------------- Hover-avatar uploader ---------------------- */
function HoverAvatarUploader({
  token,
  keyCurrent,
  onSaved
}: { token: string; keyCurrent?: string | null; onSaved: (k: string) => void }) {
  const [busy, setBusy] = React.useState(false);
  const inputRef = React.useRef<HTMLInputElement>(null);
  const src = keyCurrent ? mediaUrlFromKey(keyCurrent) : undefined;

  const onPick = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    try {
      setBusy(true);
      const r1 = await fetch(`${CONFIG.apiUrl}/avatar-url`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ contentType: file.type || 'image/jpeg' })
      });
      if (!r1.ok) throw new Error(await r1.text());
      const { url, key } = await r1.json();

      await fetch(url, { method: 'PUT', body: file, headers: { 'Content-Type': file.type || 'image/jpeg' } });

      const r2 = await fetch(`${CONFIG.apiUrl}/me/avatar`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
        body: JSON.stringify({ key })
      });
      if (!r2.ok) throw new Error(await r2.text());
      onSaved(key);
    } catch (e: any) {
      alert(e?.message ?? String(e));
    } finally {
      setBusy(false);
      if (inputRef.current) inputRef.current.value = '';
    }
  };

  return (
    <div className="relative inline-block">
      <img
        src={src || 'https://placehold.co/64x64?text=👤'}
        className="w-16 h-16 rounded-full object-cover border"
        alt=""
      />
      <button
        type="button"
        aria-label="Change profile photo"
        onClick={() => inputRef.current?.click()}
        className="absolute inset-0 rounded-full bg-black/40 opacity-0 hover:opacity-100
                   transition-opacity flex items-center justify-center text-white text-xs"
        disabled={busy}
      >
        {busy ? 'Saving…' : 'Change'}
      </button>
      <input
        ref={inputRef}
        type="file"
        accept="image/*"
        className="hidden"
        onChange={onPick}
      />
    </div>
  );
}

/* ---------------------------- Profile ---------------------------- */
function Profile(){
  const params = useParams(); 
  const handleParam = params.handle!;

  const [token,setToken]=React.useState(readIdTokenSync());
  const [data,setData]=React.useState<any>(null);
  const [following, setFollowing] = React.useState(false);
  const [pendingFollow, setPendingFollow] = React.useState(false);
  const [meData, setMeData] = React.useState<any>(null)
  const [editingId,setEditingId]=React.useState<string|null>(null);
  const [editText,setEditText]=React.useState('');
  const [editFile,setEditFile]=React.useState<File|null>(null);;

  React.useEffect(()=>{ 
    const id=Object.keys(localStorage).find(k=>k.includes('idToken')); 
    if(id){ setToken(localStorage.getItem(id)||'') } 
  },[]);

  React.useEffect(()=>{ 
    (async()=>{ 
      if(!token) return; 
      const r=await getUser(token, handleParam); 
      setData(r); 
      setFollowing(r.isFollowing || false); 
      setPendingFollow(!!(r.isFollowPending || r.followStatus==='pending')); 
    })(); 
  },[token, handleParam]);

  React.useEffect(() => { 
    (async () => { 
      if (!token) return; 
      try { const m = await me(token); setMeData(m); } catch {} 
    })(); 
  }, [token]);

  if(!token) return <Gate/>;
  if(!data) return <div>Loading…</div>;

  // robust comparison across handle vs username (case-insensitive)
  const myHandle = (meData?.handle ?? meData?.username ?? '').toLowerCase();
  const theirHandle = (data?.handle ?? data?.username ?? handleParam ?? '').toLowerCase();
  const myId = meData?.userId ?? null;
  const theirId = data?.userId ?? null;
  const isOwnProfile = (myId && theirId && myId === theirId) || (myHandle !== '' && myHandle === theirHandle);

  const profileSlug = (data?.handle ?? data?.username ?? handleParam);
  const avatarSrc = mediaUrlFromKey(data.avatarKey) || 'https://placehold.co/64x64?text=👤';

  return (
    <div className="space-y-4">
      <Card>
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            {isOwnProfile ? (
              <HoverAvatarUploader
                token={token}
                keyCurrent={data.avatarKey}
                onSaved={(k) => {
                  setData((d:any) => ({ ...d, avatarKey: k }));
                  setMeData((m:any) => (m ? { ...m, avatarKey: k } : m));
                }}
              />
            ) : (
              <img
                src={avatarSrc}
                className="w-16 h-16 rounded-full object-cover border"
                alt=""
              />
            )}

            <div>
              <div className="text-2xl font-semibold">@{profileSlug}</div>
              {data.fullName && (<div className="text-base text-gray-900 mt-0.5">{data.fullName}</div>)}
              <div className="text-sm text-gray-500 flex gap-4 mt-1">
                <a className="hover:underline" href={`/u/${profileSlug}/followers`}><b>{data.followers ?? 0}</b> followers</a>
                <a className="hover:underline" href={`/u/${profileSlug}/following`}><b>{data.following ?? 0}</b> following</a>
                <span><b>{data.posts?.length||0}</b> posts</span>
              </div>
            </div>
          </div>

          {!isOwnProfile && (
            <button
              className={"px-4 py-2 rounded-lg border " + (following?"bg-gray-100":"bg-indigo-600 text-white")}
              onClick={async()=>{
                try{
                  if(following){
                    await unfollow(token, profileSlug);
                    setFollowing(false);
                    setPendingFollow(false);
                    setData((d:any)=>({ ...d, followers: Math.max(0, (d.followers||0)-1) }));
                  } else {
                    /* follow now creates a REQUEST instead of immediate follow */
                    {
                      const API_BASE =
        (CONFIG as any).API_BASE_URL ||
        (CONFIG as any).apiUrl ||
        (window as any).CONFIG?.API_BASE_URL ||
        (window as any).CONFIG?.apiUrl ||
        (import.meta as any).env?.VITE_API_URL ||
        '';

                      await fetch(API_BASE + '/follow-request', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
                        body: JSON.stringify({ handle: profileSlug })
                      });
                    
                      // lock UI immediately; also handle 409 from backend
                      try {
                        if (res && (res.ok || res.status === 409)) {
                          setPendingFollow(true);
                        }
                      } catch {}
}
                    // mark pending locally (optional UI)
                    setFollowing(false);
                    setData((d:any)=>({ ...d, followers: (d.followers||0) }));
                  }
                }catch(e:any){ alert(e.message||String(e)) }
              }}
             disabled={pendingFollow}
            >{pendingFollow ? "Pending" : (following ? "Unfollow" : "Follow")}</button>
          )}
        </div>
      </Card>

      <div className="space-y-3">
        {(data.posts || []).map((p:any)=>(
          <Card key={p.id}>
            <div className="flex items-center gap-2 mb-1">
              <div className="text-xs text-gray-500">{new Date(p.createdAt).toLocaleString()}</div>
              <div className="ml-auto flex items-center gap-1">
                {isOwnProfile && (
                  <PostActionsMenu
                    onEdit={()=>{ setEditingId(p.id); setEditText(p.text||''); setEditFile(null); }}
                    onDelete={async()=>{
                      if(!window.confirm('Delete this post?')) return;
                      const idKey=Object.keys(localStorage).find(k=>k.includes('idToken'))!;
                      const tok=localStorage.getItem(idKey)!;
                      await deletePost(tok, p.id);
                      const r=await getUser(tok, handleParam);
                      setData(r);
                    }}
                  />
                )}
              </div>
            </div>

            {editingId!==p.id && <div className="whitespace-pre-wrap mb-3">{p.text}</div>}

            {editingId===p.id && (
              <div className="mt-2 space-y-2">
                <textarea className="w-full border rounded-lg px-3 py-2 text-sm" rows={3} value={editText} onChange={e=>setEditText(e.target.value)} />
                <div className="flex items-center gap-2">
                  <input type="file" accept="image/*" onChange={e=>setEditFile(e.target.files?.[0]||null)} />
                  <label className="text-xs flex items-center gap-1">
                    <input type="checkbox" onChange={(e)=>{ if(e.target.checked){ setEditFile(null); (window as any).__removeImage=true } else { (window as any).__removeImage=false } }} />
                    Remove current image
                  </label>
                </div>
                <div className="flex gap-2">
                  <button className="px-3 py-1.5 text-sm rounded-lg border" onClick={()=>{ setEditingId(null); setEditFile(null); (window as any).__removeImage=false; }}>Cancel</button>
                  <button className="px-3 py-1.5 text-sm rounded-lg border bg-black text-white" onClick={async()=>{
                    try{
                      const idKey=Object.keys(localStorage).find(k=>k.includes('idToken'))!;
                      const tok=localStorage.getItem(idKey)!;
                      const payload:any = { text: editText };

                      if ((window as any).__removeImage) {
                        payload.deleteImage = true;
                      } else if (editFile) {
                        const { url, key } = await getUploadUrl(tok, editFile.type);
                        await fetch(url,{ method:'PUT', body: editFile, headers:{ 'Content-Type': editFile.type } });
                        payload.imageKey = key;
                      }

                      await updatePost(tok, p.id, payload);
                      setEditingId(null); setEditFile(null); (window as any).__removeImage=false;
                      const r=await getUser(tok, handleParam);
                      setData(r);
                    } catch(e:any){ alert(e.message||String(e)) }
                  }}>Save</button>
                </div>
              </div>
            )}

            {p.imageKey && (
              <div className="mt-2">
                <img
                  src={mediaUrlFromKey(p.imageKey)}
                  alt=""
                  className="max-h-96 w-full object-contain rounded-lg border"
                  draggable={false}
                />
              </div>
            )}

            <ReactionsRow token={token} postId={p.id} />
            <CommentsBlock token={token} postId={p.id} my={meData} />
          </Card>
        ))}
      </div>
    </div>
  );
}

/* --------------------------- Followers list --------------------------- */
function FollowList({ mode }: { mode: 'followers' | 'following' }) {
  const { handle } = useParams();
  const [token, setToken] = React.useState('');
  const [items, setItems] = React.useState<any[]>([]);
  const [cursor, setCursor] = React.useState<string | undefined>();
  const [loading, setLoading] = React.useState(false);
  const [meData, setMeData] = React.useState<any | null>(null);

  React.useEffect(() => {
    const id = Object.keys(localStorage).find(k => k.includes('idToken'));
    if (id) setToken(localStorage.getItem(id) || '');
  }, []);

  React.useEffect(() => {
    (async () => {
      if (!token) return;
      try {
        const m = await me(token);
        setMeData(m);
      } catch {}
    })();
  }, [token]);

  const load = async (c?: string) => {
    if (!token || !handle) return;
    setLoading(true);
    try {
      const r = mode === 'followers'
        ? await listFollowers(token, handle, c)
        : await listFollowing(token, handle, c);
      setItems(prev => (c ? [...prev, ...(r.items || [])] : (r.items || [])));
      setCursor(r.nextCursor);
    } finally {
      setLoading(false);
    }
  };

  React.useEffect(() => { load(undefined); }, [token, handle, mode]);

  return (
    <div className="max-w-xl mx-auto space-y-3">
      {items.map((u: any) => {
        const isSelf =
          (u.userId && meData?.userId && u.userId === meData.userId) ||
          (u.handle && meData?.handle && u.handle === meData.handle);

        return (
          <Card key={u.userId || u.handle} className="p-3">
            <div className="flex items-center gap-3">
              <img
                src={u.avatarKey ? mediaUrlFromKey(u.avatarKey) : undefined}
                alt=""
                className="w-10 h-10 rounded-full border object-cover bg-gray-100"
                draggable={false}
              />
              <div className="min-w-0">
                {u.fullName && <div className="font-medium truncate">{u.fullName}</div>}
                <a href={`/u/${u.handle}`} className="text-sm text-gray-600 hover:underline truncate">
                  @{u.handle}
                </a>
              </div>
              {!isSelf && (
                <div className="ml-auto">
                  <button
                    type="button"
                    aria-pressed={u.isFollowing ? 'true' : 'false'}
                    onClick={async (e) => {
                      e.preventDefault();
                      e.stopPropagation();
                      try {
                        if (u.isFollowing) {
                          await unfollow(token, u.handle);
                        } else {
                          await follow(token, u.handle);
                        }
                        u.isFollowing = !u.isFollowing;
                        setItems([...items]);
                      } catch (err) {
                        console.error('Follow toggle failed', err);
                        alert('Sorry, that failed. Try again.');
                      }
                    }}
                    className={'text-sm px-3 py-1.5 rounded-lg border ' + (u.isFollowing ? 'bg-gray-100 border-gray-300 text-gray-900' : 'bg-indigo-600 text-white border-indigo-600')}
                  >
                    {u.isFollowing ? 'Unfollow' : 'Follow'}
                  </button>
                </div>
              )}
            </div>
          </Card>
        );
      })}
      {items.length === 0 && !loading && (
        <div className="text-center text-gray-500">No {mode} yet.</div>
      )}
      {cursor && (
        <div className="text-center">
          <button
            className="px-3 py-2 rounded-lg border"
            onClick={() => load(cursor)}
            disabled={loading}
          >
            {loading ? 'Loading…' : 'Load more'}
          </button>
        </div>
      )}
    </div>
  );
}
/* ------------------------------ Search ------------------------------ */

function Search(){
  const location = useLocation();
  const [token, setToken] = React.useState(readIdTokenSync());
  const [q, setQ] = React.useState('');
  const [items, setItems] = React.useState<any[]>([]);
  const [cursor, setCursor] = React.useState<string|undefined>();
  const [loading, setLoading] = React.useState(false);
  const [selfHandle, setSelfHandle] = React.useState<string|undefined>(undefined);

  // hydrate from ?q=
  React.useEffect(()=>{
    const sp = new URLSearchParams(location.search || '');
    const q0 = sp.get('q') || '';
    if (q0) {
      setQ(q0);
      // run with explicit q0 to avoid race with setState
      run(undefined, q0);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [location.search]);

  // one-time sync of token from localStorage
  React.useEffect(()=>{
    const id = Object.keys(localStorage).find(k=>k && (k.endsWith('.idToken') || k.includes('idToken')));
    if (id) setToken(localStorage.getItem(id) || '');
  }, []);

  // if token missing initially, wait briefly
  React.useEffect(()=>{
    (async ()=>{
      if (token) return;
      try {
        const t = await waitForIdToken(3000);
        if (t) setToken(t);
      } catch {}
    })();
  }, [token]);

  // fetch self handle to hide self Follow button
  React.useEffect(()=>{
    (async ()=>{
      if (!token) return;
      try {
        const r = await me(token);
        if (r && r.handle) setSelfHandle(r.handle);
      } catch {}
    })();
  }, [token]);

  const run = async (cur?: string, qOverride?: string) => {
    let t = token;
    if (!t) {
      try { t = await waitForIdToken(3000); if(t) setToken(t); } catch {}
    }
    if (!t) { alert('Please sign in again.'); return; }
    const effectiveQ = (qOverride ?? q).trim();
    if (!effectiveQ) { setItems([]); return; }
    setLoading(true);
    try {
      const r = await searchUsers(t, effectiveQ, cur);
      if (cur) { setItems(prev => [...prev, ...(r.items || [])]); } else { setItems(r.items || []); }
      setCursor(r.nextCursor);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-xl mx-auto space-y-3">
      <Card>
        <div className="flex gap-2">
          <input value={q} onChange={e=>setQ(e.target.value)} className="flex-1 border rounded-lg px-3 py-2" placeholder="Search users or names"
            onKeyDown={(e)=>{ if(e.key==='Enter') run(undefined); }}
          />
          <button className="px-3 py-2 rounded-lg border" onClick={()=>run(undefined)}>Search</button>
        </div>
      </Card>
      {items.map((u:any)=>(<UserRow key={u.handle} initial={u} token={token} isSelf={selfHandle === u.handle}/>))}
      {cursor && <div className="text-center"><button className="px-3 py-2 rounded-lg border" disabled={loading} onClick={()=>run(cursor)}>Load more</button></div>}
    </div>
  );
}
function UserRow({initial, token, isSelf}:{initial:any, token:string, isSelf?:boolean}){
  const [user,setUser]=React.useState<any>(initial)
  return (
    <Card>
      <div className="flex items-center gap-3">
        <div className="flex flex-col">
          <a className="font-semibold hover:underline" href={`/u/${user.handle}`}>@{user.handle}</a>
          {user.fullName && <div className="text-sm text-gray-600">{user.fullName}</div>}
        </div>
        {!isSelf && (
          <div className="ml-auto">
            <button
              disabled={user?.isFollowPending}
              className={"px-3 py-1.5 rounded-lg border text-sm " + (user.isFollowing ? "bg-gray-100" : "bg-indigo-600 text-white")}
              onClick={async()=>{
                try {
                  if (user.isFollowing) { await unfollow(token, user.handle); setUser((u:any)=>({...u, isFollowing:false, isFollowPending:true})) }
                  else {
                  const API_BASE =
        (CONFIG as any).API_BASE_URL ||
        (CONFIG as any).apiUrl ||
        (window as any).CONFIG?.API_BASE_URL ||
        (window as any).CONFIG?.apiUrl ||
        (import.meta as any).env?.VITE_API_URL ||
        '';

                  await fetch(API_BASE + '/follow-request', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
                    body: JSON.stringify({ handle: user.handle })
                  });

                  // Make button show pending even if backend returns 409 (already pending)
                  try { if (res && (res.ok || res.status === 409)) { /* no-op; visual */ } } catch {}
                  setUser((u:any)=>({...u, isFollowing:false, isFollowPending:true}))
                }
                } catch (e:any) { alert(e.message||String(e)) }
              }}
            >
              {user.isFollowPending ? 'Pending' : (user.isFollowing ? 'Unfollow' : 'Follow')}
            </button>
          </div>
        )}
      </div>
    </Card>
  )
}

/* ------------------------------ Settings ------------------------------ */

/* ------------------------- Per-user invite code ------------------------- */
function InviteCodeBlock({ token, meData }:{ token:string, meData:any }){
  const [code,setCode] = React.useState<string>('');
  React.useEffect(()=>{
    if(!token || !meData?.userId) return;
    const key = `inviteCode.${meData.userId}`;
    const cached = localStorage.getItem(key);
    async function ensure(){
      try{
        if(cached){
          setCode(cached);
          return;
        }
        // Auto-generate a code with 10 uses and cache it
        const r = await createInvite(token, 10);
        if(r?.code){
          setCode(r.code);
          localStorage.setItem(key, r.code);
        }
      }catch(e){ console.error('invite gen failed', e); }
    }
    ensure();
  }, [token, meData?.userId]);

  return (
    <div className="bg-gray-50 border rounded-lg px-3 py-2">
      <div className="text-sm text-gray-600 mb-1">Your invite code</div>
      <div className="font-mono text-base">{code ? code : 'Generating…'}</div>
    </div>
  );
}

function Settings(){
  const [pwOld, setPwOld] = React.useState('');
  const [pwNew, setPwNew] = React.useState('');
  const [pwBusy, setPwBusy] = React.useState(false);
  const [pwMsg, setPwMsg] = React.useState<string|null>(null);

  const [token,setToken]=React.useState(readIdTokenSync());
  const [meData,setMeData]=React.useState<any|null>(null);
  React.useEffect(()=>{ const id=Object.keys(localStorage).find(k=>k.includes('idToken')); if(id){ setToken(localStorage.getItem(id)||'') } },[]);
  React.useEffect(()=>{ (async()=>{ if(!token) return; try{ const r=await me(token); setMeData(r);}catch(e){} })() },[token]);
  if(!token) return <Gate/>;
  if(!meData) return <div>Loading…</div>;
  return (
    <div className="space-y-4">
      <Card>
        <h2 className="text-lg font-semibold mb-3">Profile photo</h2>
        <HoverAvatarUploader token={token} keyCurrent={meData.avatarKey} onSaved={(k)=>setMeData((d:any)=> d ? {...d, avatarKey:k} : d)} />
        <p className="text-sm text-gray-500 mt-3">Tip: square images look best. Max ~5MB.</p>
    </Card>
    <Card>
      <h2 className="text-lg font-semibold mb-3">Invite friends</h2>
      <InviteCodeBlock token={token} meData={meData} />
      <p className="text-sm text-gray-500 mt-2">This code allows up to 10 people to sign up.</p>
    </Card>


    <Card>
      <h2 className="text-lg font-semibold mb-3">Profile</h2>
      <label className="block text-sm mb-1">Full name</label>
      <input
        className="w-full border rounded-lg px-3 py-2 mb-3"
        placeholder="e.g., Your Name"
        value={(meData.fullName ?? '') as any}
        onChange={(e)=>setMeData((d:any)=> d ? {...d, fullName:e.target.value} : d)}
      />
      <div className="flex gap-2">
        <button
          className="px-3 py-2 rounded-lg border"
          onClick={async()=>{
            try{
              const r = await updateMe(token, { fullName: (meData.fullName ?? '').trim() || null });
              setMeData((d:any)=> d ? {...d, fullName: r.fullName ?? null} : d);
              alert('Saved.');
            }catch(e:any){ alert(e.message || String(e)) }
          }}
        >
          Save changes
        </button>
      </div>
      <p className="text-xs text-gray-500 mt-2">Your full name appears on your profile, not on posts.</p>
    </Card>
  </div>
);
}


/* ===================== Auth Pages (split) ===================== */
function SignUpPage() {
  const [email, setEmail] = React.useState('');
  const [pw, setPw] = React.useState('');
  const [invite, setInvite] = React.useState('');
  const nav = useNavigate();

  return (
    <div className="max-w-lg mx-auto space-y-6">
      <Card>
        <h2 className="text-xl font-semibold mb-3">Create account</h2>

        <input
          className="w-full border rounded-lg px-3 py-2 mb-3"
          placeholder="email"
          value={email}
          onChange={e => setEmail(e.target.value)}
        />
        <input
          className="w-full border rounded-lg px-3 py-2 mb-3"
          placeholder="password"
          type="password"
          value={pw}
          onChange={e => setPw(e.target.value)}
        />
        <input
          className="w-full border rounded-lg px-3 py-2 mb-4"
          placeholder="invite code"
          value={invite}
          onChange={e => setInvite(e.target.value)}
        />

        <button
          className="w-full bg-indigo-600 text-white rounded-lg py-2"
          onClick={async () => {
            try {
              await signUp(email, pw, invite);
              // store pending creds for confirm page auto-login
              sessionStorage.setItem('pendingEmail', email);
              sessionStorage.setItem('pendingPw', pw);
              // navigate to dedicated confirm page
              nav('/confirm', { replace: true });
            } catch (e) {
              alert((e && (e as any).message) || String(e));
            }
          }}
        >
          Sign up
        </button>
      </Card>

      <p className="text-center text-sm text-gray-600">
        Already have an account?{' '}
        <button className="text-indigo-600 hover:underline" onClick={() => nav('/login')}>
          Log in
        </button>
      </p>
    </div>
  );
}

function LoginPage() {
  const [email, setEmail] = React.useState('');
  const [pw, setPw] = React.useState('');
  const nav = useNavigate();

  
  const [fpMode, setFpMode] = React.useState<0|1|2>(0);
  const [fpEmail, setFpEmail] = React.useState('');
  const [fpCode, setFpCode] = React.useState('');
  const [fpNew, setFpNew] = React.useState('');
  const [fpBusy, setFpBusy] = React.useState(false);
  const [fpMsg, setFpMsg] = React.useState<string|null>(null);
return (
    <div className="max-w-lg mx-auto space-y-6">
      <Card>
        <h2 className="text-xl font-semibold mb-3">Log in</h2>

        <input className="w-full border rounded-lg px-3 py-2 mb-3" placeholder="email" value={email} onChange={e=>setEmail(e.target.value)} />
        <input className="w-full border rounded-lg px-3 py-2 mb-4" placeholder="password" type="password" value={pw} onChange={e=>setPw(e.target.value)} />

        <button
          className="w-full bg-indigo-600 text-white rounded-lg py-2"
          onClick={async () => {
            try {
              const { idToken } = await login(email, pw);
              localStorage.setItem('idToken.manual', idToken);
              try {
                const r = await me(idToken);
                nav(r?.handle ? '/' : '/claim', { replace: true });
              } catch {
                nav('/');
              }
            } catch (e) {
              alert((e && (e as any).message) || String(e));
            }
          }}
        >
          Log in
        </button>
      </Card>

      <p className="text-center text-sm text-gray-600">
        
      <div className="mt-3 text-sm">
        {fpMode === 0 && (
          <button type="button" className="text-indigo-600 hover:underline"
            onClick={() => { setFpMode(1); setFpEmail(email || ''); setFpMsg(null); }}>
            Forgot password?
          </button>
        )}

        {fpMode === 1 && (
          <div className="mt-2 space-y-2 border rounded-lg p-3">
            <div className="font-medium">Reset your password</div>
            <input className="w-full border rounded-lg px-3 py-2" placeholder="Email"
              value={fpEmail} onChange={e=>setFpEmail(e.target.value)} />
            <div className="flex gap-2">
              <button type="button" className="px-3 py-2 rounded-lg bg-indigo-600 text-white"
                disabled={fpBusy || !fpEmail}
                onClick={async ()=>{ setFpBusy(true); setFpMsg(null);
                  try { await forgotPassword(fpEmail); setFpMode(2); setFpMsg('Check your email for the code.'); }
                  catch(e:any){ setFpMsg(e.message||String(e)); }
                  finally{ setFpBusy(false); }
                }}>
                Send reset email
              </button>
              <button type="button" className="px-3 py-2 rounded-lg border" onClick={()=>setFpMode(0)}>Cancel</button>
            </div>
            {fpMsg && <div className="text-xs text-gray-600">{fpMsg}</div>}
          </div>
        )}

        {fpMode === 2 && (
          <div className="mt-2 space-y-2 border rounded-lg p-3">
            <div className="font-medium">Enter code & new password</div>
            <input className="w-full border rounded-lg px-3 py-2" placeholder="6-digit code"
              value={fpCode} onChange={e=>setFpCode(e.target.value)} />
            <input type="password" className="w-full border rounded-lg px-3 py-2" placeholder="New password"
              value={fpNew} onChange={e=>setFpNew(e.target.value)} />
            <div className="flex gap-2">
              <button type="button" className="px-3 py-2 rounded-lg bg-indigo-600 text-white"
                disabled={fpBusy || !fpEmail || !fpCode || !fpNew}
                onClick={async ()=>{ setFpBusy(true); setFpMsg(null);
                  try { await confirmForgotPassword(fpEmail, fpCode, fpNew); setFpMsg('Password updated! You can now log in.'); setFpMode(0); }
                  catch(e:any){ setFpMsg(e.message||String(e)); }
                  finally{ setFpBusy(false); }
                }}>
                Confirm reset
              </button>
              <button type="button" className="px-3 py-2 rounded-lg border" onClick={()=>setFpMode(0)}>Cancel</button>
            </div>
            {fpMsg && <div className="text-xs text-gray-600">{fpMsg}</div>}
          </div>
        )}
      </div>

New to Scoot?{' '}
        <button className="text-indigo-600 hover:underline" onClick={() => nav('/signup')}>
          Create an account
        </button>
      </p>
    </div>
  );
}
/* =================== End Auth Pages (split) =================== */

/* ------------------------------ Router ------------------------------ */

function ConfirmPage() {
  const nav = useNavigate();

  const initialEmail = sessionStorage.getItem('pendingEmail') || '';
  const [email, setEmail] = React.useState(initialEmail);
  const [code, setCode] = React.useState('');
  const [loading, setLoading] = React.useState(false);
  const [resent, setResent] = React.useState(false);

  const onConfirm = async () => {
  setLoading(true);
  try {
    await confirmSignup(email, code);

    const pw = sessionStorage.getItem('pendingPw') || '';
    if (pw) {
      try {
        const { idToken } = await login(email, pw);
        localStorage.setItem('idToken.manual', idToken);

        const r = await me(idToken);

        sessionStorage.removeItem('pendingEmail');
        sessionStorage.removeItem('pendingPw');

        nav(r?.handle ? '/' : '/claim', { replace: true });
        return;
      } catch (_) {
        nav('/login', { replace: true });
        return;
      }
    }
    nav('/login', { replace: true });
  } catch (e) {
    alert((e && (e as any).message) || String(e));
  } finally {
    setLoading(false);
  }
};

  const onResend = async () => {
    if (!email) { alert('Enter your email first.'); return; }
    try {
      await resend(email);
      setResent(true);
      setTimeout(() => setResent(false), 3000);
    } catch (e) {
      alert((e && (e as any).message) || String(e));
    }
  };

  return (
    <div className="max-w-lg mx-auto space-y-6">
      <Card>
        <h2 className="text-xl font-semibold mb-3">Confirm your email</h2>
        <p className="text-sm text-gray-600 mb-3">
          We just sent a 6‑digit code to your email. Enter it below to activate your account.
        </p>
        <input
          className="w-full border rounded-lg px-3 py-2 mb-3"
          placeholder="email"
          value={email}
          onChange={e => setEmail(e.target.value)}
        />
        <div className="flex gap-2">
          <input
            className="flex-1 border rounded-lg px-3 py-2"
            placeholder="confirm code"
            value={code}
            onChange={e => setCode(e.target.value)}
          />
          <button className="border rounded-lg px-3 py-2" onClick={onConfirm} disabled={loading}>
            {loading ? 'Confirming…' : 'Confirm'}
          </button>
        </div>
        <div className="mt-3 flex items-center gap-3">
          <button className="text-sm text-indigo-600 hover:underline" onClick={onResend}>
            Resend code
          </button>
          {resent && <span className="text-sm text-green-600">Code sent ✔︎</span>}
        </div>
      </Card>

      <p className="text-center text-sm text-gray-600">
        Already confirmed?{' '}
        <button className="text-indigo-600 hover:underline" onClick={() => nav('/login')}>
          Log in
        </button>
      </p>
    </div>
  );
}

/* ------------------------------ Single Post Page ------------------------------ */
function PostPage(){
  const { postId } = useParams();
  const [item, setItem] = React.useState<any|null>(null);
  const [meData, setMeData] = React.useState<any|null>(null);
  const [loading, setLoading] = React.useState<boolean>(true);
  const [editingId, setEditingId] = React.useState<string|null>(null);
  const [editText, setEditText] = React.useState<string>('');
  const [editFile, setEditFile] = React.useState<File|null>(null);

  React.useEffect(()=>{
    (async()=>{
      try{
        let token = readIdTokenSync();
        if(!token){ try { token = await waitForIdToken(1200) } catch {} }
        if(!token){ setLoading(false); return; }
        const it = await getPost(token, postId!);
        setItem(it);
        try { const m = await me(token); setMeData(m) } catch {}
      } finally { setLoading(false); }
    })();
  }, [postId]);

  if(loading) return <div className="max-w-3xl mx-auto p-4">Loading…</div>;
  if(!item) return <div className="max-w-3xl mx-auto p-4">Post not found.</div>;

  const isMine = meData && item.userId === meData.userId;

  return (
    <div className="max-w-3xl mx-auto p-4">
      <Card id={`post-${item.id}`}>
        <div className="flex items-center gap-2 mb-1">
          <img
            src={mediaUrlFromKey(item.avatarKey) || 'https://placehold.co/32x32?text=👤'}
            className="w-8 h-8 rounded-full object-cover border"
            alt=""
            draggable={false}
          />
          <a
            className="font-semibold hover:underline"
            href={`/u/${(item.handle || item.username || String(item.userId || '')).replace(/[^a-z0-9_-]/gi, '')}`}
          >
            @{(item.handle || item.username || String(item.userId || '')).replace(/[^a-z0-9_-]/gi, '').slice(0, 32)}
          </a>
          <div className="ml-auto text-xs text-gray-500">
            {new Date(item.createdAt).toLocaleString()}
          </div>
          {isMine && (
            <PostActionsMenu
              onEdit={() => { setEditingId(item.id); setEditText(item.text || ''); setEditFile(null); }}
              onDelete={async () => {
                const id = Object.keys(localStorage).find(k => k.includes('idToken'))!;
                const tok = localStorage.getItem(id)!;
                await deletePost(tok, item.id);
                window.location.href = '/';
              }}
            />
          )}
        </div>

        {item.imageKey && (
          <img
            src={mediaUrlFromKey(item.imageKey)}
            alt=""
            className="rounded-xl border object-cover w-full max-h-[40rem]"
            draggable={false}
          />
        )}

        {editingId === item.id ? (
          <div className="mt-2">
            <textarea
              className="w-full border rounded-lg p-2"
              value={editText}
              onChange={e => setEditText(e.target.value)}
            />
            <div className="flex gap-2 mt-2">
              <button
                className="px-3 py-1.5 rounded-lg border"
                onClick={() => { setEditingId(null); setEditText(''); setEditFile(null); }}
              >
                Cancel
              </button>
              <button
                className="px-3 py-1.5 rounded-lg bg-indigo-600 text-white"
                onClick={async () => {
                  const id = Object.keys(localStorage).find(k => k.includes('idToken'))!;
                  const tok = localStorage.getItem(id)!;
                  await updatePost(tok, item.id, editText, undefined);
                  setEditingId(null); setEditText(''); setEditFile(null);
                  const fresh = await getPost(tok, item.id);
                  setItem(fresh);
                }}
              >
                Save
              </button>
            </div>
          </div>
        ) : (
          <p className="whitespace-pre-wrap break-words mt-2">{item.text}</p>
        )}

        <ReactionsRow token={readIdTokenSync() || ''} postId={item.id} />
        <CommentsBlock token={readIdTokenSync() || ''} postId={item.id} />
      </Card>
    </div>
  );
}
const router = createBrowserRouter([
  { path: "/", element: <Layout/>, children: [
    { index: true, element: <Feed/> },
    { path: "start", element: <Navigate to="/signup" replace /> },
    { path: "signup", element: <SignUpPage/> },
    { path: "login", element: <LoginPage/> },
    { path: "confirm", element: <ConfirmPage/> },
    { path: "u/:handle", element: <Profile/> },
    { path: "u/:handle/followers", element: <FollowList mode="followers"/> },
    { path: "u/:handle/following", element: <FollowList mode="following"/> },
    { path: "search", element: <Search/> },
    { path: "settings", element: <Settings/> },
        { path: "p/:postId", element: <PostPage/> },
    { path: "claim", element: <ClaimUsernamePage/> }
  ]},
])

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <RouterProvider router={router}/>
  </React.StrictMode>
)



function ClaimUsernamePage() {
  const [handle, setHandle] = React.useState('');
  const [saving, setSaving] = React.useState(false);
  const nav = useNavigate();

  const [token, setToken] = React.useState(readIdTokenSync());
  
  // If the browser/password manager autofills something weird (like an ID token), clear it.
  React.useEffect(() => {
    if (handle && (handle.split('.').length === 3 || handle.length > 24)) {
      setHandle('');
    }
  }, [handle]);
React.useEffect(() => {
    const manual = localStorage.getItem('idToken.manual');
    if (manual) { setToken(manual); return; }
    const key = Object.keys(localStorage).find(k => k.includes('idToken'));
    if (key) setToken(localStorage.getItem(key) || '');
  }, []);

  if (!token) return <Gate/>;

  const submit = async () => {
    const h = handle.trim().toLowerCase();
    if (!/^[a-z0-9_]{3,20}$/.test(h)) {
      alert('Handle must be 3–20 chars (letters, numbers, underscore).');
      return;
    }
    setSaving(true);
    try {
      await claimUsername(token, h);
      nav('/', { replace: true });
    } catch (e:any) {
      alert(e?.message || String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="max-w-lg mx-auto space-y-6">
      <div className="rounded-xl border p-5 shadow-sm">
        <h2 className="text-xl font-semibold mb-3">Choose your username</h2>
        <p className="text-sm text-gray-600 mb-3">Pick a handle to use across the app.</p>
        <input
          className="w-full border rounded-lg px-3 py-2 mb-3"
          placeholder="username (3–20 chars)" autoComplete="off" name="new-handle" spellCheck={false} autoCorrect="off" autoCapitalize="none"
          value={handle}
          onChange={e => setHandle(e.target.value)}
        />
        <button className="w-full bg-indigo-600 text-white rounded-lg py-2" onClick={submit} disabled={saving}>
          {saving ? 'Saving…' : 'Claim username'}
        </button>
      </div>
    </div>
  );
}