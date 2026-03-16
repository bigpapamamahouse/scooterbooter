import { confirm, confirmSignup, login, login as legacyLogin, resend, resend as legacyResend, signUp, signUp as legacySignUp, forgotPassword, confirmForgotPassword, changePassword } from './auth';
// main.tsx — modernized design with landing page

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


// Fallback for legacy references — redirect unauthenticated users to landing
function Gate(){ return <Navigate to="/login" replace /> }

import { mediaUrlFromKey } from './media'

import { PhotoIcon, KeyIcon, EllipsisVerticalIcon } from '@heroicons/react/24/outline'

/* ----------------------------- Small UI bits ----------------------------- */
const LOGO_URL = "https://scooterbooter-public.s3.us-east-1.amazonaws.com/scoot.png";
const APP_STORE_URL = "https://apps.apple.com/app/scooterbooter/id6738982498";

function Card(p:any){ return <div className="card-modern" {...(p.id ? {id:p.id} : {})}>{p.children}</div> }

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
      <button onClick={toggleOpen} type="button" className="relative inline-flex items-center gap-2 text-sm font-medium px-3.5 py-2 border border-gray-200 rounded-xl hover:bg-gray-50 hover:border-gray-300 transition-all">
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
    <div className="min-h-screen bg-gradient-to-b from-gray-50 to-white">
      <header className="sticky top-0 z-30 glass border-b border-gray-200/60">
        <div className="max-w-5xl mx-auto px-4 sm:px-6 py-3 flex items-center gap-3">
          <Link to="/app" className="flex items-center gap-2.5 group">
            <img
              src={LOGO_URL}
              alt=""
              className="h-8 w-auto object-contain select-none transition-transform group-hover:scale-105"
              draggable={false}
            />
            <span className="hidden sm:inline font-bold text-lg text-gray-900 tracking-tight">ScooterBooter</span>
          </Link>

          <form action="/search" className="ml-auto hidden md:block">
            <input name="q" placeholder="Search users..." className="input-modern w-64 !py-2 !text-sm"/>
          </form>
          <Link to="/search" className="md:hidden ml-auto p-2 rounded-xl hover:bg-gray-100 transition-colors">
            <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 text-gray-600" viewBox="0 0 20 20" fill="currentColor">
              <path fillRule="evenodd" d="M8 4a4 4 0 100 8 4 4 0 000-8zM2 8a6 6 0 1110.89 3.476l4.817 4.817a1 1 0 01-1.414 1.414l-4.816-4.816A6 6 0 012 8z" clipRule="evenodd"/>
            </svg>
          </Link>

          <NotificationsDropdown/>

          <Link to="/settings" className="hidden md:inline-flex items-center text-sm font-medium px-3.5 py-2 rounded-xl border border-gray-200 hover:bg-gray-50 hover:border-gray-300 transition-all">Settings</Link>

          <button
            className="hidden md:inline-flex items-center text-sm font-medium px-3.5 py-2 rounded-xl border border-gray-200 hover:bg-gray-50 hover:border-gray-300 transition-all"
            onClick={logoutAndReload}
            aria-label="Log out"
            type="button"
          >
            Log out
          </button>
        </div>
      </header>
      <main className="max-w-5xl mx-auto px-4 sm:px-6 py-8"><Outlet/></main>
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
              className={`px-2.5 py-1.5 rounded-full border text-sm leading-none transition-all duration-200 ${active ? 'bg-brand-50 border-brand-200 shadow-sm' : 'hover:bg-gray-50 hover:border-gray-300'}`}
              title={active ? `Remove ${e}` : `React ${e}`}
              type="button"
            >
              <span className="mr-1">{e}</span>
              {cnt > 0 && <span className="text-gray-600 font-medium">{cnt}</span>}
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
    <div className="grid md:grid-cols-3 gap-6">
      <div className="md:col-span-2 space-y-6">
        <Card>
          <h2 className="text-lg font-bold mb-3">Create post</h2>
          <textarea className="input-modern resize-none mb-3" rows={3} placeholder="What's on your mind?" value={text} onChange={e=>setText(e.target.value)}/>
          <div className="flex items-center justify-between">
            <label className="inline-flex items-center gap-2 cursor-pointer group">
              <PhotoIcon className="w-5 h-5 text-gray-400 group-hover:text-brand-600 transition-colors"/>
              <input type="file" className="hidden" accept="image/*" onChange={e=>setFile(e.target.files?.[0]||null)}/>
              <span className="text-sm text-gray-500 group-hover:text-gray-700 transition-colors">{file?file.name:'Add a photo'}</span>
            </label>
            <button className="btn-primary" onClick={async()=>{ try{
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
        <div className="space-y-4">
          {items.map((it: any) => (
            <Card key={it.id} id={`post-${it.id}`}>
              <div className="flex items-center gap-3 mb-3">
                <img
                  src={mediaUrlFromKey(it.avatarKey) || 'https://placehold.co/32x32?text=👤'}
                  className="w-10 h-10 rounded-full object-cover border-2 border-gray-100"
                  alt=""
                  draggable={false}
                />
                <div className="min-w-0">
                  <a
                    className="font-semibold text-gray-900 hover:text-brand-600 transition-colors"
                    href={`/u/${(it.handle || it.username || String(it.userId || '')).replace(/[^a-z0-9_-]/gi, '')}`}
                  >
                    @{(it.handle || it.username || String(it.userId || '')).replace(/[^a-z0-9_-]/gi, '').slice(0, 32)}
                  </a>
                  <div className="text-xs text-gray-400">
                    {new Date(it.createdAt).toLocaleString()}
                  </div>
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
                  className="rounded-xl w-full object-cover border border-gray-100 mb-2"
                />
              )}

              {editingId === it.id ? (
                <div className="mt-2">
                  <textarea
                    className="input-modern"
                    value={editText}
                    onChange={e => setEditText(e.target.value)}
                  />
                  <div className="flex gap-2 mt-3">
                    <button
                      className="btn-secondary !py-2 !px-4 !text-sm"
                      onClick={() => { setEditingId(null); setEditText(''); setEditFile(null); }}
                    >
                      Cancel
                    </button>
                    <button
                      className="btn-primary !py-2 !px-4 !text-sm"
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
            <div className="text-center py-12">
              <div className="text-4xl mb-3">📭</div>
              <p className="text-gray-500 font-medium">No posts yet</p>
              <p className="text-gray-400 text-sm mt-1">Follow some people to see their posts here</p>
            </div>
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
              className={(following ? "btn-secondary" : "btn-primary")}
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
                    className={'text-sm ' + (u.isFollowing ? 'btn-secondary !py-1.5 !px-3' : 'btn-primary !py-1.5 !px-3')}
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
              className={"text-sm " + (user.isFollowing ? "btn-secondary !py-1.5 !px-3" : "btn-primary !py-1.5 !px-3")}
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
  if(!meData) return <div className="flex items-center justify-center py-12"><div className="h-6 w-6 animate-spin rounded-full border-2 border-gray-300 border-t-brand-600"></div></div>;
  return (
    <div className="max-w-2xl mx-auto space-y-6 animate-fade-up">
      <h1 className="text-2xl font-bold text-gray-900">Settings</h1>
      <Card>
        <h2 className="text-lg font-bold mb-4">Profile photo</h2>
        <HoverAvatarUploader token={token} keyCurrent={meData.avatarKey} onSaved={(k)=>setMeData((d:any)=> d ? {...d, avatarKey:k} : d)} />
        <p className="text-sm text-gray-400 mt-3">Square images look best. Max ~5MB.</p>
    </Card>
    <Card>
      <h2 className="text-lg font-bold mb-4">Invite friends</h2>
      <InviteCodeBlock token={token} meData={meData} />
      <p className="text-sm text-gray-400 mt-2">This code allows up to 10 people to sign up.</p>
    </Card>

    <Card>
      <h2 className="text-lg font-bold mb-4">Profile</h2>
      <label className="block text-sm font-medium text-gray-700 mb-1.5">Full name</label>
      <input
        className="input-modern mb-4"
        placeholder="e.g., Your Name"
        value={(meData.fullName ?? '') as any}
        onChange={(e)=>setMeData((d:any)=> d ? {...d, fullName:e.target.value} : d)}
      />
      <div className="flex gap-2">
        <button
          className="btn-primary !text-sm"
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
      <p className="text-xs text-gray-400 mt-3">Your full name appears on your profile, not on posts.</p>
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
    <div className="max-w-md mx-auto space-y-6 animate-fade-up">
      <div className="text-center mb-2">
        <img src={LOGO_URL} alt="ScooterBooter" className="h-12 w-auto mx-auto mb-4" />
        <h1 className="text-2xl font-bold text-gray-900">Join ScooterBooter</h1>
        <p className="text-gray-500 text-sm mt-1">Create your account to get started</p>
      </div>
      <Card>
        <div className="space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1.5">Email</label>
          <input
            className="input-modern"
            placeholder="you@example.com"
            type="email"
            value={email}
            onChange={e => setEmail(e.target.value)}
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1.5">Password</label>
          <input
            className="input-modern"
            placeholder="Create a password"
            type="password"
            value={pw}
            onChange={e => setPw(e.target.value)}
          />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1.5">Invite code</label>
          <input
            className="input-modern"
            placeholder="Enter your invite code"
            value={invite}
            onChange={e => setInvite(e.target.value)}
          />
        </div>

        <button
          className="btn-primary w-full !py-3"
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
        </div>
      </Card>

      <p className="text-center text-sm text-gray-500">
        Already have an account?{' '}
        <button className="text-brand-600 font-semibold hover:text-brand-700 transition-colors" onClick={() => nav('/login')}>
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
    <div className="max-w-md mx-auto space-y-6 animate-fade-up">
      <div className="text-center mb-2">
        <img src={LOGO_URL} alt="ScooterBooter" className="h-12 w-auto mx-auto mb-4" />
        <h1 className="text-2xl font-bold text-gray-900">Welcome back</h1>
        <p className="text-gray-500 text-sm mt-1">Sign in to your account</p>
      </div>
      <Card>
        <div className="space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1.5">Email</label>
          <input className="input-modern" placeholder="you@example.com" type="email" value={email} onChange={e=>setEmail(e.target.value)} />
        </div>
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1.5">Password</label>
          <input className="input-modern" placeholder="Enter your password" type="password" value={pw} onChange={e=>setPw(e.target.value)} />
        </div>

        <button
          className="btn-primary w-full !py-3"
          onClick={async () => {
            try {
              const { idToken } = await login(email, pw);
              localStorage.setItem('idToken.manual', idToken);
              try {
                const r = await me(idToken);
                nav(r?.handle ? '/app' : '/claim', { replace: true });
              } catch {
                nav('/app');
              }
            } catch (e) {
              alert((e && (e as any).message) || String(e));
            }
          }}
        >
          Log in
        </button>

      <div className="text-sm pt-2">
        {fpMode === 0 && (
          <button type="button" className="text-brand-600 hover:text-brand-700 font-medium transition-colors"
            onClick={() => { setFpMode(1); setFpEmail(email || ''); setFpMsg(null); }}>
            Forgot password?
          </button>
        )}

        {fpMode === 1 && (
          <div className="mt-2 space-y-3 border border-gray-200 rounded-xl p-4 bg-gray-50/50">
            <div className="font-semibold text-gray-900">Reset your password</div>
            <input className="input-modern" placeholder="Email"
              value={fpEmail} onChange={e=>setFpEmail(e.target.value)} />
            <div className="flex gap-2">
              <button type="button" className="btn-primary !py-2 !text-sm"
                disabled={fpBusy || !fpEmail}
                onClick={async ()=>{ setFpBusy(true); setFpMsg(null);
                  try { await forgotPassword(fpEmail); setFpMode(2); setFpMsg('Check your email for the code.'); }
                  catch(e:any){ setFpMsg(e.message||String(e)); }
                  finally{ setFpBusy(false); }
                }}>
                Send reset email
              </button>
              <button type="button" className="btn-secondary !py-2 !text-sm" onClick={()=>setFpMode(0)}>Cancel</button>
            </div>
            {fpMsg && <div className="text-xs text-gray-600 mt-1">{fpMsg}</div>}
          </div>
        )}

        {fpMode === 2 && (
          <div className="mt-2 space-y-3 border border-gray-200 rounded-xl p-4 bg-gray-50/50">
            <div className="font-semibold text-gray-900">Enter code & new password</div>
            <input className="input-modern" placeholder="6-digit code"
              value={fpCode} onChange={e=>setFpCode(e.target.value)} />
            <input type="password" className="input-modern" placeholder="New password"
              value={fpNew} onChange={e=>setFpNew(e.target.value)} />
            <div className="flex gap-2">
              <button type="button" className="btn-primary !py-2 !text-sm"
                disabled={fpBusy || !fpEmail || !fpCode || !fpNew}
                onClick={async ()=>{ setFpBusy(true); setFpMsg(null);
                  try { await confirmForgotPassword(fpEmail, fpCode, fpNew); setFpMsg('Password updated! You can now log in.'); setFpMode(0); }
                  catch(e:any){ setFpMsg(e.message||String(e)); }
                  finally{ setFpBusy(false); }
                }}>
                Confirm reset
              </button>
              <button type="button" className="btn-secondary !py-2 !text-sm" onClick={()=>setFpMode(0)}>Cancel</button>
            </div>
            {fpMsg && <div className="text-xs text-gray-600 mt-1">{fpMsg}</div>}
          </div>
        )}
      </div>
        </div>
      </Card>

      <p className="text-center text-sm text-gray-500">
New to Scoot?{' '}
        <button className="text-brand-600 font-semibold hover:text-brand-700 transition-colors" onClick={() => nav('/signup')}>
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

        nav(r?.handle ? '/app' : '/claim', { replace: true });
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
    <div className="max-w-md mx-auto space-y-6 animate-fade-up">
      <div className="text-center mb-2">
        <img src={LOGO_URL} alt="ScooterBooter" className="h-12 w-auto mx-auto mb-4" />
        <h1 className="text-2xl font-bold text-gray-900">Confirm your email</h1>
        <p className="text-gray-500 text-sm mt-1">We sent a 6-digit code to your email</p>
      </div>
      <Card>
        <div className="space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1.5">Email</label>
          <input
            className="input-modern"
            placeholder="you@example.com"
            value={email}
            onChange={e => setEmail(e.target.value)}
          />
        </div>
        <div className="flex gap-2">
          <input
            className="input-modern flex-1"
            placeholder="6-digit code"
            value={code}
            onChange={e => setCode(e.target.value)}
          />
          <button className="btn-primary whitespace-nowrap" onClick={onConfirm} disabled={loading}>
            {loading ? 'Confirming...' : 'Confirm'}
          </button>
        </div>
        <div className="flex items-center gap-3">
          <button className="text-sm text-brand-600 font-medium hover:text-brand-700 transition-colors" onClick={onResend}>
            Resend code
          </button>
          {resent && <span className="text-sm text-green-600 font-medium">Code sent!</span>}
        </div>
        </div>
      </Card>

      <p className="text-center text-sm text-gray-500">
        Already confirmed?{' '}
        <button className="text-brand-600 font-semibold hover:text-brand-700 transition-colors" onClick={() => nav('/login')}>
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
                window.location.href = '/app';
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
  { path: "/", element: <LandingPage/> },
  { path: "/privacy", element: <PrivacyPolicyPage/> },
  { path: "/support", element: <SupportPage/> },
  { path: "/signup", element: <Layout/>, children: [{ index: true, element: <SignUpPage/> }] },
  { path: "/login", element: <Layout/>, children: [{ index: true, element: <LoginPage/> }] },
  { path: "/confirm", element: <Layout/>, children: [{ index: true, element: <ConfirmPage/> }] },
  { path: "/claim", element: <Layout/>, children: [{ index: true, element: <ClaimUsernamePage/> }] },
  { path: "/app", element: <Layout/>, children: [
    { index: true, element: <Feed/> },
    { path: "start", element: <Navigate to="/signup" replace /> },
  ]},
  { path: "/u/:handle", element: <Layout/>, children: [{ index: true, element: <Profile/> }] },
  { path: "/u/:handle/followers", element: <Layout/>, children: [{ index: true, element: <FollowList mode="followers"/> }] },
  { path: "/u/:handle/following", element: <Layout/>, children: [{ index: true, element: <FollowList mode="following"/> }] },
  { path: "/search", element: <Layout/>, children: [{ index: true, element: <Search/> }] },
  { path: "/settings", element: <Layout/>, children: [{ index: true, element: <Settings/> }] },
  { path: "/p/:postId", element: <Layout/>, children: [{ index: true, element: <PostPage/> }] },
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
      nav('/app', { replace: true });
    } catch (e:any) {
      alert(e?.message || String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="max-w-md mx-auto space-y-6 animate-fade-up">
      <div className="text-center mb-2">
        <img src={LOGO_URL} alt="ScooterBooter" className="h-12 w-auto mx-auto mb-4" />
        <h1 className="text-2xl font-bold text-gray-900">Choose your handle</h1>
        <p className="text-gray-500 text-sm mt-1">This is how people will find you</p>
      </div>
      <Card>
        <div className="space-y-4">
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1.5">Username</label>
          <div className="relative">
            <span className="absolute left-4 top-1/2 -translate-y-1/2 text-gray-400 font-medium">@</span>
            <input
              className="input-modern !pl-8"
              placeholder="yourname" autoComplete="off" name="new-handle" spellCheck={false} autoCorrect="off" autoCapitalize="none"
              value={handle}
              onChange={e => setHandle(e.target.value)}
            />
          </div>
          <p className="text-xs text-gray-400 mt-1.5">3-20 characters: letters, numbers, underscores</p>
        </div>
        <button className="btn-primary w-full !py-3" onClick={submit} disabled={saving}>
          {saving ? 'Saving...' : 'Claim username'}
        </button>
        </div>
      </Card>
    </div>
  );
}

/* ------------------------------ Landing Page ------------------------------ */
function LandingPage() {
  const nav = useNavigate();
  const token = readIdTokenSync();

  // If user is already logged in, redirect to feed
  if (token) return <Navigate to="/app" replace />;

  return (
    <div className="min-h-screen bg-white">
      {/* Navigation */}
      <nav className="sticky top-0 z-30 glass border-b border-gray-200/60">
        <div className="max-w-6xl mx-auto px-4 sm:px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <img src={LOGO_URL} alt="" className="h-9 w-auto" draggable={false} />
            <span className="font-bold text-xl text-gray-900 tracking-tight">ScooterBooter</span>
          </div>
          <div className="flex items-center gap-3">
            <button onClick={() => nav('/login')} className="btn-secondary !text-sm !px-4 !py-2">Log in</button>
            <button onClick={() => nav('/signup')} className="btn-primary !text-sm !px-4 !py-2">Sign up</button>
          </div>
        </div>
      </nav>

      {/* Hero Section */}
      <section className="relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-br from-brand-50 via-white to-purple-50"></div>
        <div className="absolute top-20 left-10 w-72 h-72 bg-brand-200/30 rounded-full blur-3xl"></div>
        <div className="absolute bottom-20 right-10 w-96 h-96 bg-purple-200/20 rounded-full blur-3xl"></div>

        <div className="relative max-w-6xl mx-auto px-4 sm:px-6 pt-20 pb-24 sm:pt-28 sm:pb-32">
          <div className="text-center max-w-3xl mx-auto">
            <div className="inline-flex items-center gap-2 bg-brand-50 border border-brand-200 rounded-full px-4 py-1.5 text-sm font-medium text-brand-700 mb-8 animate-fade-in">
              <span className="w-2 h-2 rounded-full bg-brand-500 animate-pulse"></span>
              Now available on iOS
            </div>

            <h1 className="text-5xl sm:text-6xl lg:text-7xl font-extrabold text-gray-900 tracking-tight leading-[1.1] text-balance animate-fade-up">
              Share what<br/>
              <span className="gradient-text">actually matters</span>
            </h1>

            <p className="mt-6 text-lg sm:text-xl text-gray-600 max-w-2xl mx-auto leading-relaxed animate-fade-up" style={{animationDelay:'0.1s'}}>
              ScooterBooter is the social platform built for real connections.
              Post, react, comment, and connect with the people you care about most.
            </p>

            <div className="mt-10 flex flex-col sm:flex-row items-center justify-center gap-4 animate-fade-up" style={{animationDelay:'0.2s'}}>
              <a href={APP_STORE_URL} target="_blank" rel="noopener noreferrer"
                className="inline-flex items-center gap-3 bg-gray-900 text-white rounded-2xl px-6 py-3.5 hover:bg-gray-800 transition-all hover:-translate-y-0.5 shadow-lg shadow-gray-900/20">
                <svg className="w-8 h-8" viewBox="0 0 24 24" fill="currentColor">
                  <path d="M18.71 19.5c-.83 1.24-1.71 2.45-3.05 2.47-1.34.03-1.77-.79-3.29-.79-1.53 0-2 .77-3.27.82-1.31.05-2.3-1.32-3.14-2.53C4.25 17 2.94 12.45 4.7 9.39c.87-1.52 2.43-2.48 4.12-2.51 1.28-.02 2.5.87 3.29.87.78 0 2.26-1.07 3.8-.91.65.03 2.47.26 3.64 1.98-.09.06-2.17 1.28-2.15 3.81.03 3.02 2.65 4.03 2.68 4.04-.03.07-.42 1.44-1.38 2.83M13 3.5c.73-.83 1.94-1.46 2.94-1.5.13 1.17-.34 2.35-1.04 3.19-.69.85-1.83 1.51-2.95 1.42-.15-1.15.41-2.35 1.05-3.11z"/>
                </svg>
                <div className="text-left">
                  <div className="text-[10px] uppercase tracking-wider opacity-80">Download on the</div>
                  <div className="text-lg font-semibold -mt-0.5">App Store</div>
                </div>
              </a>
              <button onClick={() => nav('/signup')}
                className="btn-primary !text-base !px-8 !py-3.5">
                Get started free
              </button>
            </div>
          </div>
        </div>
      </section>

      {/* Features Section */}
      <section className="py-20 sm:py-28 bg-gray-50/50">
        <div className="max-w-6xl mx-auto px-4 sm:px-6">
          <div className="text-center mb-16">
            <h2 className="text-3xl sm:text-4xl font-extrabold text-gray-900 tracking-tight">Everything you need to connect</h2>
            <p className="mt-4 text-lg text-gray-500 max-w-2xl mx-auto">A full-featured social experience designed with simplicity and privacy in mind.</p>
          </div>

          <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-6">
            {[
              { icon: '📝', title: 'Rich Posts', desc: 'Share text, photos, and express yourself with a full suite of creation tools.' },
              { icon: '💬', title: 'Real Conversations', desc: 'Comment on posts, react with emojis, and engage in meaningful discussions.' },
              { icon: '🔔', title: 'Stay in the Loop', desc: 'Real-time notifications for follows, comments, reactions, mentions, and more.' },
              { icon: '🔒', title: 'Privacy First', desc: 'Follow requests let you control who sees your content. Block and report with ease.' },
              { icon: '🗳️', title: 'Polls & Scoops', desc: 'Create polls to get opinions and share time-limited Scoops with your followers.' },
              { icon: '🎵', title: 'Spotify Integration', desc: 'Share what you\'re listening to with embedded Spotify tracks in your posts.' },
            ].map((f, i) => (
              <div key={i} className="card-modern group hover:border-brand-200 !p-6">
                <div className="text-3xl mb-4">{f.icon}</div>
                <h3 className="text-lg font-bold text-gray-900 mb-2">{f.title}</h3>
                <p className="text-gray-500 text-sm leading-relaxed">{f.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* How it works */}
      <section className="py-20 sm:py-28">
        <div className="max-w-6xl mx-auto px-4 sm:px-6">
          <div className="text-center mb-16">
            <h2 className="text-3xl sm:text-4xl font-extrabold text-gray-900 tracking-tight">Get started in seconds</h2>
            <p className="mt-4 text-lg text-gray-500">Three simple steps to join the community.</p>
          </div>

          <div className="grid sm:grid-cols-3 gap-8 max-w-4xl mx-auto">
            {[
              { step: '1', title: 'Get an invite', desc: 'Ask a friend for their invite code to join ScooterBooter.' },
              { step: '2', title: 'Create your profile', desc: 'Sign up, pick your @handle, and upload a profile photo.' },
              { step: '3', title: 'Start connecting', desc: 'Follow people, create posts, and join the conversation.' },
            ].map((s, i) => (
              <div key={i} className="text-center">
                <div className="w-14 h-14 mx-auto mb-5 rounded-2xl bg-brand-600 text-white flex items-center justify-center text-xl font-bold shadow-lg shadow-brand-600/25">
                  {s.step}
                </div>
                <h3 className="text-lg font-bold text-gray-900 mb-2">{s.title}</h3>
                <p className="text-gray-500 text-sm leading-relaxed">{s.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-20 sm:py-28 bg-gradient-to-br from-brand-600 to-brand-800 relative overflow-hidden">
        <div className="absolute top-0 left-1/4 w-96 h-96 bg-white/10 rounded-full blur-3xl"></div>
        <div className="absolute bottom-0 right-1/4 w-72 h-72 bg-purple-400/10 rounded-full blur-3xl"></div>
        <div className="relative max-w-3xl mx-auto px-4 sm:px-6 text-center">
          <h2 className="text-3xl sm:text-4xl font-extrabold text-white tracking-tight">Ready to join?</h2>
          <p className="mt-4 text-lg text-brand-100 max-w-xl mx-auto">Download ScooterBooter on iOS or sign up on the web to start connecting with the people you care about.</p>
          <div className="mt-10 flex flex-col sm:flex-row items-center justify-center gap-4">
            <a href={APP_STORE_URL} target="_blank" rel="noopener noreferrer"
              className="inline-flex items-center gap-3 bg-white text-gray-900 rounded-2xl px-6 py-3.5 font-semibold hover:bg-gray-100 transition-all hover:-translate-y-0.5 shadow-lg">
              <svg className="w-7 h-7" viewBox="0 0 24 24" fill="currentColor">
                <path d="M18.71 19.5c-.83 1.24-1.71 2.45-3.05 2.47-1.34.03-1.77-.79-3.29-.79-1.53 0-2 .77-3.27.82-1.31.05-2.3-1.32-3.14-2.53C4.25 17 2.94 12.45 4.7 9.39c.87-1.52 2.43-2.48 4.12-2.51 1.28-.02 2.5.87 3.29.87.78 0 2.26-1.07 3.8-.91.65.03 2.47.26 3.64 1.98-.09.06-2.17 1.28-2.15 3.81.03 3.02 2.65 4.03 2.68 4.04-.03.07-.42 1.44-1.38 2.83M13 3.5c.73-.83 1.94-1.46 2.94-1.5.13 1.17-.34 2.35-1.04 3.19-.69.85-1.83 1.51-2.95 1.42-.15-1.15.41-2.35 1.05-3.11z"/>
              </svg>
              Download on iOS
            </a>
            <button onClick={() => nav('/signup')}
              className="inline-flex items-center gap-2 bg-white/20 text-white rounded-2xl px-6 py-3.5 font-semibold hover:bg-white/30 transition-all border border-white/30 hover:-translate-y-0.5">
              Sign up on web
            </button>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t border-gray-200 py-12">
        <div className="max-w-6xl mx-auto px-4 sm:px-6">
          <div className="flex flex-col sm:flex-row items-center justify-between gap-6">
            <div className="flex items-center gap-2.5">
              <img src={LOGO_URL} alt="" className="h-7 w-auto" draggable={false} />
              <span className="font-bold text-gray-900">ScooterBooter</span>
            </div>
            <div className="flex items-center gap-6 text-sm text-gray-500">
              <a href="/privacy" className="hover:text-gray-900 transition-colors">Privacy Policy</a>
              <a href="/support" className="hover:text-gray-900 transition-colors">Support</a>
              <a href="mailto:contactscoot@yahoo.com" className="hover:text-gray-900 transition-colors">Contact</a>
            </div>
            <p className="text-sm text-gray-400">&copy; {new Date().getFullYear()} ScooterBooter</p>
          </div>
        </div>
      </footer>
    </div>
  );
}

/* ------------------------------ Privacy Policy ------------------------------ */
function PrivacyPolicyPage() {
  return (
    <div>
      {/* Header for standalone page */}
      <div className="sticky top-0 z-30 glass border-b border-gray-200/60">
        <div className="max-w-5xl mx-auto px-4 sm:px-6 py-3 flex items-center gap-4">
          <a href="/" className="flex items-center gap-2.5">
            <img src={LOGO_URL} alt="" className="h-8 w-auto object-contain select-none" draggable={false} />
            <span className="font-bold text-lg text-gray-900 tracking-tight">ScooterBooter</span>
          </a>
          <a href="/" className="ml-auto text-sm font-medium text-gray-500 hover:text-gray-900 transition-colors">Back to Home</a>
        </div>
      </div>

      <div className="max-w-4xl mx-auto px-4 py-6">
        <Card>
          <h1 className="text-3xl font-bold mb-2">Privacy Policy for ScooterBooter</h1>
          <p className="text-sm text-gray-600 mb-6">Last Updated: November 18, 2025</p>

        <div className="space-y-6 text-sm">
          <section>
            <h2 className="text-xl font-semibold mb-2">Introduction</h2>
            <p className="text-gray-700">
              ScooterBooter ("we," "us," or "our") operates the ScooterBooter mobile application (the "App"). This Privacy Policy explains how we collect, use, disclose, and safeguard your information when you use our App.
            </p>
            <p className="text-gray-700 mt-2">
              By using ScooterBooter, you agree to the collection and use of information in accordance with this policy. If you do not agree with our policies and practices, do not use the App.
            </p>
          </section>

          <section>
            <h2 className="text-xl font-semibold mb-2">1. Information We Collect</h2>

            <h3 className="text-lg font-semibold mt-4 mb-2">1.1 Information You Provide to Us</h3>
            <div className="ml-4">
              <p className="font-medium mb-1">Account Information:</p>
              <ul className="list-disc ml-6 space-y-1 text-gray-700">
                <li>Email address</li>
                <li>Password (securely encrypted and stored)</li>
                <li>Full name</li>
                <li>Username/handle</li>
                <li>Profile picture/avatar</li>
                <li>Invite code (during registration)</li>
              </ul>

              <p className="font-medium mt-3 mb-1">Content You Create:</p>
              <ul className="list-disc ml-6 space-y-1 text-gray-700">
                <li>Posts (text and images)</li>
                <li>Comments on posts</li>
                <li>Emoji reactions to posts</li>
                <li>Profile information updates</li>
              </ul>

              <p className="font-medium mt-3 mb-1">Social Interactions:</p>
              <ul className="list-disc ml-6 space-y-1 text-gray-700">
                <li>Follow requests sent and received</li>
                <li>Follower and following relationships</li>
                <li>User search queries</li>
              </ul>
            </div>

            <h3 className="text-lg font-semibold mt-4 mb-2">1.2 Information Automatically Collected</h3>
            <div className="ml-4">
              <p className="font-medium mb-1">Device and Technical Information:</p>
              <ul className="list-disc ml-6 space-y-1 text-gray-700">
                <li>Device type (iOS or Android)</li>
                <li>Push notification tokens</li>
                <li>App version</li>
                <li>Operating system version</li>
                <li>Device identifiers for push notifications</li>
              </ul>

              <p className="font-medium mt-3 mb-1">Usage Information:</p>
              <ul className="list-disc ml-6 space-y-1 text-gray-700">
                <li>Authentication tokens for maintaining your session</li>
                <li>Notification interaction data (read/unread status)</li>
                <li>App feature usage and interactions</li>
                <li>Cache data for app performance optimization</li>
              </ul>
            </div>

            <h3 className="text-lg font-semibold mt-4 mb-2">1.3 Information from Third Parties</h3>
            <p className="text-gray-700 ml-4">We do not collect information about you from third-party sources.</p>
          </section>

          <section>
            <h2 className="text-xl font-semibold mb-2">2. How We Use Your Information</h2>
            <p className="text-gray-700 mb-2">We use the information we collect to:</p>
            <ul className="list-disc ml-6 space-y-1 text-gray-700">
              <li><strong>Provide and Maintain the Service:</strong> Create and manage your account, authenticate your identity, and enable core app functionality</li>
              <li><strong>Social Features:</strong> Enable you to post content, comment, react to posts, follow other users, and receive notifications about social interactions</li>
              <li><strong>Communications:</strong> Send you push notifications about comments, reactions, mentions, and follow requests (with your permission)</li>
              <li><strong>Security:</strong> Protect against unauthorized access, fraud, and abuse</li>
              <li><strong>Compliance:</strong> Comply with legal obligations and enforce our Terms of Service</li>
            </ul>
          </section>

          <section>
            <h2 className="text-xl font-semibold mb-2">3. How We Share Your Information</h2>

            <h3 className="text-lg font-semibold mt-4 mb-2">3.1 Public Information</h3>
            <p className="text-gray-700 mb-2">The following information is visible to other users of the App:</p>
            <ul className="list-disc ml-6 space-y-1 text-gray-700">
              <li>Your profile picture, full name, and username</li>
              <li>Your posts and comments</li>
              <li>Your reactions to posts</li>
              <li>Your follower and following counts</li>
              <li>Users you follow and who follow you (depending on privacy settings)</li>
            </ul>

            <h3 className="text-lg font-semibold mt-4 mb-2">3.2 Service Providers</h3>
            <p className="text-gray-700 mb-2">We share your information with trusted third-party service providers who assist us in operating the App:</p>
            <ul className="list-disc ml-6 space-y-1 text-gray-700">
              <li><strong>Amazon Web Services (AWS):</strong> We use AWS services for authentication (Cognito), data storage, API infrastructure, and content delivery. Your data is stored in AWS's us-east-1 region.</li>
              <li><strong>Expo Push Notification Service:</strong> We share device push tokens with Expo to deliver notifications about app activity.</li>
            </ul>
            <p className="text-gray-700 mt-2">These service providers are contractually obligated to protect your information and use it only for the purposes we specify.</p>

            <h3 className="text-lg font-semibold mt-4 mb-2">3.3 Legal Requirements</h3>
            <p className="text-gray-700">We may disclose your information if required to do so by law or in response to valid requests by public authorities (e.g., a court order or government agency).</p>

            <h3 className="text-lg font-semibold mt-4 mb-2">3.4 Business Transfers</h3>
            <p className="text-gray-700">If we are involved in a merger, acquisition, or sale of assets, your information may be transferred as part of that transaction.</p>
          </section>

          <section>
            <h2 className="text-xl font-semibold mb-2">4. Data Storage and Security</h2>

            <h3 className="text-lg font-semibold mt-4 mb-2">4.1 Data Storage</h3>
            <ul className="list-disc ml-6 space-y-1 text-gray-700">
              <li><strong>Cloud Storage:</strong> Your account information, posts, comments, and social interactions are stored securely on Amazon Web Services infrastructure in the United States (us-east-1 region).</li>
              <li><strong>Local Device Storage:</strong> Authentication tokens, notification preferences, and app settings are stored locally on your device using encrypted storage (iOS Keychain on iOS devices, Android Keystore on Android devices).</li>
              <li><strong>Media Storage:</strong> Images you upload are stored on Amazon S3 and delivered via Amazon CloudFront CDN.</li>
            </ul>

            <h3 className="text-lg font-semibold mt-4 mb-2">4.2 Security Measures</h3>
            <p className="text-gray-700 mb-2">We implement industry-standard security measures to protect your information:</p>
            <ul className="list-disc ml-6 space-y-1 text-gray-700">
              <li>Passwords are encrypted and never stored in plain text</li>
              <li>All data transmission uses HTTPS encryption</li>
              <li>Authentication tokens are securely stored and automatically refreshed</li>
              <li>Access to backend systems is restricted and monitored</li>
            </ul>
            <p className="text-gray-700 mt-2">However, no method of transmission over the internet or electronic storage is 100% secure. While we strive to protect your personal information, we cannot guarantee absolute security.</p>
          </section>

          <section>
            <h2 className="text-xl font-semibold mb-2">5. Data Retention</h2>
            <p className="text-gray-700 mb-2">We retain your information for as long as your account is active or as needed to provide you services. Specifically:</p>
            <ul className="list-disc ml-6 space-y-1 text-gray-700">
              <li><strong>Account Data:</strong> Retained until you delete your account through the App settings. Upon account deletion, all your data is immediately and permanently removed from our active systems.</li>
              <li><strong>Posts and Comments:</strong> Retained until you delete them individually, or until you delete your account (which removes all posts and comments)</li>
              <li><strong>Authentication Tokens:</strong> Automatically refreshed and cleared when you sign out</li>
              <li><strong>Local Cache:</strong> Automatically cleared every 5 minutes and when you sign out</li>
              <li><strong>Notification History:</strong> Retained for your reference; older notifications may be purged. All notifications are deleted when you delete your account.</li>
              <li><strong>Backup Data:</strong> Deleted data may remain in system backups for a limited period (typically 30-90 days) for disaster recovery purposes, but will not be accessible or used</li>
            </ul>
          </section>

          <section>
            <h2 className="text-xl font-semibold mb-2">6. Your Privacy Rights and Choices</h2>

            <h3 className="text-lg font-semibold mt-4 mb-2">6.1 Access and Update</h3>
            <p className="text-gray-700">You can access and update your profile information at any time through the App's settings.</p>

            <h3 className="text-lg font-semibold mt-4 mb-2">6.2 Delete Content</h3>
            <p className="text-gray-700">You can delete your posts and comments at any time through the App.</p>

            <h3 className="text-lg font-semibold mt-4 mb-2">6.3 Account Deletion</h3>
            <p className="text-gray-700 mb-2">You can delete your account at any time directly from the App's Settings screen. When you delete your account:</p>

            <p className="font-medium mt-3 mb-1">What Gets Deleted Immediately:</p>
            <ul className="list-disc ml-6 space-y-1 text-gray-700">
              <li>Your user profile (name, email, username, avatar)</li>
              <li>All posts you've created</li>
              <li>All comments you've made</li>
              <li>All reactions you've given to posts</li>
              <li>All follow relationships (users you follow and users who follow you)</li>
              <li>All notifications associated with your account</li>
              <li>All invite codes you've created</li>
              <li>Your authentication credentials</li>
            </ul>

            <p className="font-medium mt-3 mb-1">Important Notes:</p>
            <ul className="list-disc ml-6 space-y-1 text-gray-700">
              <li>Account deletion is <strong>permanent and cannot be undone</strong></li>
              <li>The deletion process happens immediately upon confirmation</li>
              <li>Your content will no longer be visible to other users</li>
              <li>You will be automatically signed out after deletion</li>
              <li>Some data may be retained in system backups for a limited period (typically 30-90 days) for disaster recovery purposes, but will not be accessible or used</li>
            </ul>
            <p className="text-gray-700 mt-2">If you have questions about account deletion, you can contact us at contactscoot@yahoo.com.</p>

            <h3 className="text-lg font-semibold mt-4 mb-2">6.4 Push Notifications</h3>
            <p className="text-gray-700 mb-2">You can opt out of push notifications by:</p>
            <ul className="list-disc ml-6 space-y-1 text-gray-700">
              <li>Denying notification permissions when prompted by the App</li>
              <li>Disabling notifications in your device settings</li>
            </ul>

            <h3 className="text-lg font-semibold mt-4 mb-2">6.5 Marketing Communications</h3>
            <p className="text-gray-700">We do not send marketing communications at this time. If we do in the future, you will be able to opt out.</p>

            <h3 className="text-lg font-semibold mt-4 mb-2">6.6 Your Legal Rights</h3>
            <p className="text-gray-700 mb-2">Depending on your location, you may have the following rights:</p>

            <p className="font-medium mt-3 mb-1">For California Residents (CCPA):</p>
            <ul className="list-disc ml-6 space-y-1 text-gray-700">
              <li>Right to know what personal information is collected</li>
              <li>Right to delete personal information</li>
              <li>Right to opt-out of sale of personal information (Note: We do not sell personal information)</li>
              <li>Right to non-discrimination for exercising your rights</li>
            </ul>

            <p className="font-medium mt-3 mb-1">For EU/UK Residents (GDPR):</p>
            <ul className="list-disc ml-6 space-y-1 text-gray-700">
              <li>Right to access your personal data</li>
              <li>Right to rectification of inaccurate data</li>
              <li>Right to erasure ("right to be forgotten")</li>
              <li>Right to restrict processing</li>
              <li>Right to data portability</li>
              <li>Right to object to processing</li>
              <li>Right to withdraw consent</li>
            </ul>
            <p className="text-gray-700 mt-2">To exercise these rights, please contact us at contactscoot@yahoo.com.</p>
          </section>

          <section>
            <h2 className="text-xl font-semibold mb-2">7. Children's Privacy</h2>
            <p className="text-gray-700">
              ScooterBooter is not intended for children under the age of 13. We do not knowingly collect personal information from children under 13. If you are a parent or guardian and believe your child has provided us with personal information, please contact us at contactscoot@yahoo.com, and we will delete such information from our systems.
            </p>
          </section>

          <section>
            <h2 className="text-xl font-semibold mb-2">8. International Data Transfers</h2>
            <p className="text-gray-700">
              Your information may be transferred to and maintained on servers located outside of your state, province, country, or other governmental jurisdiction where data protection laws may differ. By using the App, you consent to the transfer of your information to the United States and other jurisdictions where we operate.
            </p>
          </section>

          <section>
            <h2 className="text-xl font-semibold mb-2">9. Third-Party Links</h2>
            <p className="text-gray-700">
              The App may contain links to third-party websites or services. We are not responsible for the privacy practices of these third parties. We encourage you to read their privacy policies.
            </p>
          </section>

          <section>
            <h2 className="text-xl font-semibold mb-2">10. Do Not Track Signals</h2>
            <p className="text-gray-700">
              We do not track users over time and across third-party websites, and therefore do not respond to Do Not Track (DNT) signals.
            </p>
          </section>

          <section>
            <h2 className="text-xl font-semibold mb-2">11. Changes to This Privacy Policy</h2>
            <p className="text-gray-700 mb-2">We may update this Privacy Policy from time to time. We will notify you of any changes by:</p>
            <ul className="list-disc ml-6 space-y-1 text-gray-700">
              <li>Posting the new Privacy Policy in the App</li>
              <li>Updating the "Last Updated" date at the top of this policy</li>
              <li>Sending you a notification through the App or email (for material changes)</li>
            </ul>
            <p className="text-gray-700 mt-2">Your continued use of the App after changes are posted constitutes your acceptance of the updated Privacy Policy.</p>
          </section>

          <section>
            <h2 className="text-xl font-semibold mb-2">12. Contact Us</h2>
            <p className="text-gray-700">
              If you have any questions, concerns, or requests regarding this Privacy Policy or our privacy practices, please contact us at:
            </p>
            <p className="text-gray-700 mt-2"><strong>Email:</strong> contactscoot@yahoo.com</p>
          </section>

          <section className="border-t pt-4 mt-6">
            <p className="text-center text-gray-600 italic">
              By using ScooterBooter, you acknowledge that you have read and understood this Privacy Policy.
            </p>
          </section>
        </div>
      </Card>
      </div>
    </div>
  );
}

/* ------------------------------ Support Page ------------------------------ */
function SupportPage() {
  return (
    <div>
      {/* Header for standalone page */}
      <div className="sticky top-0 z-30 glass border-b border-gray-200/60">
        <div className="max-w-5xl mx-auto px-4 sm:px-6 py-3 flex items-center gap-4">
          <a href="/" className="flex items-center gap-2.5">
            <img src={LOGO_URL} alt="" className="h-8 w-auto object-contain select-none" draggable={false} />
            <span className="font-bold text-lg text-gray-900 tracking-tight">ScooterBooter</span>
          </a>
          <a href="/" className="ml-auto text-sm font-medium text-gray-500 hover:text-gray-900 transition-colors">Back to Home</a>
        </div>
      </div>

      <div className="max-w-4xl mx-auto px-4 py-6">
        <Card>
          <h1 className="text-3xl font-bold mb-2">Support</h1>
          <p className="text-sm text-gray-600 mb-6">Get help with ScooterBooter</p>

          <div className="space-y-6 text-sm">
            <section>
              <h2 className="text-xl font-semibold mb-2">Contact Us</h2>
              <p className="text-gray-700 mb-3">
                Need help or have questions about ScooterBooter? We're here to help!
              </p>
              <div className="bg-gray-50 border rounded-lg p-4">
                <p className="font-medium mb-2">Email Support</p>
                <p className="text-gray-700">
                  <strong>Email:</strong> <a href="mailto:contactscoot@yahoo.com" className="text-indigo-600 hover:underline">contactscoot@yahoo.com</a>
                </p>
                <p className="text-gray-600 text-xs mt-2">
                  We typically respond within 24-48 hours during business days.
                </p>
              </div>
            </section>

            <section>
              <h2 className="text-xl font-semibold mb-2">Frequently Asked Questions</h2>

              <div className="space-y-4">
                <div>
                  <h3 className="font-semibold mb-1">How do I create an account?</h3>
                  <p className="text-gray-700">
                    You'll need an invite code to sign up for ScooterBooter. Once you have a code, visit the signup page, enter your email, password, and invite code to create your account.
                  </p>
                </div>

                <div>
                  <h3 className="font-semibold mb-1">How do I reset my password?</h3>
                  <p className="text-gray-700">
                    On the login page, click "Forgot password?" and follow the instructions. You'll receive a reset code via email.
                  </p>
                </div>

                <div>
                  <h3 className="font-semibold mb-1">How do I delete my account?</h3>
                  <p className="text-gray-700">
                    Go to Settings in the app and you'll find the option to delete your account. Please note that account deletion is permanent and cannot be undone.
                  </p>
                </div>

                <div>
                  <h3 className="font-semibold mb-1">How do I change my username?</h3>
                  <p className="text-gray-700">
                    Currently, usernames cannot be changed after they are set. Please contact support if you need assistance.
                  </p>
                </div>

                <div>
                  <h3 className="font-semibold mb-1">How do I report inappropriate content?</h3>
                  <p className="text-gray-700">
                    Please email us at <a href="mailto:contactscoot@yahoo.com" className="text-indigo-600 hover:underline">contactscoot@yahoo.com</a> with details about the content and we'll review it promptly.
                  </p>
                </div>
              </div>
            </section>

            <section>
              <h2 className="text-xl font-semibold mb-2">Privacy & Security</h2>
              <p className="text-gray-700">
                For information about how we handle your data, please review our <a href="/privacy" className="text-indigo-600 hover:underline">Privacy Policy</a>.
              </p>
            </section>

            <section>
              <h2 className="text-xl font-semibold mb-2">Feature Requests & Feedback</h2>
              <p className="text-gray-700">
                We love hearing from our users! If you have ideas for new features or feedback about the app, please reach out to us at <a href="mailto:contactscoot@yahoo.com" className="text-indigo-600 hover:underline">contactscoot@yahoo.com</a>.
              </p>
            </section>

            <section className="border-t pt-4 mt-6">
              <p className="text-center text-gray-600 italic">
                Thank you for using ScooterBooter!
              </p>
            </section>
          </div>
        </Card>
      </div>
    </div>
  );
}