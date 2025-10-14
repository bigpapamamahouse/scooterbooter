import React from 'react'
import ReactDOM from 'react-dom/client'
import { createBrowserRouter, RouterProvider, Link, Outlet, useNavigate, useParams } from 'react-router-dom'
import './index.css'
import { login, signUp, confirm } from './auth'
import {
  me, claimUsername, getFeed, getUser, follow, unfollow,
  listFollowers, listFollowing, searchUsers,
  getUploadUrl, createPost, createInvite,
  // NEW:
  listComments, addComment, getReactions, toggleReaction,
} from './api'
import { UserCircleIcon, PhotoIcon, KeyIcon } from '@heroicons/react/24/outline'

function Card(p:any){ return <div className="bg-white rounded-2xl shadow-sm border border-gray-200 p-5">{p.children}</div> }

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
        </div>
      </div>
      <div className="max-w-4xl mx-auto px-4 py-6"><Outlet/></div>
    </div>
  )
}

/* --------------------------- Auth gate --------------------------- */
function Gate(){
  const [email,setEmail]=React.useState('')
  const [pw,setPw]=React.useState('')
  const [invite,setInvite]=React.useState('')
  const [code,setCode]=React.useState('')
  const [token,setToken]=React.useState<string>('')
  const [handle,setHandle]=React.useState<string|null>(null)
  const nav=useNavigate()

  React.useEffect(()=>{ (async()=>{ if(!token) return; const r=await me(token); setHandle(r.handle); if(r.handle) nav('/'); })() },[token])

  if(!token) return (
    <div className="max-w-lg mx-auto space-y-6">
      <Card>
        <h2 className="text-xl font-semibold mb-3">Create account</h2>
        <input className="w-full border rounded-lg px-3 py-2 mb-2" placeholder="email" value={email} onChange={e=>setEmail(e.target.value)}/>
        <input className="w-full border rounded-lg px-3 py-2 mb-2" type="password" placeholder="password" value={pw} onChange={e=>setPw(e.target.value)}/>
        <input className="w-full border rounded-lg px-3 py-2 mb-3" placeholder="invite code" value={invite} onChange={e=>setInvite(e.target.value)}/>
        <div className="flex gap-2">
          <button className="flex-1 bg-indigo-600 text-white rounded-lg py-2" onClick={async()=>{
            try{ await signUp(email,pw,invite); alert('Check email for confirmation code'); }catch(e:any){ alert(e.message||String(e)) }
          }}>Sign up</button>
          <button className="flex-1 border rounded-lg py-2" onClick={async()=>{
            try{ const r=await login(email,pw); setToken(r.idToken) }catch(e:any){ alert(e.message||String(e)) }
          }}>Log in</button>
        </div>
        <div className="mt-4 flex gap-2">
          <input className="flex-1 border rounded-lg px-3 py-2" placeholder="confirm code" value={code} onChange={e=>setCode(e.target.value)}/>
          <button className="px-3 py-2 rounded-lg border" onClick={async()=>{
            try{ await confirm(email, code); alert('Confirmed! Log in now.') }catch(e:any){ alert(e.message||String(e)) }
          }}>Confirm</button>
        </div>
      </Card>
    </div>
  )
  if(!handle) return <ClaimUsername token={token} onClaim={()=>nav('/')}/>
  return null
}

function ClaimUsername({token,onClaim}:{token:string,onClaim:()=>void}){
  const [u,setU]=React.useState('')
  return (
    <div className="max-w-lg mx-auto">
      <Card>
        <h2 className="text-xl font-semibold mb-3">Pick your username</h2>
        <div className="flex gap-2">
          <span className="px-3 py-2 bg-gray-100 rounded-l-lg border border-gray-300 text-gray-600">@</span>
          <input className="flex-1 border border-gray-300 rounded-r-lg px-3 py-2" placeholder="yourname" value={u} onChange={e=>setU(e.target.value)}/>
        </div>
        <button className="mt-4 w-full bg-indigo-600 text-white rounded-lg py-2" onClick={async()=>{
          try{ await claimUsername(token,u); onClaim() }catch(e:any){ alert(e.message||String(e)) }
        }}>Claim</button>
      </Card>
    </div>
  )
}

/* ---------------------- Reactions + Comments UI ---------------------- */
function ReactionsRow({ token, postId }:{token:string; postId:string}) {
  const EMOJIS = ['👍','❤️','😂','🔥','👏']
  const [counts, setCounts] = React.useState<Record<string,number>>({})
  const [mine, setMine] = React.useState<string[]>([])
  const [loading, setLoading] = React.useState(false)

  const load = React.useCallback(async ()=>{
    if(!token) return
    try {
      const r = await getReactions(token, postId)
      setCounts(r.counts || {})
      setMine(r.my || [])
    } catch {}
  }, [token, postId])

  React.useEffect(()=>{ load() }, [load])

  const onToggle = async (emoji:string)=>{
    if (loading) return
    setLoading(true)
    try {
      await toggleReaction(token, postId, emoji)
      // optimistic flip
      const has = mine.includes(emoji)
      setMine(m => has ? m.filter(x=>x!==emoji) : [...m, emoji])
      setCounts(c => ({...c, [emoji]: Math.max(0, (c[emoji]||0) + (has?-1:1))}))
      await load()
    } finally { setLoading(false) }
  }

  return (
    <div className="flex gap-2">
      {EMOJIS.map(e => {
        const active = mine.includes(e)
        const cnt = counts[e] || 0
        return (
          <button
            key={e}
            onClick={()=>onToggle(e)}
            className={`px-2 py-1 rounded-full border text-sm leading-none ${active ? 'bg-indigo-50 border-indigo-200' : 'hover:bg-gray-50'}`}
            title={active ? `Remove ${e}` : `React ${e}`}
          >
            <span className="mr-1">{e}</span>
            {cnt > 0 && <span className="text-gray-600">{cnt}</span>}
          </button>
        )
      })}
    </div>
  )
}

function CommentsBlock({ token, postId }:{token:string; postId:string}) {
  const [open, setOpen] = React.useState(false)
  const [items, setItems] = React.useState<any[]>([])
  const [cursor, setCursor] = React.useState<string|undefined>()
  const [text, setText] = React.useState('')
  const [loading, setLoading] = React.useState(false)

  const load = async (c?:string)=>{
    if(!token || !open) return
    setLoading(true)
    try{
      const r = await listComments(token, postId, c)
      setItems(prev => c ? [...prev, ...(r.items||[])] : (r.items||[]))
      setCursor(r.nextCursor)
    } finally { setLoading(false) }
  }

  React.useEffect(()=>{ if(open) load(undefined) }, [open, token, postId])

  const submit = async ()=>{
    if(!text.trim()) return
    const t = text.trim()
    setText('')
    try{
      await addComment(token, postId, t)
      await load(undefined)
    }catch(e:any){ alert(e.message||String(e)) }
  }

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
            {items.map(c => (
              <div key={c.id} className="text-sm">
                <a href={`/u/${c.username}`} className="font-medium hover:underline">@{c.username}</a>
                <span className="text-gray-500 ml-2">{new Date(c.createdAt).toLocaleString()}</span>
                <div className="whitespace-pre-wrap">{c.text}</div>
              </div>
            ))}
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

/* ------------------------------ Feed ------------------------------ */
function Feed(){
  const [token,setToken]=React.useState<string>('')
  const [items,setItems]=React.useState<any[]>([])
  const [text,setText]=React.useState('')
  const [file,setFile]=React.useState<File|null>(null)

  React.useEffect(()=>{ const id = Object.keys(localStorage).find(k=>k.includes('idToken')); if(id){ setToken(localStorage.getItem(id)||'') } },[])
  React.useEffect(()=>{ (async()=>{ if(!token) return; const f=await getFeed(token); setItems(f.items) })() },[token])

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
            <button className="bg-indigo-600 text-white rounded-lg px-4 py-2" onClick={async()=>{
              try{
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
          {items.map((it:any)=>(
            <Card key={it.id}>
              <div className="flex items-center gap-2 mb-1">
                <a className="font-semibold hover:underline" href={`/u/${it.username}`}>@{it.username}</a>
                <div className="text-xs text-gray-500 ml-auto">{new Date(it.createdAt).toLocaleString()}</div>
              </div>

              <div className="whitespace-pre-wrap mb-3">{it.text}</div>

              {/* Reactions under the post */}
              <ReactionsRow token={token} postId={it.id} />

              {/* Comments toggle + list + composer */}
              <CommentsBlock token={token} postId={it.id} />
            </Card>
          ))}
          {items.length===0 && <div className="text-center text-gray-500">No posts from people you follow yet.</div>}
        </div>
      </div>

      <div className="space-y-4"><Admin/></div>
    </div>
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

/* ---------------------------- Profile ---------------------------- */
function Profile(){
  const params = useParams(); const handle = params.handle!
  const [token,setToken]=React.useState('')
  const [data,setData]=React.useState<any>(null)
  const [following,setFollowing]=React.useState(false)

  React.useEffect(()=>{ const id=Object.keys(localStorage).find(k=>k.includes('idToken')); if(id){ setToken(localStorage.getItem(id)||'') } },[])
  React.useEffect(()=>{ (async()=>{ if(!token) return; const r=await getUser(token, handle); setData(r); setFollowing(r.isFollowing || false) })() },[token, handle])

  if(!token) return <Gate/>
  if(!data) return <div>Loading…</div>

  return (
    <div className="space-y-4">
      <Card>
        <div className="flex items-center justify-between">
          <div>
            <div className="text-2xl font-semibold">@{data.handle}</div>
            <div className="text-sm text-gray-500 flex gap-4 mt-1">
              <a className="hover:underline" href={`/u/${handle}/followers`}><b>{data.followers ?? 0}</b> followers</a>
              <a className="hover:underline" href={`/u/${handle}/following`}><b>{data.following ?? 0}</b> following</a>
              <span><b>{data.posts?.length||0}</b> posts</span>
            </div>
          </div>
          <button
            className={"px-4 py-2 rounded-lg border " + (following?"bg-gray-100":"bg-indigo-600 text-white")}
            onClick={async()=>{
              try{
                if(following){
                  await unfollow(token, handle)
                  setFollowing(false)
                  setData((d:any)=>({ ...d, followers: Math.max(0, (d.followers||0)-1) }))
                } else {
                  await follow(token, handle)
                  setFollowing(true)
                  setData((d:any)=>({ ...d, followers: (d.followers||0)+1 }))
                }
              }catch(e:any){ alert(e.message||String(e)) }
            }}
          >{following? "Unfollow":"Follow"}</button>
        </div>
      </Card>

      <div className="space-y-3">
        {data.posts.map((p:any)=>(
          <Card key={p.id}>
            <div className="text-xs text-gray-500">{new Date(p.createdAt).toLocaleString()}</div>
            <div className="whitespace-pre-wrap mb-3">{p.text}</div>

            {/* reactions + comments on profile post */}
            <ReactionsRow token={token} postId={p.id} />
            <CommentsBlock token={token} postId={p.id} />
          </Card>
        ))}
      </div>
    </div>
  )
}

/* --------------------------- Followers list --------------------------- */
function FollowList({mode}:{mode:'followers'|'following'}){
  const params = useParams(); const handle = params.handle!
  const [token,setToken]=React.useState('')
  const [items,setItems]=React.useState<any[]>([])
  const [cursor,setCursor]=React.useState<string|undefined>()
  const [loading,setLoading]=React.useState(false)

  React.useEffect(()=>{ const id=Object.keys(localStorage).find(k=>k.includes('idToken')); if(id){ setToken(localStorage.getItem(id)||'') } },[])

  const load = async (c?:string) => {
    if(!token) return; setLoading(true)
    try {
      const r = mode==='followers' ? await listFollowers(token, handle, c) : await listFollowing(token, handle, c)
      setItems(prev => c ? [...prev, ...(r.items||[])] : (r.items||[]))
      setCursor(r.nextCursor)
    } finally { setLoading(false) }
  }

  React.useEffect(()=>{ load(undefined) },[token, handle, mode])

  return (
    <div className="max-w-xl mx-auto space-y-3">
      {items.map((u:any) => (
        <Card key={u.handle}>
          <a className="font-semibold hover:underline" href={`/u/${u.handle}`}>
            @{u.handle}
          </a>
        </Card>
      ))}
      {items.length === 0 && !loading && (
        <div className="text-center text-gray-500">
          No {mode} yet.
        </div>
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
  )
}

/* ------------------------------ Search ------------------------------ */
function Search(){
  const [token,setToken]=React.useState('')
  const [q,setQ]=React.useState('')
  const [items,setItems]=React.useState<any[]>([])
  const [cursor,setCursor]=React.useState<string|undefined>()
  const [loading,setLoading]=React.useState(false)

  React.useEffect(()=>{ const id=Object.keys(localStorage).find(k=>k.includes('idToken')); if(id){ setToken(localStorage.getItem(id)||'') } },[])

  const run = async (cur?:string) => {
    if(!token) return; setLoading(true)
    try{
      const r = await searchUsers(token, q, cur)
      if(cur){ setItems(prev=>[...prev, ...(r.items||[])]) } else { setItems(r.items||[]) }
      setCursor(r.nextCursor)
    } finally { setLoading(false) }
  }

  return (
    <div className="max-w-xl mx-auto space-y-3">
      <Card>
        <div className="flex gap-2">
          <input value={q} onChange={e=>setQ(e.target.value)} className="flex-1 border rounded-lg px-3 py-2" placeholder="Search @handles"/>
          <button className="px-3 py-2 rounded-lg border" onClick={()=>run(undefined)}>Search</button>
        </div>
      </Card>
      {items.map((u:any)=>(<UserRow key={u.handle} initial={u} token={token}/>))}
      {cursor && <div className="text-center"><button className="px-3 py-2 rounded-lg border" disabled={loading} onClick={()=>run(cursor)}>{loading?'Loading…':'Load more'}</button></div>}
      {!loading && items.length===0 && <div className="text-center text-gray-500">No results</div>}
    </div>
  )
}

function UserRow({initial, token}:{initial:any, token:string}){
  const [user,setUser]=React.useState<any>(initial)
  return (
    <Card>
      <div className="flex items-center gap-3">
        <a className="font-semibold hover:underline" href={`/u/${user.handle}`}>@{user.handle}</a>
        <div className="ml-auto">
          <button
            className={"px-3 py-1.5 rounded-lg border text-sm " + (user.isFollowing?"bg-gray-100":"bg-indigo-600 text-white")}
            onClick={async()=>{
              try{
                if(user.isFollowing){ await unfollow(token, user.handle); setUser((u:any)=>({...u, isFollowing:false})) }
                else { await follow(token, user.handle); setUser((u:any)=>({...u, isFollowing:true})) }
              }catch(e:any){ alert(e.message||String(e)) }
            }}
          >{user.isFollowing? 'Unfollow':'Follow'}</button>
        </div>
      </div>
    </Card>
  )
}

/* ------------------------------ Router ------------------------------ */
const router = createBrowserRouter([
  { path: "/", element: <Layout/>, children: [
    { index: true, element: <Feed/> },
    { path: "start", element: <Gate/> },
    { path: "u/:handle", element: <Profile/> },
    { path: "u/:handle/followers", element: <FollowList mode="followers"/> },
    { path: "u/:handle/following", element: <FollowList mode="following"/> },
    { path: "search", element: <Search/> }
  ]}
])

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <RouterProvider router={router}/>
  </React.StrictMode>
)
