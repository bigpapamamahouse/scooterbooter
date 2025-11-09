// === ScooterBooter Lambda (merged with Notifications / Mentions / Follow-Requests) ===
// ---------- CommonJS + AWS SDK v3 ----------
const crypto = require('crypto');
const { randomUUID } = require('crypto');

const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  ScanCommand,
  DeleteCommand,
  UpdateCommand,
  BatchGetCommand,
} = require('@aws-sdk/lib-dynamodb');

const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

// ---------- Env ----------
const POSTS_TABLE     = process.env.POSTS_TABLE;
const USERS_TABLE     = process.env.USERS_TABLE;
const INVITES_TABLE   = process.env.INVITES_TABLE;
const FOLLOWS_TABLE   = process.env.FOLLOWS_TABLE; // optional but recommended
const MEDIA_BUCKET    = process.env.MEDIA_BUCKET;
const COMMENTS_TABLE  = process.env.COMMENTS_TABLE;   // pk: POST#<postId>, sk: C#<ts>#<uuid>
const REACTIONS_TABLE = process.env.REACTIONS_TABLE;  // pk: POST#<postId>, sk: COUNT#<emoji> (count item)
//                                                    // pk: POST#<postId>, sk: USER#<userId>  (who reacted + emoji)
const NOTIFICATIONS_TABLE = process.env.NOTIFICATIONS_TABLE; // NEW: pk USER#<targetId>, sk N#<ts>#<uuid>

// ---------- Allowed origins for CORS ----------
const ALLOWED_ORIGINS = new Set([
  'https://app.scooterbooter.com',
  'http://localhost:5173',
]);

const ADMIN_EMAILS = (process.env.ADMIN_EMAILS || '')
  .split(',')
  .map(s => s.trim().toLowerCase())
  .filter(Boolean);

// ---------- Clients ----------
const ddbClient = new DynamoDBClient({});
const ddb = DynamoDBDocumentClient.from(ddbClient, {
  marshallOptions: { removeUndefinedValues: true },
});
const s3 = new S3Client({});

// ---------- CORS + response helpers ----------

function corsFor(event) {
  const o = event?.headers?.origin || event?.headers?.Origin || '';
  const allow = ALLOWED_ORIGINS.has(o) ? o : 'https://app.scooterbooter.com';
  // Allow both canonical and lowercase header names + our custom header.
  const allowHeaders = [
    'Content-Type',
    'content-type',
    'Authorization',
    'authorization',
    'Accept',
    'accept',
    'X-Requested-With',
    'x-requested-with',
    'Origin',
    'origin',
    'X-Ignore-Auth-Redirect',
    'x-ignore-auth-redirect',
  ].join(', ');
  return {
    'Access-Control-Allow-Origin': allow,
    'Access-Control-Allow-Credentials': 'true',
    'Access-Control-Allow-Headers': allowHeaders,
    'Access-Control-Allow-Methods': 'GET,POST,PATCH,DELETE,OPTIONS',
    'Access-Control-Max-Age': '86400',
    'Vary': 'Origin',
  };
}

// we keep the original ok/bad call sites unchanged by capturing the current event
let __event = null;
const ok = (body, status = 200, extraHeaders = {}) => ({
  statusCode: status,
  headers: {
    ...(corsFor(__event) || {}),
    'Content-Type': 'application/json; charset=utf-8', // ← ensure JSON content type
    ...extraHeaders
  },
  body: typeof body === 'string' ? body : JSON.stringify(body),
});
const bad = (message = 'Bad Request', status = 400) => ok({ message }, status);

// ---------- Misc helpers ----------
const claimsFrom = (event) => event?.requestContext?.authorizer?.jwt?.claims || {};

async function getHandleForUserId(userId) {
  const r = await ddb.send(new GetCommand({
    TableName: USERS_TABLE,
    Key: { pk: `USER#${userId}` },
    ConsistentRead: true,
  }));
  return r.Item?.handle || null;
}

// Resolve a userId from a handle
async function userIdFromHandle(handle) {
  if (!handle) return null;
  const h = String(handle).trim().toLowerCase();
  // Primary: HANDLE mapping row -> USER id
  try {
    const r = await ddb.send(new GetCommand({
      TableName: USERS_TABLE,
      Key: { pk: `HANDLE#${h}` },
      ProjectionExpression: 'userId',
      ConsistentRead: true,
    }));
    if (r.Item && r.Item.userId) return String(r.Item.userId);
  } catch (e) {}

  // Optional fallback via GSI if present
  try {
    const qr = await ddb.send(new QueryCommand({
      TableName: USERS_TABLE,
      IndexName: 'byHandle',
      KeyConditionExpression: '#t = :t AND #h = :h',
      ExpressionAttributeNames: { '#t': 'type', '#h': 'handle' },
      ExpressionAttributeValues: { ':t': 'HANDLE', ':h': h },
      Limit: 1,
      ConsistentRead: true,
    }));
    const it = (qr.Items || [])[0];
    if (it && it.userId) return String(it.userId);
  } catch (e) {}

  return null;
}

// NEW: notifications helper
async function createNotification(targetUserId, type, fromUserId, postId = null, message = '') {
  if (!NOTIFICATIONS_TABLE || !targetUserId || targetUserId === fromUserId) return;
  const now = Date.now();
  const id = randomUUID();
  await ddb.send(new PutCommand({
    TableName: NOTIFICATIONS_TABLE,
    Item: {
      pk: `USER#${targetUserId}`,
      sk: `N#${now}#${id}`,
      id,
      type,
      fromUserId,
      postId,
      message,
      read: false,
      createdAt: now,
    },
  }));
}


// Helper: delete notifications that match a predicate (type/fromUserId/postId)
async function deleteNotifications(targetUserId, type, fromUserId = null, postId = null, sk = null) {
  if (!NOTIFICATIONS_TABLE || !targetUserId) return;
  if (sk) {
    try {
      await ddb.send(new DeleteCommand({
        TableName: NOTIFICATIONS_TABLE,
        Key: { pk: `USER#${targetUserId}`, sk }
      }));
    } catch (e) {
      console.error('deleteNotifications (by sk) failed for', targetUserId, sk, e);
    }
    return;
  }
  try {
    const q = await ddb.send(new QueryCommand({
      TableName: NOTIFICATIONS_TABLE,
      KeyConditionExpression: 'pk = :p',
      ExpressionAttributeValues: { ':p': `USER#${targetUserId}` },
      ScanIndexForward: false,
      Limit: 200,
      ConsistentRead: true,
    }));
    const items = (q.Items || []).filter(it => {
      if (type && it.type !== type) return false;
      if (fromUserId && String(it.fromUserId) !== String(fromUserId)) return false;
      if (postId && String(it.postId || '') !== String(postId)) return false;
      return true;
    });
    for (const it of items) {
      try {
        await ddb.send(new DeleteCommand({
          TableName: NOTIFICATIONS_TABLE,
          Key: { pk: `USER#${targetUserId}`, sk: it.sk },
        }));
      } catch (e) {
        console.error('deleteNotifications failed for', targetUserId, e);
      }
    }
  } catch (e) {
    console.error('deleteNotifications query failed', e);
  }
}


// Helper: check if a notification already exists (best-effort; scans recent)
async function hasNotification(targetUserId, type, fromUserId = null, postId = null) {
  if (!NOTIFICATIONS_TABLE || !targetUserId) return false;
  try {
    const q = await ddb.send(new QueryCommand({
      TableName: NOTIFICATIONS_TABLE,
      KeyConditionExpression: 'pk = :p',
      ExpressionAttributeValues: { ':p': `USER#${targetUserId}` },
      ScanIndexForward: false,
      Limit: 200,
    }));
    const items = q.Items || [];
    for (const it of items) {
      if (type && it.type !== type) continue;
      if (fromUserId && String(it.fromUserId) !== String(fromUserId)) continue;
      if (postId && String(it.postId || '') !== String(postId)) continue;
      return true;
    }
  } catch (e) { console.error('hasNotification failed', e); }
  return false;
}
function normalizePath(event) {
  // Handles both API Gateway v1 (REST) and v2 (HTTP) payloads
  const method = (event?.httpMethod || event?.requestContext?.http?.method || '').toUpperCase();
  const rawPath = event?.path || event?.rawPath || event?.requestContext?.http?.path || '';
  const stage   = event?.requestContext?.stage || '';

  let path = rawPath || '/';
  // Strip stage if it's part of the path
  if (stage && path.startsWith(`/${stage}`)) {
    path = path.slice(stage.length + 1);
  }

  // Clean up the path
  path = ('/' + path.split('?')[0]).replace(/\/{2,}/g, '/').replace(/\/+$/g, '') || '/';

  return { method, rawPath, stage, path, route: `${method} ${path}` };
}

function matchUserRoutes(path) {
  const m1 = path.match(/^\/u\/([^\/]+)$/);
  if (m1) return { kind: 'user', handle: decodeURIComponent(m1[1]) };

  const m2 = path.match(/^\/u\/([^\/]+)\/followers$/);
  if (m2) return { kind: 'followers', handle: decodeURIComponent(m2[1]) };

  const m3 = path.match(/^\/u\/([^\/]+)\/following$/);
  if (m3) return { kind: 'following', handle: decodeURIComponent(m3[1]) };

  const m4 = path.match(/^\/u\/([^\/]+)\/posts$/);
  if (m4) return { kind: 'posts', handle: decodeURIComponent(m4[1]) };

  return null;
}

async function listPostsByUserId(targetId, limit = 50) {
  const r = await ddb.send(new QueryCommand({
    TableName: POSTS_TABLE,
    KeyConditionExpression: 'pk = :p',
    ExpressionAttributeValues: { ':p': `USER#${targetId}` },
    ScanIndexForward: false,
    Limit: limit,
    ConsistentRead: true,
  }));
  return (r.Items || []).map(i => ({
    id: i.id,
    userId: i.userId,
    username: i.username || 'unknown',
    text: i.text || '',
    imageKey: i.imageKey || null,
    avatarKey: i.avatarKey || null,
    createdAt: i.createdAt,
  }));
}

// ---- ConsistentRead helpers for follow state ----
async function countFollowers(targetUserId) {
  if (!FOLLOWS_TABLE) return 0;
  const r = await ddb.send(new ScanCommand({
    TableName: FOLLOWS_TABLE,
    FilterExpression: 'sk = :t',
    ExpressionAttributeValues: { ':t': targetUserId },
    ProjectionExpression: 'pk',
    ConsistentRead: true,
  }));
  return (r.Items || []).length;
}

async function countFollowing(userId) {
  if (!FOLLOWS_TABLE) return 0;
  const r = await ddb.send(new QueryCommand({
    TableName: FOLLOWS_TABLE,
    KeyConditionExpression: 'pk = :me',
    ExpressionAttributeValues: { ':me': userId },
    ProjectionExpression: 'sk',
    ConsistentRead: true,
  }));
  return (r.Items || []).length;
}

async function isFollowing(userId, targetUserId) {
  if (!FOLLOWS_TABLE) return false;
  const r = await ddb.send(new GetCommand({
    TableName: FOLLOWS_TABLE,
    Key: { pk: userId, sk: targetUserId },
    ConsistentRead: true,
  }));
  return !!r.Item;
}

// Get handle + avatarKey for a list of userIds.
async function fetchUserSummaries(userIds) {
  const unique = Array.from(new Set(userIds)).filter(Boolean);
  if (unique.length === 0) return [];

  const chunk = (arr, n) =>
    arr.reduce((acc, _, i) => (i % n ? acc : [...acc, arr.slice(i, i + n)]), []);
  const chunks = chunk(unique, 100);

  const out = [];
  for (const ids of chunks) {
    const resp = await ddb.send(new BatchGetCommand({
      RequestItems: {
        [USERS_TABLE]: {
          Keys: ids.map(id => ({ pk: `USER#${id}` })),
          ProjectionExpression: 'pk, handle, userId, avatarKey, fullName',
        }
      }
    }));
    const rows = (resp.Responses?.[USERS_TABLE] || []);
    for (const it of rows) {
      let id = (it.userId && String(it.userId)) || '';
      if (!id && typeof it.pk === 'string' && it.pk.startsWith('USER#')) {
        id = it.pk.slice('USER#'.length);
      }
      if (!id) continue;
      out.push({
        userId: id,
        handle: it.handle || null,
        avatarKey: it.avatarKey || null,
        fullName: it.fullName || null,
      });
    }
  }

  // Fallback per-item
  if (out.length === 0 && unique.length > 0) {
    for (const id of unique) {
      const r = await ddb.send(new GetCommand({
        TableName: USERS_TABLE,
        Key: { pk: `USER#${id}` },
        ProjectionExpression: 'pk, handle, userId, avatarKey, fullName',
        ConsistentRead: true,
      }));
      const it = r.Item;
      if (!it) continue;
      let uid = (it.userId && String(it.userId)) || '';
      if (!uid && typeof it.pk === 'string' && it.pk.startsWith('USER#')) {
        uid = it.pk.slice('USER#'.length);
      }
      if (!uid) continue;
      out.push({
        userId: uid,
        handle: it.handle || null,
        avatarKey: it.avatarKey || null,
        fullName: it.fullName || null,
      });
    }
  }
  return out;
}

// ---------- Handler ----------
module.exports.handler = async (event) => {
  __event = event; // capture for CORS headers everywhere

  // Always return 200 for preflight with CORS headers
  if ((event?.requestContext?.http?.method || event?.httpMethod) === 'OPTIONS') {
    return ok({});
  }

  const { method, rawPath, stage, path, route } = normalizePath(event);
  console.log('ROUTE', { method, rawPath, stage, normalized: path });

  const claims = claimsFrom(event);
  const userId = claims.sub;
  const email  = (claims.email || '').toLowerCase();
  const usernameFallback = claims['cognito:username'] || email || userId || 'user';

  try {
    // ===== Notifications API =====
    if (route === 'GET /notifications') {
      if (!userId) return bad('Unauthorized', 401);
      if (!NOTIFICATIONS_TABLE) return bad('Notifications not enabled', 501);

      const r = await ddb.send(new QueryCommand({
        TableName: NOTIFICATIONS_TABLE,
        KeyConditionExpression: 'pk = :p',
        ExpressionAttributeValues: { ':p': `USER#${userId}` },
        ScanIndexForward: false,
        Limit: 50,
      }));

      // Optional markRead=1
      const markRead = event?.queryStringParameters?.markRead === '1';
      if (markRead) {
        for (const it of (r.Items || [])) {
          await ddb.send(new UpdateCommand({
            TableName: NOTIFICATIONS_TABLE,
            Key: { pk: `USER#${userId}`, sk: it.sk },
            UpdateExpression: 'SET #r = :t',
            ExpressionAttributeNames: { '#r': 'read' },
            ExpressionAttributeValues: { ':t': true },
          }));
        }
      }
      
      const items = r.Items || [];
      // Gather sender ids
      const fromIds = items.map(it => it.fromUserId).filter(Boolean);
      // Fetch handle+avatar for senders
      let summaries = [];
      try { summaries = await fetchUserSummaries(fromIds); } catch (_) {}
      const byId = Object.fromEntries(summaries.map(s => [String(s.userId), s]));

      const enriched = items.map(it => {
        const prof = byId[String(it.fromUserId)] || {};
        const handle = prof.handle || null;
        const userUrl = handle ? `/u/${handle}` : undefined;
        const postUrl = it.postId ? `/p/${encodeURIComponent(String(it.postId))}` : undefined;
        return {
          ...it,
          fromHandle: handle,
          avatarKey: prof.avatarKey || null,
          userUrl,
          postUrl,
        };
      });
      return ok({ items: enriched });
    
    }
    // Request to follow -> notify target user
    if (route === 'POST /follow-request') {
      if (!userId) return bad('Unauthorized', 401);
      if (!NOTIFICATIONS_TABLE) return bad('Notifications not enabled', 501);
      const body = JSON.parse(event.body || '{}');
      const handle = String(body.handle || '').trim().toLowerCase();
      const targetId = await userIdFromHandle(handle);
      if (!targetId || targetId === userId) return bad('Invalid target', 400);
      // avoid duplicate follow request notifications
      if (await hasNotification(targetId, 'follow_request', userId, null)) return ok({ requested: true });
      await createNotification(targetId, 'follow_request', userId, null, 'wants to follow you');
      return ok({ requested: true });
    }

    // Cancel a previously-sent follow request (remove target's pending notification)
    if (route === 'POST /follow-cancel') {
      if (!userId) return bad('Unauthorized', 401);
      if (!NOTIFICATIONS_TABLE) return bad('Notifications not enabled', 501);
      const body = JSON.parse(event.body || '{}');
      const handle = String(body.handle || '').trim().toLowerCase();
      const targetId = await userIdFromHandle(handle);
      if (!targetId || targetId === userId) return bad('Invalid target', 400);
      try { await deleteNotifications(targetId, 'follow_request', userId, null); } 
      catch (e) { console.error('follow-cancel deleteNotifications failed', e); }
      return ok({ cancelled: true });
    }

    // Accept request -> create follow row + notify requester
    if (route === 'POST /follow-accept') {
      if (!userId) return bad('Unauthorized', 401);
      if (!FOLLOWS_TABLE) return bad('Follows not enabled', 501);
      const body = JSON.parse(event.body || '{}');
      const requesterId = String(body.fromUserId || '');
      if (!requesterId) return bad('Missing requesterId', 400);
      await ddb.send(new PutCommand({
        TableName: FOLLOWS_TABLE,
        Item: { pk: requesterId, sk: userId },
      }));
      // remove pending request notification from my inbox
      try { await deleteNotifications(userId, 'follow_request', requesterId, null); } catch (e) { console.error('follow-accept cleanup failed', e); }
      // notify requester
      try { await createNotification(requesterId, 'follow_accept', userId, null, 'accepted your follow request'); } catch (e) {}
      return ok({ accepted: true });
    }

    // Decline a follow request
    if (route === 'POST /follow-decline') {
      if (!userId) return bad('Unauthorized', 401);
      if (!NOTIFICATIONS_TABLE) return bad('Notifications not enabled', 501);
      const body = JSON.parse(event.body || '{}');
      const requesterId = String(body.fromUserId || '');
      if (!requesterId) return bad('Missing requesterId', 400);
      // Remove the pending request notification from my inbox
      await deleteNotifications(userId, 'follow_request', requesterId, null);
      // Optional: notify requester of decline
      try { await createNotification(requesterId, 'follow_declined', userId, null, 'declined your follow request'); } catch (e) {}
      return ok({ declined: true });
    }


    // (A) Return current profile (add avatarKey + fullName so UI can show it)
    if (route === 'GET /me') {
      if (!userId) return ok({ message: 'Unauthorized' }, 401);
      const r = await ddb.send(new GetCommand({ TableName: USERS_TABLE, Key: { pk: `USER#${userId}` } }));
      return ok({
        userId,
        handle: r.Item?.handle ?? null,
        email,
        avatarKey: r.Item?.avatarKey ?? null,
        fullName: r.Item?.fullName ?? null,
      });
    }

    // (A2) Update fields on me (currently: fullName)
    if (route === 'PATCH /me') {
      if (!userId) return ok({ message: 'Unauthorized' }, 401);
      const body = JSON.parse(event.body || '{}');
      const raw = (body.fullName ?? '').toString().trim();
      const fullName = raw ? raw.slice(0, 80) : null;
      await ddb.send(new UpdateCommand({
        TableName: USERS_TABLE,
        Key: { pk: `USER#${userId}` },
        UpdateExpression: 'SET #uid = :u, #fn = :n',
        ExpressionAttributeNames: { '#uid': 'userId', '#fn': 'fullName' },
        ExpressionAttributeValues: { ':u': userId, ':n': fullName },
      }));
      return ok({ ok: true, fullName });
    }

    // (A3) Allow POST /me as alias for updating fullName
    if (route === 'POST /me') {
      if (!userId) return ok({ message: 'Unauthorized' }, 401);
      const body = JSON.parse(event.body || '{}');
      const raw = (body.fullName ?? '').toString().trim();
      const fullName = raw ? raw.slice(0, 80) : null;
      await ddb.send(new UpdateCommand({
        TableName: USERS_TABLE,
        Key: { pk: `USER#${userId}` },
        UpdateExpression: 'SET #uid = :u, #fn = :n',
        ExpressionAttributeNames: { '#uid': 'userId', '#fn': 'fullName' },
        ExpressionAttributeValues: { ':u': userId, ':n': fullName },
      }));
      return ok({ ok: true, fullName });
    }

    // (B) Set avatar for current user (client uploads to S3 -> sends { key })
    if (route === 'POST /me/avatar') {
      if (!userId) return ok({ message: 'Unauthorized' }, 401);
      const { key } = JSON.parse(event.body || '{}');
      if (!key) return ok({ message: 'Missing key' }, 400);

      await ddb.send(new PutCommand({
        TableName: USERS_TABLE,
        Item: { pk: `USER#${userId}`, avatarKey: key, userId },
      }));

      const u = await ddb.send(new GetCommand({ TableName: USERS_TABLE, Key: { pk: `USER#${userId}` } }));
      if (u.Item?.handle) {
        await ddb.send(new PutCommand({
          TableName: USERS_TABLE,
          Item: { pk: `HANDLE#${u.Item.handle}`, userId, avatarKey: key }
        }));
      }
      return ok({ success: true, avatarKey: key });
    }

    // GET /posts/{id}/comments
    if (method === 'GET' && path.startsWith('/comments/')) {
      if (!COMMENTS_TABLE) return ok({ message: 'Comments not enabled' }, 501);
      const postId = path.split('/')[2];
      const r = await ddb.send(new QueryCommand({
        TableName: COMMENTS_TABLE,
        KeyConditionExpression: 'pk = :p',
        ExpressionAttributeValues: { ':p': `POST#${postId}` },
        ScanIndexForward: true,
        Limit: 50
      }));
      const items = (r.Items || []).map(it => ({ id: it.id, userId: it.userId,
        userHandle: it.userHandle || 'unknown',
        text: it.text || '',
        createdAt: it.createdAt || 0,
      }));
      return ok({ items });
    }

    // POST /posts/{id}/comments  body: { text }
    if (method === 'POST' && path.startsWith('/comments/')) {
      if (!COMMENTS_TABLE) return ok({ message: 'Comments not enabled' }, 501);
      if (!userId) return ok({ message: 'Unauthorized' }, 401);
      const postId = path.split('/')[2];
      const body = JSON.parse(event.body || '{}');
      const text = String(body.text || '').trim().slice(0, 500);
      if (!text) return ok({ message: 'Text required' }, 400);
      const handle = await getHandleForUserId(userId) || 'unknown';
      const now = Date.now();
      const id = randomUUID();
      const item = {
        pk: `POST#${postId}`,
        sk: `C#${now}#${id}`,
        id,
        postId,
        userId,
        userHandle: handle,
        text,
        createdAt: now
      };
      await ddb.send(new PutCommand({ TableName: COMMENTS_TABLE, Item: item }));

      // NEW: notify post owner + mentions
      try {
        // find post owner by GSI byId
        const qr = await ddb.send(new QueryCommand({
          TableName: POSTS_TABLE,
          IndexName: 'byId',
          KeyConditionExpression: 'id = :id',
          ExpressionAttributeValues: { ':id': postId },
          Limit: 1,
        }));
        const post = (qr.Items || [])[0];
        if (post && post.userId && post.userId !== userId) {
          await createNotification(post.userId, 'comment', userId, postId, 'commented on your post');
        }
      } catch (e) { console.error('notify comment post owner failed', e); }

      try {
        const mentionRegex = /@([a-z0-9_]+)/gi;
        const mentions = [...text.matchAll(mentionRegex)].map(m => m[1].toLowerCase());
        for (const h of mentions) {
          const mid = await userIdFromHandle(h);
          if (mid && mid !== userId) {
            await createNotification(mid, 'mention', userId, postId, 'mentioned you in a comment');
          }
        }
      } catch (e) { console.error('notify mentions (comment) failed', e); }

      return ok({ id, userHandle: handle, text, createdAt: now });
    }

    // PATCH /comments/{postId}  body: { id, text }
    if (method === 'PATCH' && path.startsWith('/comments/')) {
      if (!COMMENTS_TABLE) return ok({ message: 'Comments not enabled' }, 501);
      if (!userId) return ok({ message: 'Unauthorized' }, 401);
      const postId = path.split('/')[2];
      const body = JSON.parse(event.body || '{}');
      const id = String(body.id || '').trim();
      const text = String(body.text || '').trim().slice(0, 500);
      if (!id || !text) return ok({ message: 'id and text required' }, 400);

      const qr = await ddb.send(new QueryCommand({
        TableName: COMMENTS_TABLE,
        KeyConditionExpression: 'pk = :p AND begins_with(sk, :c)',
        ExpressionAttributeValues: { ':p': `POST#${postId}`, ':c': 'C#' },
        ConsistentRead: true,
        Limit: 200
      }));
      const item = (qr.Items || []).find(i => i.id === id);
      if (!item) return ok({ message: 'Comment not found' }, 404);
      if (item.userId !== userId) return ok({ message: 'Forbidden' }, 403);

      await ddb.send(new UpdateCommand({
        TableName: COMMENTS_TABLE,
        Key: { pk: item.pk, sk: item.sk },
        UpdateExpression: 'SET #t = :t',
        ExpressionAttributeNames: { '#t': 'text' },
        ExpressionAttributeValues: { ':t': text },
        ConditionExpression: 'attribute_exists(pk) AND attribute_exists(sk)'
      }));

      return ok({ success: true });
    }

    // DELETE /comments/{postId}  body: { id }
    if (method === 'DELETE' && path.startsWith('/comments/')) {
      if (!COMMENTS_TABLE) return ok({ message: 'Comments not enabled' }, 501);
      if (!userId) return ok({ message: 'Unauthorized' }, 401);
      const postId = path.split('/')[2];
      const body = JSON.parse(event.body || '{}');
      const id = String(body.id || '').trim();
      if (!id) return ok({ message: 'id required' }, 400);

      const qr = await ddb.send(new QueryCommand({
        TableName: COMMENTS_TABLE,
        KeyConditionExpression: 'pk = :p AND begins_with(sk, :c)',
        ExpressionAttributeValues: { ':p': `POST#${postId}`, ':c': 'C#' },
        ConsistentRead: true,
        Limit: 200
      }));
      const item = (qr.Items || []).find(i => i.id === id);
      if (!item) return ok({ message: 'Comment not found' }, 404);
      if (item.userId !== userId) return ok({ message: 'Forbidden' }, 403);

      await ddb.send(new DeleteCommand({
        TableName: COMMENTS_TABLE,
        Key: { pk: item.pk, sk: item.sk },
        ConditionExpression: 'attribute_exists(pk) AND attribute_exists(sk)'
      }));

      
      // Cascade-delete notifications created by this comment
      try {
        // (a) Remove post owner's 'comment' notification from this commenter
        try {
          const qrPost = await ddb.send(new QueryCommand({
            TableName: POSTS_TABLE,
            IndexName: 'byId',
            KeyConditionExpression: 'id = :id',
            ExpressionAttributeValues: { ':id': postId },
            Limit: 1,
          }));
          const post = (qrPost.Items || [])[0];
          if (post && post.userId && post.userId !== userId) {
            await deleteNotifications(post.userId, 'comment', userId, postId);
          }
        } catch (e) { console.error('cleanup comment notif (owner) failed', e); }

        // (b) Remove mention notifications that originated from this comment
        try {
          const text = String(item.text || '');
          const mentionRegex = /@([a-z0-9_]+)/gi;
          const mentions = [...text.matchAll(mentionRegex)].map(m => m[1].toLowerCase());
          for (const h of mentions) {
            const mid = await userIdFromHandle(h);
            if (mid && mid !== userId) {
              await deleteNotifications(mid, 'mention', userId, postId);
            }
          }
        } catch (e) { console.error('cleanup comment notif (mentions) failed', e); }
      } catch (e) { console.error('cleanup notifications on comment delete failed', e); }
    
      return ok({ success: true });
    }

    // GET /posts/{id}/reactions  -> counts + mine (+who optional)
    if (method === 'GET' && path.startsWith('/reactions/')) {
      if (!REACTIONS_TABLE) return ok({ message: 'Reactions not enabled' }, 501);
      const postId = path.split('/')[2];
      const qr = await ddb.send(new QueryCommand({
        TableName: REACTIONS_TABLE,
        KeyConditionExpression: 'pk = :p',
        ExpressionAttributeValues: { ':p': `POST#${postId}` },
        ProjectionExpression: 'sk, #c, emoji',
        ExpressionAttributeNames: { '#c': 'count' },
        ConsistentRead: true,
      }));
      const counts = {};
      let mine = null;
      for (const it of (qr.Items || [])) {
        if (typeof it.sk === 'string' && it.sk.startsWith('COUNT#')) {
          const emoji = it.sk.slice('COUNT#'.length);
          counts[emoji] = Number(it.count || 0);
        } else if (userId && it.sk === `USER#${userId}`) {
          mine = it.emoji || null;
        }
      }
      if (userId && mine === null) {
        const ur = await ddb.send(new GetCommand({
          TableName: REACTIONS_TABLE,
          Key: { pk: `POST#${postId}`, sk: `USER#${userId}` },
          ProjectionExpression: 'emoji',
          ConsistentRead: true
        }));
        mine = ur.Item?.emoji || null;
      }

      const wantWho = (event?.queryStringParameters?.who === '1');
      let who = undefined;
      if (wantWho) {
        const uqr = await ddb.send(new QueryCommand({
          TableName: REACTIONS_TABLE,
          KeyConditionExpression: 'pk = :p AND begins_with(sk, :u)',
          ExpressionAttributeValues: { ':p': `POST#${postId}`, ':u': 'USER#' },
          ProjectionExpression: 'sk, emoji',
          ConsistentRead: true,
        }));
        const byEmoji = {};
        const uids = [];
        for (const row of (uqr.Items || [])) {
          const uid = String(row.sk).slice('USER#'.length);
          const e = row.emoji || '';
          if (!e) continue;
          (byEmoji[e] ||= []).push(uid);
          uids.push(uid);
        }
        const profiles = await fetchUserSummaries(uids);
        const map = Object.fromEntries((profiles || []).map(p => [p.userId, p]));
        who = {};
        for (const [e, ids] of Object.entries(byEmoji)) {
          who[e] = ids.map(uid => ({
            userId: uid,
            handle: map[uid]?.handle || null,
            avatarKey: map[uid]?.avatarKey || null,
          }));
        }
      }
      return ok({ counts, my: mine ? [mine] : [], ...(wantWho ? { who } : {}) });
    }

    // POST /posts/{id}/reactions  body: { emoji, action:'toggle' }
    if (method === 'POST' && path.startsWith('/reactions/')) {
      if (!REACTIONS_TABLE) return ok({ message: 'Reactions not enabled' }, 501);
      if (!userId) return ok({ message: 'Unauthorized' }, 401);
      const postId = path.split('/')[2];
      const body = JSON.parse(event.body || '{}');
      const raw = String(body.emoji || '').trim();
      const emoji = raw.slice(0, 8);
      if (!emoji) return ok({ message: 'Invalid emoji' }, 400);

      // Clean up any prior 'reaction' notifications from this user for this post (covers un-react & switches)
      try {
        const qr0 = await ddb.send(new QueryCommand({
          TableName: POSTS_TABLE,
          IndexName: 'byId',
          KeyConditionExpression: 'id = :id',
          ExpressionAttributeValues: { ':id': postId },
          Limit: 1,
        }));
        const post0 = (qr0.Items || [])[0];
        if (post0 && post0.userId && post0.userId !== userId) {
          await deleteNotifications(post0.userId, 'reaction', userId, postId);
        }
      } catch (e) { console.error('cleanup prior reaction notif failed', e); }

      const current = await ddb.send(new GetCommand({
        TableName: REACTIONS_TABLE,
        Key: { pk: `POST#${postId}`, sk: `USER#${userId}` },
        ProjectionExpression: 'emoji',
        ConsistentRead: true
      }));
      const prev = current.Item?.emoji || null;

      if (prev && prev === emoji) {
        await ddb.send(new UpdateCommand({
          TableName: REACTIONS_TABLE,
          Key: { pk: `POST#${postId}`, sk: `COUNT#${emoji}` },
          UpdateExpression: 'ADD #c :neg',
          ExpressionAttributeNames: { '#c': 'count' },
          ExpressionAttributeValues: { ':neg': -1 },
        }));
        await ddb.send(new DeleteCommand({
          TableName: REACTIONS_TABLE,
          Key: { pk: `POST#${postId}`, sk: `USER#${userId}` },
        }));
      } else {
        await ddb.send(new UpdateCommand({
          TableName: REACTIONS_TABLE,
          Key: { pk: `POST#${postId}`, sk: `COUNT#${emoji}` },
          UpdateExpression: 'ADD #c :one',
          ExpressionAttributeNames: { '#c': 'count' },
          ExpressionAttributeValues: { ':one': 1 },
        }));
        if (prev) {
          await ddb.send(new UpdateCommand({
            TableName: REACTIONS_TABLE,
            Key: { pk: `POST#${postId}`, sk: `COUNT#${prev}` },
            UpdateExpression: 'ADD #c :neg',
            ExpressionAttributeNames: { '#c': 'count' },
            ExpressionAttributeValues: { ':neg': -1 },
          }));
        }
        await ddb.send(new PutCommand({
          TableName: REACTIONS_TABLE,
          Item: { pk: `POST#${postId}`, sk: `USER#${userId}`, emoji },
        }));
      }

      // NEW: notify post owner about reaction
      try {
        const qr = await ddb.send(new QueryCommand({
          TableName: POSTS_TABLE,
          IndexName: 'byId',
          KeyConditionExpression: 'id = :id',
          ExpressionAttributeValues: { ':id': postId },
          Limit: 1,
        }));
        const post = (qr.Items || [])[0];
        if (post && post.userId && post.userId !== userId) {
          await createNotification(post.userId, 'reaction', userId, postId, 'reacted to your post');
        }
      } catch (e) { console.error('notify reaction post owner failed', e); }

      // Return the caller’s current reaction so the UI can refresh accurately
      const self = await ddb.send(new GetCommand({
        TableName: REACTIONS_TABLE,
        Key: { pk: `POST#${postId}`, sk: `USER#${userId}` },
        ProjectionExpression: 'emoji',
        ConsistentRead: true
      }));
      return ok({ ok: true, my: self.Item?.emoji ? [self.Item.emoji] : [] });
    }

    // ----- /username -----
    if (route === 'POST /username') {
      if (!userId) return bad('Unauthorized', 401);
      const body = JSON.parse(event.body || '{}');
      const candidate = String(body.handle || '').trim().toLowerCase();
      if (!/^[a-z0-9_]{3,20}$/.test(candidate)) {
        return bad('Handle must be 3-20 chars, letters/numbers/underscore', 400);
      }

      const taken = await ddb.send(new GetCommand({
        TableName: USERS_TABLE,
        Key: { pk: `HANDLE#${candidate}` },
        ConsistentRead: true,
      }));
      if (taken.Item) return bad('Handle already taken', 409);

      await ddb.send(new PutCommand({
        TableName: USERS_TABLE,
        Item: { pk: `HANDLE#${candidate}`, userId },
        ConditionExpression: 'attribute_not_exists(pk)',
      }));

      await ddb.send(new PutCommand({
        TableName: USERS_TABLE,
        Item: { pk: `USER#${userId}`, handle: candidate, type: 'HANDLE', userId },
        ConditionExpression: 'attribute_not_exists(pk)',
      }));

      return ok({ handle: candidate });
    }

    // ----- /feed (prefer following; also include my own posts; fallback to global) -----
    if (route === 'GET /feed') {
      if (!userId) return bad('Unauthorized', 401);

      try {
        if (FOLLOWS_TABLE) {
          const following = await ddb.send(new QueryCommand({
            TableName: FOLLOWS_TABLE,
            KeyConditionExpression: 'pk = :me',
            ExpressionAttributeValues: { ':me': userId },
            ProjectionExpression: 'sk',
            Limit: 500,
            ConsistentRead: true,
          }));
          const followIds = new Set((following.Items || []).map(i => i.sk));
          followIds.add(userId);

          if (followIds.size > 0) {
            const results = [];
            for (const fid of followIds) {
              const r = await ddb.send(new QueryCommand({
                TableName: POSTS_TABLE,
                KeyConditionExpression: 'pk = :p',
                ExpressionAttributeValues: { ':p': `USER#${fid}` },
                ScanIndexForward: false,
                Limit: 20,
                ConsistentRead: true,
              }));
              (r.Items || []).forEach(i => results.push(i));
            }
            results.sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0));

            const items = results.slice(0, 50).map(i => ({
              id: i.id, userId: i.userId, username: i.username || 'unknown',
              handle: i.handle || null,
              text: i.text || '', imageKey: i.imageKey || null,
              avatarKey: i.avatarKey || null,
              createdAt: i.createdAt,
            }));

            // Hydrate avatars/handles
            try {
              const summaries = await fetchUserSummaries(items.map(i => i.userId));
              const avatarMap = {};
              const handleMap = {};
              for (const u of summaries) {
                if (u.userId) {
                  avatarMap[u.userId] = u.avatarKey || null;
                  handleMap[u.userId] = u.handle || null;
                }
              }
              const looksLikeUuid = (s) => !!s && /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i.test(String(s));
              for (const it of items) {
                const a = avatarMap[it.userId];
                if (a) it.avatarKey = a;
                const h = handleMap[it.userId];
                if (h) {
                  it.handle = h;
                  if ((!it.username || it.username === 'unknown' || looksLikeUuid(it.username))) {
                    it.username = h;
                  }
                }
              }
            } catch (e) {
              console.error('FEED avatar/handle hydrate failed', e);
            }

            return ok({ items });
          }
        }

        // fallback to global GSI 'gsi1'
        const gf = await ddb.send(new QueryCommand({
          TableName: POSTS_TABLE,
          IndexName: 'gsi1',
          KeyConditionExpression: 'gsi1pk = :g',
          ExpressionAttributeValues: { ':g': 'FEED' },
          ScanIndexForward: false,
          Limit: 50,
          ConsistentRead: true,
        }));

        const items = (gf.Items || []).map(i => ({
          id: i.id, userId: i.userId, username: i.username || 'unknown',
          handle: i.handle || null,
          text: i.text || '', imageKey: i.imageKey || null,
          avatarKey: i.avatarKey || null,
          createdAt: i.createdAt,
        }));

        try {
          const summaries = await fetchUserSummaries(items.map(i => i.userId));
          const avatarMap = {};
          const handleMap = {};
          for (const u of summaries) {
            if (u.userId) {
              avatarMap[u.userId] = u.avatarKey || null;
              handleMap[u.userId] = u.handle || null;
            }
          }
          const looksLikeUuid = (s) => !!s && /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i.test(String(s));
          for (const it of items) {
            const a = avatarMap[it.userId];
            if (a) it.avatarKey = a;
            const h = handleMap[it.userId];
            if (h) {
              it.handle = h;
              if ((!it.username || it.username === 'unknown' || looksLikeUuid(it.username))) {
                it.username = h;
              }
            }
          }
        } catch (e) {
          console.error('FEED avatar/handle hydrate failed', e);
        }

        return ok({ items });
      } catch (e) {
        console.error('FEED_ERROR', e);
        return bad('Server error', 500);
      }
    }

    // ----- /posts (create) -----
    if (route === 'POST /posts') {
      if (!userId) return bad('Unauthorized', 401);
      const body = JSON.parse(event.body || '{}');
      const id = crypto.randomUUID();
      const now = Date.now();

      const handle = await getHandleForUserId(userId);
      const display = handle || usernameFallback;

      const userProfile = await ddb.send(new GetCommand({
        TableName: USERS_TABLE,
        Key: { pk: `USER#${userId}` },
        ConsistentRead: true,
      }));
      const avatarKey = userProfile.Item?.avatarKey || null;

      const item = {
        pk: `USER#${userId}`,
        sk: `POST#${now}`,
        gsi1pk: 'FEED',
        gsi1sk: String(now),
        id, userId,
        handle: handle || null,
        username: display,
        avatarKey,
        text: String(body.text || '').slice(0, 500),
        createdAt: now,
      };
      if (body.imageKey) item.imageKey = body.imageKey;

      await ddb.send(new PutCommand({ TableName: POSTS_TABLE, Item: item }));

      // NEW: detect mentions in post text
      try {
        const text = String(body.text || '');
        const mentionRegex = /@([a-z0-9_]+)/gi;
        const mentions = [...text.matchAll(mentionRegex)].map(m => m[1].toLowerCase());
        for (const h of mentions) {
          const mid = await userIdFromHandle(h);
          if (mid && mid !== userId) {
            await createNotification(mid, 'mention', userId, id, 'mentioned you in a post');
          }
        }
      } catch (e) { console.error('notify mentions (post) failed', e); }

      return ok({ id });
    }

    // ----- /upload-url (media for posts) -----
    if (route === 'POST /upload-url') {
      if (!userId) return bad('Unauthorized', 401);
      const { contentType } = JSON.parse(event.body || '{}');
      const key = `u/${userId}/${Date.now()}-${crypto.randomUUID()}`;
      const put = new PutObjectCommand({
        Bucket: MEDIA_BUCKET,
        Key: key,
        ContentType: contentType || 'application/octet-stream',
      });
      const url = await getSignedUrl(s3, put, { expiresIn: 60 });
      return ok({ url, key });
    }

    // ----- /avatar-url (upload for profile avatar) -----
    if (route === 'POST /avatar-url') {
      if (!userId) return bad('Unauthorized', 401);
      const { contentType } = JSON.parse(event.body || '{}');
      const key = `a/${userId}/${Date.now()}-${crypto.randomUUID()}`;
      const put = new PutObjectCommand({
        Bucket: MEDIA_BUCKET,
        Key: key,
        ContentType: contentType || 'image/jpeg',
      });
      const url = await getSignedUrl(s3, put, { expiresIn: 60 });
      return ok({ url, key });
    }

    // ----- PATCH /posts/{id} -----
    if (method === 'PATCH' && path.startsWith('/posts/')) {
      if (!userId) return bad('Unauthorized', 401);
      const id = path.split('/')[2];
      const body = JSON.parse(event.body || '{}');

      let qr;
      try {
        qr = await ddb.send(new QueryCommand({
          TableName: POSTS_TABLE,
          IndexName: 'byId',
          KeyConditionExpression: 'id = :id',
          ExpressionAttributeValues: { ':id': id },
          Limit: 1,
        }));
      } catch (e) {
        return ok({ message: 'POSTS_TABLE needs GSI "byId" (PK: id)', detail: String(e) }, 501);
      }
      const post = (qr.Items || [])[0];
      if (!post) return bad('Not found', 404);
      if (post.userId !== userId) return bad('Forbidden', 403);

      const sets = [];
      const names = {};
      const values = { ':u': userId };
      const remove = [];

      if (typeof body.text === 'string') {
        sets.push('#text = :text');
        names['#text'] = 'text';
        values[':text'] = String(body.text).slice(0, 500);
      }
      if (typeof body.imageKey === 'string') {
        sets.push('#imageKey = :imageKey');
        names['#imageKey'] = 'imageKey';
        values[':imageKey'] = body.imageKey;
      }
      if (body.deleteImage) {
        remove.push('#imageKey');
        names['#imageKey'] = 'imageKey';
      }

      const UpdateExpression =
        (sets.length ? ('SET ' + sets.join(', ')) : '') +
        (remove.length ? (sets.length ? ' ' : '') + 'REMOVE ' + remove.join(', ') : '');

      if (!UpdateExpression) return ok({ message: 'No changes' });

      await ddb.send(new UpdateCommand({
        TableName: POSTS_TABLE,
        Key: { pk: post.pk, sk: post.sk },
        UpdateExpression,
        ExpressionAttributeNames: Object.keys(names).length ? names : undefined,
        ExpressionAttributeValues: Object.keys(values).length ? values : undefined,
        ConditionExpression: 'userId = :u'
      }));

      return ok({ ok: true });
    }

    // ----- DELETE /posts/{id} -----
    if (method === 'DELETE' && path.startsWith('/posts/')) {
      if (!userId) return bad('Unauthorized', 401);
      const id = path.split('/')[2];

      let qr;
      try {
        qr = await ddb.send(new QueryCommand({
          TableName: POSTS_TABLE,
          IndexName: 'byId',
          KeyConditionExpression: 'id = :id',
          ExpressionAttributeValues: { ':id': id },
          Limit: 1,
        }));
      } catch (e) {
        return ok({ message: 'POSTS_TABLE needs GSI "byId" (PK: id)', detail: String(e) }, 501);
      }
      const post = (qr.Items || [])[0];
      if (!post) return bad('Not found', 404);
      if (post.userId !== userId) return bad('Forbidden', 403);

      await ddb.send(new DeleteCommand({
        TableName: POSTS_TABLE,
        Key: { pk: post.pk, sk: post.sk },
        ConditionExpression: 'userId = :u',
        ExpressionAttributeValues: { ':u': userId }
      }));

      return ok({ ok: true });
    }

    // ----- GET /posts/{id} -----
    if (method === 'GET' && path.startsWith('/posts/')) {
      const id = path.split('/')[2];
      if (!id) return bad('Missing id', 400);

      let qr;
      try {
        qr = await ddb.send(new QueryCommand({
          TableName: POSTS_TABLE,
          IndexName: 'byId',
          KeyConditionExpression: 'id = :id',
          ExpressionAttributeValues: { ':id': id },
          Limit: 1,
          }));
      } catch (e) {
        return ok({ message: 'POSTS_TABLE needs GSI \"byId\" (PK: id)', detail: String(e) }, 501);
      }
      const post = (qr.Items || [])[0];
      if (!post) return bad('Not found', 404);

      // Hydrate latest handle/avatar to be safe
      try {
        const profiles = await fetchUserSummaries([post.userId]);
        if (profiles && profiles[0]) {
          post.handle = profiles[0].handle || post.handle || null;
          post.avatarKey = profiles[0].avatarKey ?? post.avatarKey ?? null;
        }
      } catch (e) { console.error('GET /posts hydrate failed', e); }

      return ok({
        id: post.id,
        userId: post.userId,
        username: post.username || 'unknown',
        handle: post.handle || null,
        text: post.text || '',
        imageKey: post.imageKey || null,
        avatarKey: post.avatarKey || null,
        createdAt: post.createdAt,
      });
    }

    // ----- /u/:handle, /u/:handle/followers, /u/:handle/following, /u/:handle/posts -----
    const userRoute = matchUserRoutes(path);
    if (method === 'GET' && userRoute) {
      if (!userId) return bad('Unauthorized', 401);

      const h = userRoute.handle.toLowerCase();
      const u = await ddb.send(new GetCommand({
        TableName: USERS_TABLE,
        Key: { pk: `HANDLE#${h}` },
        ConsistentRead: true,
      }));
      if (!u.Item?.userId) return bad('User not found', 404);
      const targetId = u.Item.userId;

      if (userRoute.kind === 'user') {
        const profile = await ddb.send(new GetCommand({
          TableName: USERS_TABLE,
          Key: { pk: `USER#${targetId}` },
          ConsistentRead: true,
        }));

        const [followerCount, followingCount, iFollow] = await Promise.all([
          countFollowers(targetId),
          countFollowing(targetId),
          isFollowing(userId, targetId),
        ]);

        
        let isFollowPending = false;
        try { if (!iFollow) { isFollowPending = await hasPendingFollow(userId, targetId); } } catch (e) { console.error('compute isFollowPending', e); }
const items = await listPostsByUserId(targetId, 50);

        // Hydrate profile posts with fresh avatar/handle
        try {
          const summaries = await fetchUserSummaries([targetId]);
          const freshAvatar = (summaries[0] && summaries[0].avatarKey) || null;
          const freshHandle = (summaries[0] && summaries[0].handle) || null;
          if (freshAvatar) { for (const it of items) it.avatarKey = freshAvatar; }
          if (freshHandle) { for (const it of items) { it.username = (it.username && it.username !== 'unknown') ? it.username : freshHandle; it.handle = freshHandle; } }
        } catch (e) { console.error('PROFILE avatar/handle hydrate failed', e); }

        return ok({ handle: h,
          userId: targetId,
          exists: !!profile.Item,
          avatarKey: profile.Item?.avatarKey || null,
          fullName: profile.Item?.fullName || null,
          followerCount,
          followingCount,
          followers: followerCount,
          following: followingCount,
          isFollowing: iFollow,
          items,
          posts: items,
          isFollowPending, followStatus: (iFollow ? 'following' : (isFollowPending ? 'pending' : 'none')) });
      }

      if (userRoute.kind === 'followers') {
        if (!FOLLOWS_TABLE) return bad('Follows not enabled', 500);
        const scan = await ddb.send(new ScanCommand({
          TableName: FOLLOWS_TABLE,
          FilterExpression: 'sk = :t',
          ExpressionAttributeValues: { ':t': targetId },
          ProjectionExpression: 'pk',
          ConsistentRead: true,
        }));
        const followerIds = (scan.Items || []).map(i => i.pk).filter(Boolean);
        const users = await fetchUserSummaries(followerIds);
        const items = [];
        for (const u of users) {
          let following = false;
          if (FOLLOWS_TABLE && userId) {
            const rel = await ddb.send(new GetCommand({
              TableName: FOLLOWS_TABLE,
              Key: { pk: userId, sk: u.userId },
              ConsistentRead: true,
            }));
            following = !!rel.Item;
          }
          items.push({
            handle: u.handle,
            fullName: u.fullName || null,
            avatarKey: u.avatarKey || null,
            userId: u.userId,
            isFollowing: following,
          });
        }
        return ok({ items, _debugFollowerIdCount: followerIds.length });
      }

      if (userRoute.kind === 'following') {
        if (!FOLLOWS_TABLE) return bad('Follows not enabled', 500);
        const q = await ddb.send(new QueryCommand({
          TableName: FOLLOWS_TABLE,
          KeyConditionExpression: 'pk = :p',
          ExpressionAttributeValues: { ':p': targetId },
          ProjectionExpression: 'sk',
          ConsistentRead: true,
        }));
        const followingIds = (q.Items || []).map(i => i.sk).filter(Boolean);
        const users = await fetchUserSummaries(followingIds);
        const items = [];
        for (const u of users) {
          let viewerFollows = false;
          if (FOLLOWS_TABLE && userId) {
            const rel = await ddb.send(new GetCommand({
              TableName: FOLLOWS_TABLE,
              Key: { pk: userId, sk: u.userId },
              ConsistentRead: true,
            }));
            viewerFollows = !!rel.Item;
          }
          items.push({
            handle: u.handle,
            fullName: u.fullName || null,
            avatarKey: u.avatarKey || null,
            userId: u.userId,
            isFollowing: viewerFollows,
          });
        }
        return ok({ items, _debugFollowingIdCount: followingIds.length });
      }

      if (userRoute.kind === 'posts') {
        const items = await listPostsByUserId(targetId, 50);
        try {
          const summaries = await fetchUserSummaries([targetId]);
          const fresh = (summaries[0] && summaries[0].avatarKey) || null;
          if (fresh) { for (const it of items) it.avatarKey = fresh; }
        } catch (e) { console.error('USER POSTS avatar hydrate failed', e); }
        return ok({ items });
      }
    }

    // ----- /search?q=prefix -----
    if (route === 'GET /search') {
      if (!userId) return bad('Unauthorized', 401);

      const qs = event?.queryStringParameters || {};
      const q = String(qs.q || '').replace(/^@/, '').trim().toLowerCase();
      if (!q) return ok({ items: [] });

      let items = [];
      try {
        const qr = await ddb.send(new QueryCommand({
          TableName: USERS_TABLE,
          IndexName: 'byHandle',
          KeyConditionExpression: '#t = :H AND begins_with(#h, :q)',
          ExpressionAttributeNames: { '#t': 'type', '#h': 'handle' },
          ExpressionAttributeValues: { ':H': 'HANDLE', ':q': q },
          Limit: 25,
          ConsistentRead: true,
        }));
        items = qr.Items || [];
      } catch (e) {
        console.error('GSI byHandle query failed', e);
      }

      const scan = await ddb.send(new ScanCommand({
        TableName: USERS_TABLE,
        ProjectionExpression: 'pk, handle, userId, fullName, avatarKey',
        FilterExpression: 'begins_with(pk, :p)',
        ExpressionAttributeValues: { ':p': 'USER#' },
        Limit: 1000,
        ConsistentRead: true,
      }));
      const extra = (scan.Items || []).filter(it => {
        const h = (it.handle || '').toLowerCase();
        const n = (it.fullName || '').toLowerCase();
        return (h.includes(q) || n.includes(q));
      });
      const byId = new Map();
      for (const it of [...items, ...extra]) {
        const id = it.userId;
        if (id && !byId.has(id)) byId.set(id, it);
      }
      items = Array.from(byId.values()).slice(0, 25);

      const out = [];
      for (const it of items) {
        const handle = it.handle;
        const targetId = it.userId;
        if (!handle || !targetId) continue;

        let following = false;
        if (FOLLOWS_TABLE) {
          const rel = await ddb.send(new GetCommand({
            TableName: FOLLOWS_TABLE,
            Key: { pk: userId, sk: targetId },
            ConsistentRead: true,
          }));
          following = !!rel.Item;
        }

        out.push({ handle, fullName: it.fullName || null, avatarKey: it.avatarKey || null, isFollowing: following });
      }
      return ok({ items: out });
    }

    // ----- /invites (admin only) -----
    if (route === 'POST /invites') {
      if (!userId) return bad('Unauthorized', 401);
      if (!ADMIN_EMAILS.includes(email)) return bad('Forbidden', 403);

      const body = JSON.parse(event.body || '{}');
      const uses = Math.max(1, Math.min(100, Number(body.uses || 1)));
      const code = (crypto.randomUUID().slice(0, 8)).toUpperCase();

      await ddb.send(new PutCommand({
        TableName: INVITES_TABLE,
        Item: { code, usesRemaining: uses },
      }));

      return ok({ code, uses });
    }

    // ----- follow/unfollow (legacy direct follow; you can keep alongside requests) -----
    if (route === 'POST /follow') {
      if (!userId) return bad('Unauthorized', 401);
      const body = JSON.parse(event.body || '{}');
      const handle = String(body.handle || '').trim().toLowerCase();
      const targetId = await userIdFromHandle(handle);
      if (!targetId) return bad('Unknown user', 404);
      if (targetId === userId) return bad('Cannot follow yourself', 400);
      await ddb.send(new PutCommand({
        TableName: FOLLOWS_TABLE,
        Item: { pk: userId, sk: targetId },
      }));
      // Notify the target user about the new follower
      try { if (NOTIFICATIONS_TABLE) { await createNotification(targetId, 'follow', userId, null, 'started following you'); } } catch (e) { console.error('follow notify failed', e); }
      return ok({ ok: true });
    }

    if (route === 'POST /unfollow') {
      if (!userId) return bad('Unauthorized', 401);
      const body = JSON.parse(event.body || '{}');
      const handle = String(body.handle || '').trim().toLowerCase();
      const targetId = await userIdFromHandle(handle);
      if (!targetId) return bad('Unknown user', 404);
      if (targetId === userId) return bad('Cannot unfollow yourself', 400);
      await ddb.send(new DeleteCommand({
        TableName: FOLLOWS_TABLE,
        Key: { pk: userId, sk: targetId },
      }));
      // Remove any existing 'follow' notification
      try { if (NOTIFICATIONS_TABLE && targetId !== userId) { await deleteNotifications(targetId, 'follow', userId, null); } } catch (e) { console.error('unfollow cleanup notify failed', e); }
      return ok({ ok: true });
    }

    // ----- default -----
    return bad('Not found', 404);

  } catch (err) {
    console.error(err);
    return bad('Server error', 500);
  }
};


// Determine if 'requesterId' has a pending follow_request to 'targetId'
async function hasPendingFollow(requesterId, targetId) {
  try {
    if (!NOTIFICATIONS_TABLE || !requesterId || !targetId) return false;
    const q = await ddb.send(new QueryCommand({
      ConsistentRead: true,
      TableName: NOTIFICATIONS_TABLE,
      KeyConditionExpression: 'pk = :p',
      ExpressionAttributeValues: { ':p': `USER#${targetId}` },
      ScanIndexForward: false,
      Limit: 200,
    }));
    const items = q.Items || [];
    for (const it of items) {
      if (it.type === 'follow_request' && String(it.fromUserId) === String(requesterId)) {
        return true;
      }
    }
  } catch (e) { console.error('hasPendingFollow error', e); }
  return false;
}
