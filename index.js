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
const POSTS_TABLE   = process.env.POSTS_TABLE;
const USERS_TABLE   = process.env.USERS_TABLE;
const INVITES_TABLE = process.env.INVITES_TABLE;
const FOLLOWS_TABLE = process.env.FOLLOWS_TABLE; // optional but recommended
const MEDIA_BUCKET  = process.env.MEDIA_BUCKET;
const COMMENTS_TABLE  = process.env.COMMENTS_TABLE;   // pk: POST#<postId>, sk: C#<ts>#<uuid>
const REACTIONS_TABLE = process.env.REACTIONS_TABLE;  // pk: POST#<postId>, sk: COUNT#<emoji>  (count item)
                                                     // pk: POST#<postId>, sk: USER#<userId>  (who reacted + emoji)


const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || '*';
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

// ---------- Helpers ----------
const ok = (body, status = 200, extraHeaders = {}) => ({
  statusCode: status,
  headers: {
    'Access-Control-Allow-Origin': ALLOWED_ORIGIN,
    'Access-Control-Allow-Credentials': 'true',
    'Access-Control-Allow-Headers': 'authorization,content-type',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    ...extraHeaders,
  },
  body: JSON.stringify(body),
});

const bad = (message = 'Bad Request', status = 400) => ok({ message }, status);

const claimsFrom = (event) => event?.requestContext?.authorizer?.jwt?.claims || {};

async function getHandleForUserId(userId) {
  const r = await ddb.send(new GetCommand({
    TableName: USERS_TABLE,
    Key: { pk: `USER#${userId}` },
    ConsistentRead: true,
  }));
  return r.Item?.handle || null;
}

const getCorsHeaders = (origin) => {
  const allowedOrigins = [
    'https://app.scooterbooter.com',
    'http://localhost:5173'
  ];
  
  const corsOrigin = allowedOrigins.includes(origin) ? origin : 'https://app.scooterbooter.com';
  
  return {
    'Access-Control-Allow-Origin': corsOrigin,
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With, Accept, Origin',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Credentials': 'true',
    'Access-Control-Max-Age': '86400'
  };
};

exports.handler = async (event) => {
  const origin = event.headers?.origin || event.headers?.Origin;
  const corsHeaders = getCorsHeaders(origin);
  
  // Handle preflight OPTIONS requests
  if (event.httpMethod === 'OPTIONS' || event.requestContext?.http?.method === 'OPTIONS') {
    return {
      statusCode: 200,
      headers: corsHeaders,
      body: ''
    };
  }
  
  try {
    const method = event.httpMethod || event.requestContext?.http?.method;
    const path = event.path || event.requestContext?.http?.path;
    
    // Your routing logic here
    switch (method) {
      case 'GET':
        // Handle GET requests
        break;
      case 'POST':
        // Handle POST requests
        break;
      default:
        return {
          statusCode: 405,
          headers: corsHeaders,
          body: JSON.stringify({ error: 'Method not allowed' })
        };
    }
    
    // Success response
    return {
      statusCode: 200,
      headers: corsHeaders,
      body: JSON.stringify({ message: 'Success' })
    };
    
  } catch (error) {
    console.error('Error:', error);
    return {
      statusCode: 500,
      headers: corsHeaders,
      body: JSON.stringify({ error: 'Internal server error' })
    };
  }
};


// normalize /prod/posts -> /posts and trim trailing slashes
function normalizePath(event) {
  const method = (event?.requestContext?.http?.method || '').toUpperCase();
  const rawPath = event?.rawPath || event?.requestContext?.http?.path || '';
  const stage   = event?.requestContext?.stage || '';

  let path = rawPath || '/';
  if (stage && path.startsWith('/' + stage + '/')) {
    path = path.slice(stage.length + 1);
  }
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

// ---- UPDATED: ConsistentRead everywhere for follow state ----
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
// Falls back to deriving userId from the "USER#<id>" key if the userId attribute is missing.
// DynamoDB BatchGet max = 100 keys per call.
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
          ProjectionExpression: 'pk, handle, userId, avatarKey',
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
      });
    }
  }

  // Fallback to per-item Get if batch didn’t return anything
  if (out.length === 0 && unique.length > 0) {
    for (const id of unique) {
      const r = await ddb.send(new GetCommand({
        TableName: USERS_TABLE,
        Key: { pk: `USER#${id}` },
        ProjectionExpression: 'pk, handle, userId, avatarKey',
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
      });
    }
  }

  return out;
}

// ---------- Handler ----------
module.exports.handler = async (event) => {
  if (event?.requestContext?.http?.method === 'OPTIONS') return ok({});

  const { method, rawPath, stage, path, route } = normalizePath(event);
  console.log('ROUTE', { method, rawPath, stage, normalized: path });

  const claims = claimsFrom(event);
  const userId = claims.sub;
  const email  = (claims.email || '').toLowerCase();
  const usernameFallback = claims['cognito:username'] || email || userId || 'user';

  try {
    // (A) Return current profile (add avatarKey so UI can show it)
    if (route === 'GET /me') {
      if (!userId) return ok({ message: 'Unauthorized' }, 401)
      const r = await ddb.get({ TableName: USERS_TABLE, Key: { pk: `USER#${userId}` } }).promise()
      return ok({ handle: r.Item?.handle ?? null, email, avatarKey: r.Item?.avatarKey ?? null })
    }
    
    // (B) Set avatar for current user (after client uploads to S3)
    if (route === 'POST /me/avatar') {
      if (!userId) return ok({ message: 'Unauthorized' }, 401)
      const { key } = JSON.parse(event.body || '{}')
      if (!key) return ok({ message: 'Missing key' }, 400)
    
      // save on user row
      await ddb.put({
        TableName: USERS_TABLE,
        Item: { pk: `USER#${userId}`, avatarKey: key },
        // ensure row exists even if user never opened profile before
      }).promise()
    
      // also record handle mapping if it already exists (read then update)
      const u = await ddb.get({ TableName: USERS_TABLE, Key: { pk: `USER#${userId}` } }).promise()
      if (u.Item?.handle) {
        await ddb.put({
          TableName: USERS_TABLE,
          Item: { pk: `HANDLE#${u.Item.handle}`, userId, avatarKey: key }
        }).promise()
      }
      return ok({ success: true, avatarKey: key })
    }

    // GET /posts/{id}/comments
    if (event.requestContext.http.method === 'GET' && /^\/posts\/[^/]+\/comments$/.test(event.requestContext.http.path)) {
      if (!COMMENTS_TABLE) return ok({ message: 'Comments not enabled' }, 501);
    
      const postId = decodeURIComponent(event.requestContext.http.path.split('/')[2]);
      // newest first? switch ScanIndexForward to true for oldest first
      const r = await ddb.send(new QueryCommand({
        TableName: COMMENTS_TABLE,
        KeyConditionExpression: 'pk = :p',
        ExpressionAttributeValues: { ':p': `POST#${postId}` },
        ScanIndexForward: true,
        Limit: 50
      }));
    
      const items = (r.Items || []).map(it => ({
        id: it.id,
        userHandle: it.userHandle || 'unknown',
        text: it.text || '',
        createdAt: it.createdAt || 0,
      }));
      // NOTE: add pagination later with LastEvaluatedKey → nextCursor
      return ok({ items });
    }
    
    // POST /posts/{id}/comments  body: { text }
    if (event.requestContext.http.method === 'POST' && /^\/posts\/[^/]+\/comments$/.test(event.requestContext.http.path)) {
      if (!COMMENTS_TABLE) return ok({ message: 'Comments not enabled' }, 501);
      if (!userId) return ok({ message: 'Unauthorized' }, 401);
    
      const postId = decodeURIComponent(event.requestContext.http.path.split('/')[2]);
      const body = JSON.parse(event.body || '{}');
      const text = String(body.text || '').trim().slice(0, 500);
      if (!text) return ok({ message: 'Text required' }, 400);
    
      const handle = await getHandle(userId) || 'unknown';
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
    
      return ok({ id, userHandle: handle, text, createdAt: now });
    }

    // GET /posts/{id}/reactions  -> { counts: { '👍':2, ... }, mine: '❤️' | null }
    if (event.requestContext.http.method === 'GET' && /^\/posts\/[^/]+\/reactions$/.test(event.requestContext.http.path)) {
      if (!REACTIONS_TABLE) return ok({ message: 'Reactions not enabled' }, 501);
      const postId = decodeURIComponent(event.requestContext.http.path.split('/')[2]);
    
      // get all counts for this post
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
    
      // If userId present but we didn't see their USER# row in the query (because projection filtered?), read it directly
      if (userId && mine === null) {
        const ur = await ddb.send(new GetCommand({
          TableName: REACTIONS_TABLE,
          Key: { pk: `POST#${postId}`, sk: `USER#${userId}` },
          ProjectionExpression: 'emoji',
          ConsistentRead: true
        }));
        mine = ur.Item?.emoji || null;
      }
    
      return ok({ counts, mine });
    }
    
    // POST /posts/{id}/reactions  body: { emoji, action:'toggle' }
    if (event.requestContext.http.method === 'POST' && /^\/posts\/[^/]+\/reactions$/.test(event.requestContext.http.path)) {
      if (!REACTIONS_TABLE) return ok({ message: 'Reactions not enabled' }, 501);
      if (!userId) return ok({ message: 'Unauthorized' }, 401);
    
      const postId = decodeURIComponent(event.requestContext.http.path.split('/')[2]);
      const body = JSON.parse(event.body || '{}');
      const raw = String(body.emoji || '').trim();
      // whitelist simple emojis; alternatively accept any short string
      const emoji = raw.slice(0, 8); // keep it short; works for common emoji
      if (!emoji) return ok({ message: 'Invalid emoji' }, 400);
    
      // current reaction by this user?
      const current = await ddb.send(new GetCommand({
        TableName: REACTIONS_TABLE,
        Key: { pk: `POST#${postId}`, sk: `USER#${userId}` },
        ProjectionExpression: 'emoji',
        ConsistentRead: true
      }));
      const prev = current.Item?.emoji || null;
    
      // toggle behavior:
      // - if prev === emoji -> remove reaction (decrement count, delete USER row)
      // - if prev !== emoji -> set to new emoji (increment new count; if prev existed, decrement prev)
      if (prev && prev === emoji) {
        // decrement this emoji count
        await ddb.send(new UpdateCommand({
          TableName: REACTIONS_TABLE,
          Key: { pk: `POST#${postId}`, sk: `COUNT#${emoji}` },
          UpdateExpression: 'ADD #c :neg',
          ExpressionAttributeNames: { '#c': 'count' },
          ExpressionAttributeValues: { ':neg': -1 },
        }));
        // delete user row
        await ddb.send(new DeleteCommand({
          TableName: REACTIONS_TABLE,
          Key: { pk: `POST#${postId}`, sk: `USER#${userId}` },
        }));
      } else {
        // increment new emoji
        await ddb.send(new UpdateCommand({
          TableName: REACTIONS_TABLE,
          Key: { pk: `POST#${postId}`, sk: `COUNT#${emoji}` },
          UpdateExpression: 'ADD #c :one',
          ExpressionAttributeNames: { '#c': 'count' },
          ExpressionAttributeValues: { ':one': 1 },
        }));
        // if had a previous different emoji, decrement it
        if (prev) {
          await ddb.send(new UpdateCommand({
            TableName: REACTIONS_TABLE,
            Key: { pk: `POST#${postId}`, sk: `COUNT#${prev}` },
            UpdateExpression: 'ADD #c :neg',
            ExpressionAttributeNames: { '#c': 'count' },
            ExpressionAttributeValues: { ':neg': -1 },
          }));
        }
        // upsert user row
        await ddb.send(new PutCommand({
          TableName: REACTIONS_TABLE,
          Item: { pk: `POST#${postId}`, sk: `USER#${userId}`, emoji },
        }));
      }
    
      return ok({ ok: true });
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
          // include myself
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
              text: i.text || '', imageKey: i.imageKey || null,
              avatarKey: i.avatarKey || null,
              createdAt: i.createdAt,
            }));
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
          text: i.text || '', imageKey: i.imageKey || null,
          avatarKey: i.avatarKey || null,
          createdAt: i.createdAt,
        }));
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

      // get avatar for this user (if any) to attach on post
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
        username: display,
        avatarKey,
        text: String(body.text || '').slice(0, 500),
        createdAt: now,
      };
      if (body.imageKey) item.imageKey = body.imageKey;

      await ddb.send(new PutCommand({ TableName: POSTS_TABLE, Item: item }));
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

    // ----- /me/avatar (save avatarKey) -----
    if (route === 'POST /me/avatar') {
      if (!userId) return bad('Unauthorized', 401);
      const { avatarKey } = JSON.parse(event.body || '{}');
      if (!avatarKey || typeof avatarKey !== 'string') return bad('Invalid avatarKey', 400);

      await ddb.send(new UpdateCommand({
        TableName: USERS_TABLE,
        Key: { pk: `USER#${userId}` },
        UpdateExpression: 'SET avatarKey = :a, #t = if_not_exists(#t, :H), userId = if_not_exists(userId, :uid)',
        ExpressionAttributeNames: { '#t': 'type' },
        ExpressionAttributeValues: { ':a': avatarKey, ':H': 'HANDLE', ':uid': userId },
      }));

      return ok({ avatarKey });
    }

    // ----- /follow -----
    if (route === 'POST /follow') {
      if (!userId) return bad('Unauthorized', 401);
      if (!FOLLOWS_TABLE) return bad('Follows not enabled', 500);

      const { handle } = JSON.parse(event.body || '{}');
      const handleClean = String(handle || '').trim().toLowerCase();
      if (!handleClean) return bad('Missing handle', 400);

      const target = await ddb.send(new GetCommand({
        TableName: USERS_TABLE,
        Key: { pk: `HANDLE#${handleClean}` },
        ConsistentRead: true,
      }));
      const targetId = target.Item?.userId;
      if (!targetId) return bad('User not found', 404);
      if (targetId === userId) return bad('Cannot follow yourself', 400);

      await ddb.send(new PutCommand({
        TableName: FOLLOWS_TABLE,
        Item: { pk: userId, sk: targetId, createdAt: Date.now() },
      }));

      const [followerCount, followingCount] = await Promise.all([
        countFollowers(targetId),
        countFollowing(userId),
      ]);

      return ok({
        followed: handleClean,
        isFollowing: true,
        followerCount,
        followingCount,
        followers: followerCount,      // alias for UI
        following: followingCount,     // alias for UI
      });
    }

    // ----- /unfollow -----
    if (route === 'POST /unfollow') {
      if (!userId) return bad('Unauthorized', 401);
      if (!FOLLOWS_TABLE) return bad('Follows not enabled', 500);

      const { handle } = JSON.parse(event.body || '{}');
      const handleClean = String(handle || '').trim().toLowerCase();
      if (!handleClean) return bad('Missing handle', 400);

      const target = await ddb.send(new GetCommand({
        TableName: USERS_TABLE,
        Key: { pk: `HANDLE#${handleClean}` },
        ConsistentRead: true,
      }));
      const targetId = target.Item?.userId;
      if (!targetId) return bad('User not found', 404);

      await ddb.send(new DeleteCommand({
        TableName: FOLLOWS_TABLE,
        Key: { pk: userId, sk: targetId },
      }));

      const [followerCount, followingCount] = await Promise.all([
        countFollowers(targetId),
        countFollowing(userId),
      ]);

      return ok({
        unfollowed: handleClean,
        isFollowing: false,
        followerCount,
        followingCount,
        followers: followerCount,      // alias for UI
        following: followingCount,     // alias for UI
      });
    }

    if (route === 'GET /posts') {
      const r = await ddb.query({
        TableName: POSTS_TABLE,
        IndexName: 'gsi1',
        KeyConditionExpression: 'gsi1pk = :g',
        ExpressionAttributeValues: { ':g': 'FEED' },
        ScanIndexForward: false,
        Limit: 50
      }).promise()
      const items = (r.Items || []).map(i => ({
        id: i.id,
        userId: i.userId,
        username: i.username || 'unknown',
        avatarKey: i.avatarKey || null,     // 👈
        text: i.text || '',
        imageKey: i.imageKey || null,
        createdAt: i.createdAt
      }))
      return ok({ items })
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

        const items = await listPostsByUserId(targetId, 50);

        return ok({
          handle: h,
          userId: targetId,
          exists: !!profile.Item,
          avatarKey: profile.Item?.avatarKey || null,
          followerCount,
          followingCount,
          followers: followerCount,      // alias
          following: followingCount,     // alias
          isFollowing: iFollow,
          items,        // for UI expecting .items
          posts: items, // for UI expecting .posts
        });
      }

      if (userRoute.kind === 'followers') {
        if (!FOLLOWS_TABLE) return bad('Follows not enabled', 500);
      
        // Get all rows where someone follows the target -> pk = followerId, sk = targetId
        const scan = await ddb.send(new ScanCommand({
          TableName: FOLLOWS_TABLE,
          FilterExpression: 'sk = :t',
          ExpressionAttributeValues: { ':t': targetId },
          ProjectionExpression: 'pk',   // follower userId
          ConsistentRead: true,
        }));
      
        const followerIds = (scan.Items || []).map(i => i.pk).filter(Boolean);
      
        const users = await fetchUserSummaries(followerIds);
      
        // annotate whether *viewer* is following each returned user
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
            avatarKey: u.avatarKey,
            isFollowing: following,
          });
        }
      
        // Small debug count so you can verify the API is seeing followers at all
        return ok({ items, _debugFollowerIdCount: followerIds.length });
      }
      
      // ... inside your "GET /u/:handle/(followers|following)" route handler ...

    if (userRoute.kind === 'following') {
      if (!FOLLOWS_TABLE) return bad('Follows not enabled', 500);
    
      // People that TARGET is following: rows where pk = targetId
      const q = await ddb.send(new QueryCommand({
        TableName: FOLLOWS_TABLE,
        KeyConditionExpression: 'pk = :p',
        ExpressionAttributeValues: { ':p': targetId },
        ProjectionExpression: 'sk', // the followed user's id
        ConsistentRead: true,
      }));
    
      const followingIds = (q.Items || []).map(i => i.sk).filter(Boolean);
    
      // Hydrate to handles / avatars
      const users = await fetchUserSummaries(followingIds);
    
      // Annotate whether the VIEWER follows each returned user
      const items = [];
      for (const u of users) {
        let viewerFollows = false;
        if (FOLLOWS_TABLE && userId) {
          const rel = await ddb.send(new GetCommand({
            TableName: FOLLOWS_TABLE,
            Key: { pk: userId, sk: u.userId }, // does VIEWER follow this user?
            ConsistentRead: true,
          }));
          viewerFollows = !!rel.Item;
        }
        items.push({
          handle: u.handle,
          avatarKey: u.avatarKey || null,
          isFollowing: viewerFollows,
        });
      }
    
      return ok({ items, _debugFollowingIdCount: followingIds.length });
    }

      

      if (userRoute.kind === 'posts') {
        const items = await listPostsByUserId(targetId, 50);
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
        // Prefer GSI byHandle (type=HANDLE, handle begins_with)
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

      // Fallback: scan USER# rows and JS-prefix filter
      if (items.length === 0) {
        const scan = await ddb.send(new ScanCommand({
          TableName: USERS_TABLE,
          ProjectionExpression: 'pk, handle, userId',
          FilterExpression: 'begins_with(pk, :p)',
          ExpressionAttributeValues: { ':p': 'USER#' },
          Limit: 1000,
          ConsistentRead: true,
        }));
        items = (scan.Items || [])
          .filter(it => typeof it.handle === 'string')
          .filter(it => it.handle.toLowerCase().startsWith(q))
          .slice(0, 25);
      }

      // Annotate follow status
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
        out.push({ handle, isFollowing: following });
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

    // ----- default -----
    return bad('Not found', 404);

  } catch (err) {
    console.error(err);
    return bad('Server error', 500);
  }
};

