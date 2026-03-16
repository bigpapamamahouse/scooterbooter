// === ScooterBooter Lambda (merged with Notifications / Mentions / Follow-Requests) ===
// ---------- CommonJS + AWS SDK v3 ----------
const crypto = require('crypto');
const { randomUUID } = require('crypto');
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const { pipeline } = require('stream/promises');
const { Readable } = require('stream');

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
  BatchWriteCommand,
} = require('@aws-sdk/lib-dynamodb');

const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const { CognitoIdentityProviderClient, AdminDeleteUserCommand } = require('@aws-sdk/client-cognito-identity-provider');

const { BedrockRuntimeClient, InvokeModelCommand } = require('@aws-sdk/client-bedrock-runtime');

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
const PUSH_TOKENS_TABLE = process.env.PUSH_TOKENS_TABLE; // pk: USER#<userId>, sk: TOKEN#<tokenHash>
const REPORTS_TABLE = process.env.REPORTS_TABLE; // pk: REPORT#<reportId>, sk: <timestamp>
const BLOCKS_TABLE = process.env.BLOCKS_TABLE; // pk: USER#<userId>, sk: BLOCKED#<blockedUserId>
const SCOOPS_TABLE = process.env.SCOOPS_TABLE; // pk: USER#<userId>, sk: SCOOP#<ts>#<uuid> - Stories/Scoops
const POLLS_TABLE = process.env.POLLS_TABLE; // pk: POST#<postId>, sk: OPTION#<optionId> (option+count) or VOTE#<userId> (user vote)
const USER_POOL_ID = process.env.USER_POOL_ID;

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
const ddbClient = new DynamoDBClient({
  requestHandler: {
    requestTimeout: 3000, // 3 second timeout per request to prevent cascading delays
  },
  maxAttempts: 2, // Reduce retries from default 3 to prevent timeout cascade
});
const ddb = DynamoDBDocumentClient.from(ddbClient, {
  marshallOptions: { removeUndefinedValues: true },
});
const s3 = new S3Client({});
const cognito = new CognitoIdentityProviderClient({});
const bedrock = new BedrockRuntimeClient({ region: 'us-east-1' });

// ---------- Video Processing with FFmpeg ----------
// FFmpeg is provided via Lambda Layer: arn:aws:lambda:us-east-1:678705476278:layer:ffmpeg:1
// Or you can use a custom layer built from https://github.com/serverlesspub/ffmpeg-aws-lambda-layer
const FFMPEG_PATH = process.env.FFMPEG_PATH || '/opt/bin/ffmpeg';

/**
 * Process video: trim to specified segment using stream copy (no re-encoding)
 * Video compression is handled client-side before upload, so this function
 * only needs to extract the selected time range.
 * @param {string} bucket - S3 bucket name
 * @param {string} key - S3 object key
 * @param {number} startTime - Start time in seconds
 * @param {number} endTime - End time in seconds
 * @returns {Promise<string>} - New S3 key for trimmed video
 */
async function processVideo(bucket, key, startTime, endTime) {
  const timestamp = Date.now();
  const tmpInput = `/tmp/input_${timestamp}.mp4`;
  const tmpOutput = `/tmp/output_${timestamp}.mp4`;

  try {
    // Log Lambda environment info for debugging
    console.log('[VideoProcess] === Environment Debug Info ===');
    console.log(`[VideoProcess] FFMPEG_PATH env: ${process.env.FFMPEG_PATH || '(not set, using default)'}`);
    console.log(`[VideoProcess] Expected FFmpeg location: ${FFMPEG_PATH}`);
    console.log(`[VideoProcess] Lambda memory: ${process.env.AWS_LAMBDA_FUNCTION_MEMORY_SIZE || 'unknown'} MB`);
    console.log(`[VideoProcess] Lambda temp space: checking /tmp...`);

    // Check /opt directory (where Lambda layers are mounted)
    try {
      const optContents = fs.existsSync('/opt') ? fs.readdirSync('/opt') : [];
      console.log(`[VideoProcess] /opt contents: ${JSON.stringify(optContents)}`);
      if (fs.existsSync('/opt/bin')) {
        const optBinContents = fs.readdirSync('/opt/bin');
        console.log(`[VideoProcess] /opt/bin contents: ${JSON.stringify(optBinContents)}`);
      } else {
        console.log('[VideoProcess] /opt/bin does NOT exist - Lambda layer may not be attached!');
      }
    } catch (e) {
      console.error('[VideoProcess] Error checking /opt:', e.message);
    }

    // Check /tmp space
    try {
      const tmpStats = execSync('df -h /tmp 2>/dev/null || echo "df not available"', { encoding: 'utf8' });
      console.log(`[VideoProcess] /tmp disk space:\n${tmpStats}`);
    } catch (e) {
      console.log('[VideoProcess] Could not check /tmp space');
    }

    console.log('[VideoProcess] === Starting Video Processing ===');
    console.log(`[VideoProcess] Downloading video from s3://${bucket}/${key}`);

    // Download video from S3 using streaming to avoid loading into memory
    const getResult = await s3.send(new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    }));

    // Stream the S3 body directly to a file (memory efficient)
    const writeStream = fs.createWriteStream(tmpInput);
    await pipeline(getResult.Body, writeStream);
    console.log(`[VideoProcess] Downloaded ${fs.statSync(tmpInput).size} bytes (streamed to disk)`);

    // Calculate duration
    const duration = endTime - startTime;
    console.log(`[VideoProcess] Trimming from ${startTime}s to ${endTime}s (${duration}s)`);

    // Check if FFmpeg is available
    try {
      const ffmpegVersion = execSync(`${FFMPEG_PATH} -version`, { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] });
      const versionLine = ffmpegVersion.split('\n')[0];
      console.log(`[VideoProcess] FFmpeg found: ${versionLine}`);
    } catch (e) {
      console.error('[VideoProcess] FFmpeg not available at', FFMPEG_PATH);
      console.error('[VideoProcess] FFmpeg check error:', e.message);
      if (e.stderr) console.error('[VideoProcess] FFmpeg stderr:', e.stderr.toString());
      // Check if file exists
      console.log(`[VideoProcess] File exists at ${FFMPEG_PATH}:`, fs.existsSync(FFMPEG_PATH));
      // Return original key if FFmpeg is not available
      fs.unlinkSync(tmpInput);
      return key;
    }

    // Check input size to determine processing mode
    // If video is large (>1.5 MB/sec), it's likely uncompressed (old app or failed client compression)
    // In that case, do full re-encoding. Otherwise, use fast stream copy.
    const inputSize = fs.statSync(tmpInput).size;
    const mbPerSecond = (inputSize / 1024 / 1024) / duration;
    const COMPRESSION_THRESHOLD_MB_PER_SEC = 1.5; // ~15MB for 10 seconds
    const needsCompression = mbPerSecond > COMPRESSION_THRESHOLD_MB_PER_SEC;

    console.log(`[VideoProcess] Input: ${(inputSize / 1024 / 1024).toFixed(2)} MB, ${duration}s, ${mbPerSecond.toFixed(2)} MB/sec`);
    console.log(`[VideoProcess] Mode: ${needsCompression ? 'FULL RE-ENCODE (large file detected)' : 'STREAM COPY (already compressed)'}`);

    let cmd;
    if (needsCompression) {
      // Full re-encoding for uncompressed videos (fallback for old app or failed client compression)
      cmd = [
        FFMPEG_PATH,
        '-y',
        '-ss', startTime.toString(),
        '-i', tmpInput,
        '-t', duration.toString(),
        '-c:v', 'libx264',
        '-crf', '28',
        '-preset', 'fast',
        '-c:a', 'aac',
        '-b:a', '128k',
        '-movflags', '+faststart',
        '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2',
        tmpOutput,
      ].join(' ');
    } else {
      // Fast stream copy for already-compressed videos
      cmd = [
        FFMPEG_PATH,
        '-y',
        '-ss', startTime.toString(),
        '-i', tmpInput,
        '-t', duration.toString(),
        '-c', 'copy',
        '-avoid_negative_ts', 'make_zero',
        tmpOutput,
      ].join(' ');
    }

    console.log(`[VideoProcess] Running: ${cmd}`);
    const startProcessTime = Date.now();

    try {
      const result = execSync(cmd, {
        maxBuffer: needsCompression ? 50 * 1024 * 1024 : 10 * 1024 * 1024,
        timeout: needsCompression ? 120000 : 30000, // 2 min for re-encode, 30s for stream copy
        encoding: 'utf8',
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      if (result) console.log(`[VideoProcess] FFmpeg stdout: ${result}`);
    } catch (execError) {
      // FFmpeg outputs progress to stderr, so we need to check if output file was created
      if (execError.stderr) {
        console.log(`[VideoProcess] FFmpeg stderr (last 500 chars): ${execError.stderr.toString().slice(-500)}`);
      }
      // Check if output file was created despite error
      if (!fs.existsSync(tmpOutput)) {
        console.error('[VideoProcess] FFmpeg failed - no output file created');
        console.error('[VideoProcess] FFmpeg error:', execError.message);
        throw execError;
      }
      console.log('[VideoProcess] FFmpeg completed (stderr had content but output file exists)');
    }

    // Verify output file was created (even if execSync didn't throw)
    if (!fs.existsSync(tmpOutput)) {
      throw new Error('FFmpeg completed but output file was not created');
    }

    const processTime = Date.now() - startProcessTime;
    const outputStats = fs.statSync(tmpOutput);
    const outputSize = outputStats.size;

    if (outputSize === 0) {
      throw new Error('FFmpeg created empty output file');
    }
    const sizeChange = ((1 - outputSize / inputSize) * 100).toFixed(1);

    console.log(`[VideoProcess] === ${needsCompression ? 'Compression' : 'Trim'} Results ===`);
    console.log(`[VideoProcess] Input size: ${(inputSize / 1024 / 1024).toFixed(2)} MB`);
    console.log(`[VideoProcess] Output size: ${(outputSize / 1024 / 1024).toFixed(2)} MB`);
    console.log(`[VideoProcess] Size reduction: ${sizeChange}%${needsCompression ? ' (re-encoded)' : ' (trimmed only)'}`);
    console.log(`[VideoProcess] Processing time: ${(processTime / 1000).toFixed(1)}s`);

    // Upload processed video back to S3
    // Ensure processed key always has .mp4 extension for proper playback
    const hasExtension = /\.[^./]+$/.test(key);
    let processedKey;
    if (hasExtension) {
      // Replace existing extension with _processed.mp4
      processedKey = key.replace(/\.[^.]+$/, '_processed.mp4');
    } else {
      // No extension - add _processed.mp4
      processedKey = `${key}_processed.mp4`;
    }
    console.log(`[VideoProcess] Uploading to s3://${bucket}/${processedKey}`);

    // Verify file still exists before creating stream
    if (!fs.existsSync(tmpOutput)) {
      throw new Error('Output file disappeared before upload');
    }

    // Upload using stream to avoid loading entire file into memory
    // Wrap in promise to handle stream errors properly
    await new Promise((resolve, reject) => {
      const uploadStream = fs.createReadStream(tmpOutput);

      // Handle stream errors before S3 SDK gets it
      uploadStream.on('error', (err) => {
        console.error('[VideoProcess] Read stream error:', err.message);
        reject(err);
      });

      s3.send(new PutObjectCommand({
        Bucket: bucket,
        Key: processedKey,
        Body: uploadStream,
        ContentType: 'video/mp4',
        ContentLength: outputSize,
      }))
        .then(resolve)
        .catch(reject);
    });

    console.log('[VideoProcess] Upload complete (streamed from disk)');

    // Clean up temp files
    fs.unlinkSync(tmpInput);
    fs.unlinkSync(tmpOutput);

    // Delete original file (only if different from processed key)
    if (key !== processedKey) {
      try {
        await s3.send(new DeleteObjectCommand({
          Bucket: bucket,
          Key: key,
        }));
        console.log('[VideoProcess] Deleted original file');
      } catch (e) {
        console.warn('[VideoProcess] Failed to delete original:', e.message);
      }
    }

    return processedKey;
  } catch (error) {
    console.error('[VideoProcess] Error:', error);
    // Clean up temp files on error
    try { fs.unlinkSync(tmpInput); } catch (e) {}
    try { fs.unlinkSync(tmpOutput); } catch (e) {}
    // Return original key on error
    return key;
  }
}

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
      // Note: GSIs do not support ConsistentRead - only eventually consistent
    }));
    const it = (qr.Items || [])[0];
    if (it && it.userId) return String(it.userId);
  } catch (e) {}

  return null;
}

// NEW: Get user notification preferences
async function getUserNotificationPreferences(userId) {
  if (!USERS_TABLE || !userId) return null;

  try {
    const r = await ddb.send(new GetCommand({
      TableName: USERS_TABLE,
      Key: { pk: `USER#${userId}` },
      ProjectionExpression: 'notificationPreferences',
      ConsistentRead: true,
    }));

    // Default: all notifications enabled
    const defaults = {
      mentions: true,
      comments: true,
      reactions: true,
    };

    return r.Item?.notificationPreferences || defaults;
  } catch (e) {
    console.error('[Preferences] Failed to get notification preferences:', e);
    // Return defaults on error
    return {
      mentions: true,
      comments: true,
      reactions: true,
    };
  }
}

// NEW: notifications helper
async function createNotification(targetUserId, type, fromUserId, postId = null, message = '') {
  if (!NOTIFICATIONS_TABLE || !targetUserId || targetUserId === fromUserId) return;

  // Check user's notification preferences
  const prefs = await getUserNotificationPreferences(targetUserId);

  // Map notification types to preference keys
  const prefMap = {
    'mention': 'mentions',
    'comment': 'comments',
    'reply': 'comments',
    'reaction': 'reactions',
  };

  const prefKey = prefMap[type];

  // If this notification type is mapped to a preference and it's disabled, skip notification
  if (prefKey && prefs && prefs[prefKey] === false) {
    console.log(`[Notifications] Skipping ${type} notification for user ${targetUserId} (preference disabled)`);
    return;
  }

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

  // NEW: Send push notification
  try {
    // Get sender's handle for personalized notification
    let senderHandle = 'Someone';
    try {
      senderHandle = await getHandleForUserId(fromUserId) || 'Someone';
    } catch (e) {
      console.error('[Push] Failed to get sender handle:', e);
    }

    // Create notification title and body based on type
    let title = 'New Notification';
    let body = message;

    switch (type) {
      case 'comment':
        title = `${senderHandle} commented`;
        body = 'commented on your post';
        break;
      case 'reaction':
        title = `${senderHandle} reacted`;
        body = 'reacted to your post';
        break;
      case 'mention':
        title = `${senderHandle} mentioned you`;
        body = 'mentioned you';
        break;
      case 'follow':
        title = `${senderHandle} followed you`;
        body = 'started following you';
        break;
      case 'follow_request':
        title = `${senderHandle} wants to follow you`;
        body = 'sent you a follow request';
        break;
      case 'follow_accept':
        title = `${senderHandle} accepted`;
        body = 'accepted your follow request';
        break;
      case 'follow_declined':
        title = `Follow request declined`;
        body = `${senderHandle} declined your follow request`;
        break;
      case 'reply':
        title = `${senderHandle} replied`;
        body = 'replied to your comment';
        break;
      case 'photo_tag':
        title = `${senderHandle} tagged you`;
        body = 'tagged you in a photo';
        break;
      case 'scoop_reaction':
        title = `${senderHandle} reacted`;
        body = 'reacted to your scoop';
        break;
      case 'scoop_reply':
        title = `${senderHandle} replied`;
        body = 'replied to your scoop';
        break;
      default:
        title = 'New Notification';
        body = message || 'You have a new notification';
    }

    // Fire-and-forget: don't block the API response on push delivery (100-500ms to Expo).
    // The DB notification is already persisted above. For production at scale,
    // consider routing through SQS for reliable async delivery.
    sendPushNotification(targetUserId, title, body, {
      notificationId: id,
      postId: postId || undefined,
      type: type,
    }).catch(err => {
      console.error('[Push] Failed to send push notification:', err);
    });
  } catch (err) {
    console.error('[Push] Failed to prepare push notification in createNotification:', err);
  }
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

// ---------- Content Moderation with Amazon Bedrock ----------

/**
 * Moderate content using Amazon Bedrock (text and/or image)
 * Returns { safe: boolean, reason: string }
 */
async function moderateContent(text, imageKey = null) {
  // Must have at least text or image
  if ((!text || typeof text !== 'string') && !imageKey) {
    return { safe: true, reason: null };
  }

  try {
    // Build content array for multimodal input
    const contentParts = [];

    // Add text prompt
    const prompt = imageKey
      ? `You are a content moderation system for a social media app. Your job is to protect users from harmful content while allowing normal everyday content.

Analyze the provided image and/or text. You MUST block content if it contains ANY of the following:

BLOCK IMMEDIATELY:
- Explicit nudity showing genitals, exposed breasts, or buttocks in a sexual context
- Pornographic content or explicit sexual acts
- Graphic violence, blood, gore, or disturbing imagery
- Hate symbols, slurs, or attacks on protected groups (race, religion, ethnicity, gender, sexual orientation, disability)
- Weapons being used to threaten or harm
- Drug paraphernalia or illegal drug use
- Self-harm or suicide content

ALLOW (safe content):
- Artistic nudity in classical art/sculptures (museums, famous paintings)
- Medical or educational diagrams
- Swimwear, beachwear, or beach photos
- Clothing products (lingerie, sleepwear, etc.) shown in non-sexual product photos
- Sleep masks, eye masks, and similar everyday items
- Fashion photography and modeling (even if revealing, as long as not pornographic)
- Casual profanity in text (when not attacking people/groups)
- Political opinions or criticism

${text ? `Text: "${text}"` : 'No text provided.'}

IMPORTANT: Only block content that is clearly and explicitly pornographic or harmful. Everyday items, clothing products, and non-sexual content should be allowed even if they might be considered mildly suggestive. When in doubt, allow the content.

Respond ONLY with a JSON object:
{"safe": true/false, "reason": "brief explanation if unsafe, null if safe"}`
      : `You are a content moderation system. Analyze the following text and flag ONLY if it contains:

BLOCK if it contains:
- Graphic descriptions of violence or gore
- Hate speech targeting protected groups (race, religion, ethnicity, gender, sexual orientation, disability)
- Direct threats or harassment targeting specific individuals
- Content promoting illegal activities (terrorism, child exploitation, drug trafficking)

ALLOW (do not block):
- Casual profanity or strong language (e.g., "fuck", "shit", "damn") when not directed at groups or individuals
- Political opinions or criticism (even if heated)
- Edgy humor that doesn't target protected groups
- General complaints or frustration
- Sexual jokes, innuendo, or sexually suggestive text (text-only sexual content is allowed)

Text to analyze: "${text}"

Respond ONLY with a JSON object in this exact format:
{"safe": true/false, "reason": "brief explanation if unsafe, null if safe"}`;

    contentParts.push({
      type: "text",
      text: prompt
    });

    // Add image if provided
    if (imageKey) {
      try {
        console.log(`[Moderation] Fetching image from S3: ${imageKey}`);

        // Fetch image from S3
        const getCommand = new GetObjectCommand({
          Bucket: MEDIA_BUCKET,
          Key: imageKey
        });
        const s3Response = await s3.send(getCommand);

        // Convert stream to buffer
        const chunks = [];
        for await (const chunk of s3Response.Body) {
          chunks.push(chunk);
        }
        const imageBuffer = Buffer.concat(chunks);
        const base64Image = imageBuffer.toString('base64');

        console.log(`[Moderation] Image fetched, size: ${imageBuffer.length} bytes`);

        // Determine and normalize media type to Bedrock's strict requirements
        // Bedrock only accepts: image/jpeg, image/png, image/gif, image/webp
        let mediaType = 'image/jpeg'; // Default

        // First check S3 ContentType
        const s3ContentType = s3Response.ContentType?.toLowerCase();
        console.log(`[Moderation] S3 ContentType: ${s3ContentType}`);

        if (s3ContentType) {
          if (s3ContentType.includes('png')) {
            mediaType = 'image/png';
          } else if (s3ContentType.includes('gif')) {
            mediaType = 'image/gif';
          } else if (s3ContentType.includes('webp')) {
            mediaType = 'image/webp';
          } else if (s3ContentType.includes('jpeg') || s3ContentType.includes('jpg')) {
            mediaType = 'image/jpeg';
          }
        }

        // Fallback: check image key for extension hints
        if (mediaType === 'image/jpeg') {
          const keyLower = imageKey.toLowerCase();
          if (keyLower.includes('.png')) {
            mediaType = 'image/png';
          } else if (keyLower.includes('.gif')) {
            mediaType = 'image/gif';
          } else if (keyLower.includes('.webp')) {
            mediaType = 'image/webp';
          }
        }

        console.log(`[Moderation] Normalized media type: ${mediaType}`);

        contentParts.push({
          type: "image",
          source: {
            type: "base64",
            media_type: mediaType,
            data: base64Image
          }
        });

        console.log('[Moderation] Image added to content parts for analysis');
      } catch (imageError) {
        console.error('[Moderation] Failed to fetch image from S3:', imageError);
        console.error('[Moderation] Image key:', imageKey);
        console.error('[Moderation] Bucket:', MEDIA_BUCKET);
        // Continue with text-only moderation if image fetch fails
      }
    }

    const payload = {
      anthropic_version: "bedrock-2023-05-31",
      max_tokens: 200,
      messages: [{
        role: "user",
        content: contentParts
      }]
    };

    console.log(`[Moderation] Calling Bedrock with ${contentParts.length} content parts (text: ${!!text}, image: ${!!imageKey})`);

    const command = new InvokeModelCommand({
      modelId: "anthropic.claude-3-haiku-20240307-v1:0",
      contentType: "application/json",
      accept: "application/json",
      body: JSON.stringify(payload)
    });

    const response = await bedrock.send(command);
    const responseBody = JSON.parse(new TextDecoder().decode(response.body));

    // Extract the content from Claude's response
    const content = responseBody.content[0].text;
    console.log('[Moderation] Bedrock response:', content);

    // Try to parse the JSON response
    try {
      const result = JSON.parse(content);
      const isSafe = result.safe === true;
      console.log(`[Moderation] Result: ${isSafe ? 'SAFE' : 'BLOCKED'} - ${result.reason || 'no reason'}`);
      return {
        safe: isSafe,
        reason: result.reason || null
      };
    } catch (parseErr) {
      console.error('[Moderation] Failed to parse Bedrock response:', content);
      // Default to safe if we can't parse (fail open to prevent blocking all content)
      return { safe: true, reason: null };
    }
  } catch (error) {
    console.error('[Moderation] Bedrock moderation failed:', error);
    // Fail open - allow content if moderation service is down
    return { safe: true, reason: null };
  }
}

// ---------- Blocking helpers ----------

/**
 * Check if userA has blocked userB
 */
async function isBlocked(userA, userB) {
  if (!BLOCKS_TABLE || !userA || !userB) return false;
  try {
    const result = await ddb.send(new GetCommand({
      TableName: BLOCKS_TABLE,
      Key: { pk: `USER#${userA}`, sk: `BLOCKED#${userB}` },
      ConsistentRead: true,
    }));
    return !!result.Item;
  } catch (e) {
    console.error('[Blocking] isBlocked check failed:', e);
    return false;
  }
}

/**
 * Check if there's a bidirectional block between two users
 */
async function hasBlockBetween(userA, userB) {
  if (!userA || !userB) return false;
  const [aBlocksB, bBlocksA] = await Promise.all([
    isBlocked(userA, userB),
    isBlocked(userB, userA)
  ]);
  return aBlocksB || bBlocksA;
}

/**
 * Get list of user IDs that the given user has blocked
 */
async function getBlockedUserIds(userId) {
  if (!BLOCKS_TABLE || !userId) return [];
  try {
    const result = await ddb.send(new QueryCommand({
      TableName: BLOCKS_TABLE,
      KeyConditionExpression: 'pk = :pk',
      ExpressionAttributeValues: { ':pk': `USER#${userId}` },
      ConsistentRead: true,
    }));
    return (result.Items || []).map(item => item.blockedUserId).filter(Boolean);
  } catch (e) {
    console.error('[Blocking] getBlockedUserIds failed:', e);
    return [];
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

// NEW: Send push notification via Expo Push Service
async function sendPushNotification(userId, title, body, data = {}) {
  if (!PUSH_TOKENS_TABLE || !userId) return;

  try {
    // Get all push tokens for this user
    const tokensQuery = await ddb.send(new QueryCommand({
      TableName: PUSH_TOKENS_TABLE,
      KeyConditionExpression: 'pk = :p',
      ExpressionAttributeValues: { ':p': `USER#${userId}` },
      ConsistentRead: true,
    }));

    const tokens = (tokensQuery.Items || []).map(item => item.token).filter(Boolean);

    if (tokens.length === 0) {
      console.log(`[Push] No tokens found for user ${userId}`);
      return;
    }

    // Send push notification to each token via Expo Push Service
    const messages = tokens.map(token => ({
      to: token,
      sound: 'default',
      title: title,
      body: body,
      data: data,
      priority: 'high',
      channelId: 'default',
    }));

    // Send to Expo Push Service
    const response = await fetch('https://exp.host/--/api/v2/push/send', {
      method: 'POST',
      headers: {
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(messages),
    });

    const result = await response.json();
    console.log(`[Push] Sent to ${tokens.length} device(s) for user ${userId}:`, result);

    // Handle receipts (optional - for production you'd want to track and handle errors)
    if (result.data) {
      for (let i = 0; i < result.data.length; i++) {
        const receipt = result.data[i];
        if (receipt.status === 'error') {
          console.error(`[Push] Error sending to token ${tokens[i]}:`, receipt.message);
          // In production, you might want to delete invalid tokens here
        }
      }
    }
  } catch (err) {
    console.error('[Push] Failed to send push notification:', err);
  }
}

// NEW: Generate or retrieve a unique invite code for a user
async function generateUserInviteCode(userId) {
  if (!INVITES_TABLE || !userId) return null;

  // First, check if user already has an invite code by querying with GSI
  try {
    const existing = await ddb.send(new QueryCommand({
      TableName: INVITES_TABLE,
      IndexName: 'byUserId',
      KeyConditionExpression: 'userId = :uid',
      ExpressionAttributeValues: { ':uid': userId },
      Limit: 1,
      // Note: GSIs do not support ConsistentRead - only eventually consistent
    }));

    if (existing.Items && existing.Items[0]) {
      console.log(`[Invites] Found existing code via GSI: ${existing.Items[0].code}`);
      return existing.Items[0].code;
    }
  } catch (e) {
    // If GSI doesn't exist yet, fall back to Scan with filter
    console.log('[Invites] GSI byUserId not available, trying Scan fallback:', e.message);

    try {
      const scanResult = await ddb.send(new ScanCommand({
        TableName: INVITES_TABLE,
        FilterExpression: 'userId = :uid',
        ExpressionAttributeValues: { ':uid': userId },
        Limit: 10, // Should only be 1, but allow a few in case of duplicates
        ConsistentRead: true,
      }));

      if (scanResult.Items && scanResult.Items.length > 0) {
        // Return the first (oldest) code for this user
        console.log(`[Invites] Found existing code via Scan: ${scanResult.Items[0].code}`);
        return scanResult.Items[0].code;
      }
    } catch (scanError) {
      console.error('[Invites] Scan fallback also failed:', scanError.message);
      // Continue to create new code
    }
  }

  // Generate a new code if none exists
  const code = crypto.randomUUID().slice(0, 8).toUpperCase();

  try {
    await ddb.send(new PutCommand({
      TableName: INVITES_TABLE,
      Item: {
        code,
        userId,
        usesRemaining: 10,
        createdAt: Date.now(),
      },
    }));

    console.log(`[Invites] Generated new invite code ${code} for user ${userId}`);
    return code;
  } catch (e) {
    console.error('[Invites] Failed to create user invite code:', e);
    return null;
  }
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

async function listPostsByUserId(targetId, limit = 1000) {
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
    imageAspectRatio: i.imageAspectRatio || null,
    images: i.images || null,
    avatarKey: i.avatarKey || null,
    spotifyEmbed: i.spotifyEmbed || null,
    location: i.location || null,
    poll: i.poll || null,
    createdAt: i.createdAt,
  }));
}

// ---- ConsistentRead helpers for follow state ----
// Uses denormalized counts on user record for O(1) lookup, with fallback to query
async function countFollowers(targetUserId) {
  if (!FOLLOWS_TABLE) return 0;
  // First try to get denormalized count from user record (O(1))
  try {
    const userResult = await ddb.send(new GetCommand({
      TableName: USERS_TABLE,
      Key: { pk: `USER#${targetUserId}` },
      ProjectionExpression: 'followerCount',
      ConsistentRead: true,
    }));
    if (userResult.Item && typeof userResult.Item.followerCount === 'number') {
      return userResult.Item.followerCount;
    }
  } catch (e) {
    console.warn('[countFollowers] Failed to get denormalized count, falling back to GSI query', e);
  }
  // Fallback: Use GSI on sk (target user) if available, otherwise use scan with limit
  // This is still better than full scan as it uses the GSI
  try {
    const r = await ddb.send(new QueryCommand({
      TableName: FOLLOWS_TABLE,
      IndexName: 'byFollowee', // GSI with sk (target user) as partition key
      KeyConditionExpression: 'sk = :t',
      ExpressionAttributeValues: { ':t': targetUserId },
      Select: 'COUNT',
    }));
    return r.Count || 0;
  } catch (e) {
    // GSI might not exist yet, fall back to limited scan
    console.warn('[countFollowers] GSI query failed, using limited scan', e);
    const r = await ddb.send(new ScanCommand({
      TableName: FOLLOWS_TABLE,
      FilterExpression: 'sk = :t',
      ExpressionAttributeValues: { ':t': targetUserId },
      Select: 'COUNT',
      Limit: 10000, // Cap the scan to prevent timeouts
    }));
    return r.Count || 0;
  }
}

async function countFollowing(userId) {
  if (!FOLLOWS_TABLE) return 0;
  // First try to get denormalized count from user record (O(1))
  try {
    const userResult = await ddb.send(new GetCommand({
      TableName: USERS_TABLE,
      Key: { pk: `USER#${userId}` },
      ProjectionExpression: 'followingCount',
      ConsistentRead: true,
    }));
    if (userResult.Item && typeof userResult.Item.followingCount === 'number') {
      return userResult.Item.followingCount;
    }
  } catch (e) {
    console.warn('[countFollowing] Failed to get denormalized count, falling back to query', e);
  }
  // Fallback: Query by pk (efficient - uses partition key)
  const r = await ddb.send(new QueryCommand({
    TableName: FOLLOWS_TABLE,
    KeyConditionExpression: 'pk = :me',
    ExpressionAttributeValues: { ':me': userId },
    Select: 'COUNT',
    ConsistentRead: true,
  }));
  return r.Count || 0;
}

// Batch check if viewer follows multiple users - prevents N+1 queries
async function batchCheckFollowing(viewerId, targetUserIds) {
  if (!FOLLOWS_TABLE || !viewerId || !targetUserIds || targetUserIds.length === 0) {
    return new Set();
  }
  const followingSet = new Set();
  // DynamoDB BatchGetItem limit is 100 items
  const chunks = [];
  for (let i = 0; i < targetUserIds.length; i += 100) {
    chunks.push(targetUserIds.slice(i, i + 100));
  }
  for (const chunk of chunks) {
    try {
      const resp = await ddb.send(new BatchGetCommand({
        RequestItems: {
          [FOLLOWS_TABLE]: {
            Keys: chunk.map(targetId => ({ pk: viewerId, sk: targetId })),
            ProjectionExpression: 'sk',
          }
        }
      }));
      const items = resp.Responses?.[FOLLOWS_TABLE] || [];
      for (const item of items) {
        if (item.sk) followingSet.add(item.sk);
      }
    } catch (e) {
      console.error('[batchCheckFollowing] Batch get failed for chunk', e);
    }
  }
  return followingSet;
}

// Update denormalized follower/following counts on user records
async function updateFollowCounts(followerId, targetId, delta) {
  const updates = [];
  // Update follower's followingCount
  updates.push(ddb.send(new UpdateCommand({
    TableName: USERS_TABLE,
    Key: { pk: `USER#${followerId}` },
    UpdateExpression: 'SET followingCount = if_not_exists(followingCount, :zero) + :delta',
    ExpressionAttributeValues: { ':delta': delta, ':zero': 0 },
  })).catch(e => console.error('[updateFollowCounts] Failed to update followingCount', e)));
  // Update target's followerCount
  updates.push(ddb.send(new UpdateCommand({
    TableName: USERS_TABLE,
    Key: { pk: `USER#${targetId}` },
    UpdateExpression: 'SET followerCount = if_not_exists(followerCount, :zero) + :delta',
    ExpressionAttributeValues: { ':delta': delta, ':zero': 0 },
  })).catch(e => console.error('[updateFollowCounts] Failed to update followerCount', e)));
  await Promise.all(updates);
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

  // Fallback per-item (LIMITED to prevent excessive queries)
  if (out.length === 0 && unique.length > 0) {
    console.warn(`[fetchUserSummaries] BatchGet returned 0 results for ${unique.length} users - using limited fallback`);
    // IMPORTANT: Limit fallback to prevent timeout on large reaction lists
    const limit = Math.min(unique.length, 5);
    for (let i = 0; i < limit; i++) {
      const id = unique[i];
      try {
        const r = await ddb.send(new GetCommand({
          TableName: USERS_TABLE,
          Key: { pk: `USER#${id}` },
          ProjectionExpression: 'pk, handle, userId, avatarKey, fullName',
          ConsistentRead: false, // Use eventually consistent for speed
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
      } catch (err) {
        console.error(`[fetchUserSummaries] Fallback GetCommand failed for user ${id}:`, err);
        // Continue to next user instead of failing entirely
      }
    }
    if (unique.length > limit) {
      console.warn(`[fetchUserSummaries] Truncated fallback from ${unique.length} to ${limit} users to prevent timeout`);
    }
  }
  return out;
}

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

// ---------- Poll Helpers ----------
/**
 * Hydrate a post's poll data with live vote counts and the current user's vote.
 * Returns null if the post has no poll or POLLS_TABLE is not configured.
 */
async function hydratePoll(post, userId) {
  if (!post.poll || !POLLS_TABLE) return null;

  try {
    // Fetch all items for this poll (options + votes)
    const qr = await ddb.send(new QueryCommand({
      TableName: POLLS_TABLE,
      KeyConditionExpression: 'pk = :p',
      ExpressionAttributeValues: { ':p': `POST#${post.id}` },
      ConsistentRead: false,
    }));

    const items = qr.Items || [];
    const optionCounts = {};
    let userVotedOptionId = null;
    let totalVotes = 0;

    for (const item of items) {
      if (item.sk.startsWith('OPTION#')) {
        const optId = item.optionId;
        optionCounts[optId] = Number(item.voteCount || 0);
        totalVotes += Number(item.voteCount || 0);
      } else if (userId && item.sk === `VOTE#${userId}`) {
        userVotedOptionId = item.optionId || null;
      }
    }

    const poll = {
      question: post.poll.question,
      options: post.poll.options.map(o => ({ id: o.id, text: o.text })),
      totalVotes,
      userVotedOptionId,
    };

    // If the user has voted, include results
    if (userVotedOptionId) {
      poll.results = post.poll.options.map(o => ({
        id: o.id,
        text: o.text,
        voteCount: optionCounts[o.id] || 0,
        percentage: totalVotes > 0 ? Math.round(((optionCounts[o.id] || 0) / totalVotes) * 100) : 0,
      }));
    }

    return poll;
  } catch (e) {
    console.error('[hydratePoll] Failed:', e);
    // Return basic poll without live data
    return {
      question: post.poll.question,
      options: post.poll.options.map(o => ({ id: o.id, text: o.text })),
      totalVotes: post.poll.totalVotes || 0,
      userVotedOptionId: null,
    };
  }
}

// ---------- Handler ----------
module.exports.handler = async (event) => {
  __event = event; // capture for CORS headers everywhere

  // Always return 200 for preflight with CORS headers
  if ((event?.requestContext?.http?.method || event?.httpMethod) === 'OPTIONS') {
    return ok({});
  }

  const { method, rawPath, stage, path, route } = normalizePath(event);
  // Always log DELETE requests for debugging
  if (method === 'DELETE') {
    console.log('===== DELETE REQUEST =====', { method, rawPath, stage, normalized: path, route });
  } else {
    console.log('ROUTE', { method, rawPath, stage, normalized: path, route });
  }

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

    // NEW: Register push token
    if (route === 'POST /push/register') {
      if (!userId) return bad('Unauthorized', 401);
      if (!PUSH_TOKENS_TABLE) return bad('Push notifications not enabled', 501);

      const body = JSON.parse(event.body || '{}');
      const token = String(body.token || '').trim();
      const platform = String(body.platform || 'ios').toLowerCase();

      if (!token) return bad('Token required', 400);

      // Create a hash of the token to use as sort key (for deduplication)
      const tokenHash = crypto.createHash('sha256').update(token).digest('hex').slice(0, 16);

      try {
        await ddb.send(new PutCommand({
          TableName: PUSH_TOKENS_TABLE,
          Item: {
            pk: `USER#${userId}`,
            sk: `TOKEN#${platform}#${tokenHash}`,
            token: token,
            platform: platform,
            registeredAt: Date.now(),
            lastUsedAt: Date.now(),
          },
        }));

        console.log(`[Push] Registered token for user ${userId} on ${platform}`);
        return ok({ success: true, registered: true });
      } catch (err) {
        console.error('[Push] Failed to register token:', err);
        return bad('Failed to register push token', 500);
      }
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


    // (A) Return current profile (add avatarKey + fullName + inviteCode so UI can show it)
    if (route === 'GET /me') {
      if (!userId) return ok({ message: 'Unauthorized' }, 401);
      const r = await ddb.send(new GetCommand({
        TableName: USERS_TABLE,
        Key: { pk: `USER#${userId}` },
        ConsistentRead: true,
      }));

      // Get invite code from user record, or generate if not present
      let inviteCode = r.Item?.inviteCode ?? null;
      if (!inviteCode) {
        try {
          inviteCode = await generateUserInviteCode(userId);
          // Store the invite code in the user record for future requests
          if (inviteCode) {
            await ddb.send(new UpdateCommand({
              TableName: USERS_TABLE,
              Key: { pk: `USER#${userId}` },
              UpdateExpression: 'SET inviteCode = :code',
              ExpressionAttributeValues: { ':code': inviteCode },
            }));
          }
        } catch (e) {
          console.error('[Invites] Failed to get invite code for user:', e);
        }
      }

      return ok({
        userId,
        handle: r.Item?.handle ?? null,
        email,
        avatarKey: r.Item?.avatarKey ?? null,
        fullName: r.Item?.fullName ?? null,
        termsAccepted: r.Item?.termsAccepted ?? false,
        inviteCode, // Include invite code in response
      });
    }

    // GET /me/notification-preferences - Get user's notification preferences
    if (route === 'GET /me/notification-preferences') {
      if (!userId) return bad('Unauthorized', 401);

      const prefs = await getUserNotificationPreferences(userId);
      return ok(prefs);
    }

    // PATCH /me/notification-preferences - Update user's notification preferences
    if (route === 'PATCH /me/notification-preferences') {
      if (!userId) return bad('Unauthorized', 401);

      const body = JSON.parse(event.body || '{}');

      // Validate preferences
      const validKeys = ['mentions', 'comments', 'reactions'];
      const updates = {};

      for (const key of validKeys) {
        if (key in body && typeof body[key] === 'boolean') {
          updates[key] = body[key];
        }
      }

      if (Object.keys(updates).length === 0) {
        return bad('No valid preferences provided', 400);
      }

      // Get current preferences
      const currentPrefs = await getUserNotificationPreferences(userId);

      // Merge with updates
      const newPrefs = { ...currentPrefs, ...updates };

      // Update in database
      await ddb.send(new UpdateCommand({
        TableName: USERS_TABLE,
        Key: { pk: `USER#${userId}` },
        UpdateExpression: 'SET notificationPreferences = :prefs',
        ExpressionAttributeValues: {
          ':prefs': newPrefs,
        },
      }));

      return ok(newPrefs);
    }

    // POST /me/accept-terms - Accept terms of service
    if (route === 'POST /me/accept-terms') {
      if (!userId) return bad('Unauthorized', 401);

      await ddb.send(new UpdateCommand({
        TableName: USERS_TABLE,
        Key: { pk: `USER#${userId}` },
        UpdateExpression: 'SET termsAccepted = :true, termsAcceptedAt = :now',
        ExpressionAttributeValues: {
          ':true': true,
          ':now': Date.now(),
        },
      }));

      return ok({ success: true, termsAccepted: true });
    }

    // NEW: GET /me/invite - Get user's invite code
    if (route === 'GET /me/invite') {
      if (!userId) return bad('Unauthorized', 401);
      if (!INVITES_TABLE) return bad('Invites not enabled', 501);

      const inviteCode = await generateUserInviteCode(userId);

      if (!inviteCode) {
        return bad('Failed to generate invite code', 500);
      }

      // Get remaining uses
      const invite = await ddb.send(new GetCommand({
        TableName: INVITES_TABLE,
        Key: { code: inviteCode },
        ConsistentRead: true,
      }));

      return ok({
        code: inviteCode,
        usesRemaining: invite.Item?.usesRemaining ?? 10,
        inviteCode, // Include both formats for frontend compatibility
      });
    }

    // NEW: POST /me/invite - Create/regenerate invite code
    if (route === 'POST /me/invite') {
      if (!userId) return bad('Unauthorized', 401);
      if (!INVITES_TABLE) return bad('Invites not enabled', 501);

      const body = JSON.parse(event.body || '{}');
      const uses = Math.max(1, Math.min(100, Number(body.uses || 10)));

      const inviteCode = await generateUserInviteCode(userId);

      if (!inviteCode) {
        return bad('Failed to generate invite code', 500);
      }

      return ok({
        code: inviteCode,
        uses,
        inviteCode, // Include both formats for frontend compatibility
      });
    }

    // NEW: GET /me/invites - List all user's invite codes
    if (route === 'GET /me/invites') {
      if (!userId) return bad('Unauthorized', 401);
      if (!INVITES_TABLE) return bad('Invites not enabled', 501);

      try {
        const result = await ddb.send(new QueryCommand({
          TableName: INVITES_TABLE,
          IndexName: 'byUserId',
          KeyConditionExpression: 'userId = :uid',
          ExpressionAttributeValues: { ':uid': userId },
          // Note: GSIs do not support ConsistentRead - only eventually consistent
        }));

        const items = (result.Items || []).map(it => ({
          code: it.code,
          usesRemaining: it.usesRemaining ?? 0,
          createdAt: it.createdAt ?? null,
        }));

        // If no invites exist, generate one
        if (items.length === 0) {
          const code = await generateUserInviteCode(userId);
          if (code) {
            return ok({
              items: [{
                code,
                usesRemaining: 10,
                createdAt: Date.now(),
              }],
              inviteCode: code, // Include for frontend compatibility
            });
          }
        }

        return ok({
          items,
          inviteCode: items[0]?.code ?? null, // Return first code for frontend compatibility
        });
      } catch (e) {
        console.error('[Invites] Failed to list user invites:', e);
        // Fallback: try to generate a new one
        const code = await generateUserInviteCode(userId);
        if (code) {
          return ok({
            items: [{ code, usesRemaining: 10, createdAt: Date.now() }],
            inviteCode: code,
          });
        }
        return bad('Failed to retrieve invites', 500);
      }
    }

    // ----- GET /me/tagged-posts -----
    // Returns posts where the current user is tagged in a photo or mentioned in text.
    // Uses the NOTIFICATIONS_TABLE to find tagged/mentioned posts (O(n) on user's
    // notifications instead of loading every post in the app).
    if (route === 'GET /me/tagged-posts') {
      if (!userId) return bad('Unauthorized', 401);

      const qs = event?.queryStringParameters || {};
      const limit = Math.min(parseInt(qs.limit) || 20, 50);
      const offset = Math.max(parseInt(qs.offset) || 0, 0);

      try {
        // Query this user's mention and photo_tag notifications to find relevant post IDs.
        // This replaces loading ALL posts and filtering in memory.
        const notifTypes = ['mention', 'photo_tag'];
        const allNotifs = [];
        let lastKey = undefined;
        do {
          const notifResult = await ddb.send(new QueryCommand({
            TableName: NOTIFICATIONS_TABLE,
            KeyConditionExpression: 'pk = :pk',
            ExpressionAttributeValues: { ':pk': `USER#${userId}` },
            ScanIndexForward: false, // newest first
            ...(lastKey ? { ExclusiveStartKey: lastKey } : {}),
          }));
          for (const n of (notifResult.Items || [])) {
            if (notifTypes.includes(n.type) && n.postId) {
              allNotifs.push(n);
            }
          }
          lastKey = notifResult.LastEvaluatedKey;
        } while (lastKey);

        // Deduplicate by postId (a post can have both a mention and a tag)
        const seenPostIds = new Set();
        const uniqueNotifs = [];
        for (const n of allNotifs) {
          if (!seenPostIds.has(n.postId)) {
            seenPostIds.add(n.postId);
            uniqueNotifs.push(n);
          }
        }

        const total = uniqueNotifs.length;

        // Apply pagination before fetching post details
        const paginatedNotifs = uniqueNotifs.slice(offset, offset + limit);
        if (paginatedNotifs.length === 0) {
          return ok({ items: [], total });
        }

        // Batch-fetch the actual posts using the byId GSI
        const postIds = paginatedNotifs.map(n => n.postId);
        const posts = [];
        for (const postId of postIds) {
          try {
            const pq = await ddb.send(new QueryCommand({
              TableName: POSTS_TABLE,
              IndexName: 'byId',
              KeyConditionExpression: 'id = :id',
              ExpressionAttributeValues: { ':id': postId },
              Limit: 1,
            }));
            if (pq.Items && pq.Items[0]) posts.push(pq.Items[0]);
          } catch (e) {
            console.error(`[tagged-posts] Failed to fetch post ${postId}:`, e);
          }
        }

        // Hydrate with fresh user data
        const authorIds = [...new Set(posts.map(p => p.userId).filter(Boolean))];
        let profileMap = {};
        try {
          const summaries = await fetchUserSummaries(authorIds);
          for (const s of summaries) {
            profileMap[s.userId || s.id] = s;
          }
        } catch (e) { console.error('[tagged-posts] hydrate failed', e); }

        const items = posts.map(p => {
          const profile = profileMap[p.userId] || {};
          return {
            id: p.id,
            userId: p.userId,
            username: p.username || 'unknown',
            handle: profile.handle || p.handle || null,
            text: p.text || '',
            imageKey: p.imageKey || null,
            imageAspectRatio: p.imageAspectRatio || null,
            images: p.images || null,
            avatarKey: profile.avatarKey ?? p.avatarKey ?? null,
            spotifyEmbed: p.spotifyEmbed || null,
            location: p.location || null,
            createdAt: p.createdAt,
          };
        });

        // Hydrate with comment previews (matches feed endpoint behavior)
        if (COMMENTS_TABLE && items.length > 0) {
          try {
            const commentResults = await Promise.all(items.map(post =>
              ddb.send(new QueryCommand({
                TableName: COMMENTS_TABLE,
                KeyConditionExpression: 'pk = :p',
                ExpressionAttributeValues: { ':p': `POST#${post.id}` },
                ScanIndexForward: true,
                Limit: 4,
                ConsistentRead: false,
              })).then(r => ({
                postId: post.id,
                items: r.Items || [],
                count: r.Count || (r.Items || []).length,
              })).catch(e => {
                console.error(`[tagged-posts] Failed to fetch comments for post ${post.id}:`, e);
                return { postId: post.id, items: [], count: 0 };
              })
            ));

            const commentMap = {};
            const allCommentUserIds = new Set();
            for (const result of commentResults) {
              const comments = result.items.slice(0, 3).map(it => ({
                id: it.id,
                userId: it.userId,
                handle: it.userHandle || null,
                text: it.text || '',
                createdAt: it.createdAt || 0,
              }));
              commentMap[result.postId] = { comments, count: result.count };
              comments.forEach(c => { if (c.userId) allCommentUserIds.add(c.userId); });
            }

            const commentAuthorUserIds = Array.from(allCommentUserIds);
            let commentAuthorAvatarMap = {};
            if (commentAuthorUserIds.length > 0) {
              try {
                const summaries = await fetchUserSummaries(commentAuthorUserIds);
                commentAuthorAvatarMap = Object.fromEntries(
                  summaries.map(u => [u.userId, u.avatarKey || null])
                );
              } catch (e) {
                console.error('[tagged-posts] Failed to fetch comment avatars:', e);
              }
            }

            for (const post of items) {
              const data = commentMap[post.id] || { comments: [], count: 0 };
              for (const comment of data.comments) {
                if (comment.userId && commentAuthorAvatarMap[comment.userId]) {
                  comment.avatarKey = commentAuthorAvatarMap[comment.userId];
                }
              }
              post.comments = data.comments;
              post.commentCount = data.count;
            }
          } catch (e) {
            console.error('[tagged-posts] Failed to fetch comment previews:', e);
          }
        }

        return ok({ items, total });
      } catch (e) {
        console.error('[tagged-posts] error:', e);
        return bad('Failed to fetch tagged posts', 500);
      }
    }

    // (A2) Update fields on me (currently: fullName)
    if (route === 'PATCH /me') {
      if (!userId) return ok({ message: 'Unauthorized' }, 401);
      const body = JSON.parse(event.body || '{}');

      // Parse fullName
      const rawFullName = (body.fullName ?? '').toString().trim();
      const fullName = rawFullName ? rawFullName.slice(0, 80) : null;

      // Parse handle (from any format the client sends)
      const rawHandle = (body.handle || body.username || body.user_handle || body.user_name || '').toString().trim().toLowerCase();
      const handle = rawHandle ? rawHandle.slice(0, 20) : null;

      // Validate handle format if provided
      if (handle && !/^[a-z0-9_]{3,20}$/.test(handle)) {
        return bad('Handle must be 3-20 chars, letters/numbers/underscore', 400);
      }

      // Check if handle is taken (if trying to set a new one)
      if (handle) {
        const existing = await ddb.send(new GetCommand({
          TableName: USERS_TABLE,
          Key: { pk: `HANDLE#${handle}` },
          ConsistentRead: true,
        }));
        // Allow if it's their own handle or if it's not taken
        if (existing.Item && existing.Item.userId !== userId) {
          return bad('Handle already taken', 409);
        }
      }

      // Build UpdateExpression dynamically
      const updates = ['#uid = :u'];
      const names = { '#uid': 'userId' };
      const values = { ':u': userId };

      if ('fullName' in body) {
        updates.push('#fn = :fn');
        names['#fn'] = 'fullName';
        values[':fn'] = fullName;
      }

      if (handle) {
        updates.push('#h = :h');
        names['#h'] = 'handle';
        values[':h'] = handle;
      }

      // Update USER record
      await ddb.send(new UpdateCommand({
        TableName: USERS_TABLE,
        Key: { pk: `USER#${userId}` },
        UpdateExpression: `SET ${updates.join(', ')}`,
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
      }));

      // Update HANDLE mapping if handle changed
      if (handle) {
        await ddb.send(new PutCommand({
          TableName: USERS_TABLE,
          Item: {
            pk: `HANDLE#${handle}`,
            userId,
            type: 'HANDLE',
            handle,
          },
        }));
      }

      return ok({ ok: true, fullName, handle });
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
    // FIX: Use UpdateCommand instead of PutCommand to preserve other user fields
    if (route === 'POST /me/avatar') {
      if (!userId) return ok({ message: 'Unauthorized' }, 401);
      const { key } = JSON.parse(event.body || '{}');
      if (!key) return ok({ message: 'Missing key' }, 400);

      // FIX: Use UpdateCommand to only update avatarKey field, preserving fullName, handle, etc.
      await ddb.send(new UpdateCommand({
        TableName: USERS_TABLE,
        Key: { pk: `USER#${userId}` },
        UpdateExpression: 'SET avatarKey = :key, userId = :uid',
        ExpressionAttributeValues: { ':key': key, ':uid': userId },
      }));

      // Also update the HANDLE mapping if user has a handle
      const u = await ddb.send(new GetCommand({ TableName: USERS_TABLE, Key: { pk: `USER#${userId}` } }));
      if (u.Item?.handle) {
        await ddb.send(new UpdateCommand({
          TableName: USERS_TABLE,
          Key: { pk: `HANDLE#${u.Item.handle}` },
          UpdateExpression: 'SET avatarKey = :key, userId = :uid',
          ExpressionAttributeValues: { ':key': key, ':uid': userId },
        }));
      }
      return ok({ success: true, avatarKey: key });
    }

    // DELETE /me - Delete user account and all associated data
    if (route === 'DELETE /me') {
      console.log(`===== MATCHED DELETE /me ENDPOINT =====`);
      if (!userId) return bad('Unauthorized', 401);

      console.log(`[DELETE /me] Starting account deletion for user ${userId}`);

      try {
        // Get user's handle before deletion
        const userRecord = await ddb.send(new GetCommand({
          TableName: USERS_TABLE,
          Key: { pk: `USER#${userId}` },
          ConsistentRead: true,
        }));
        const userHandle = userRecord.Item?.handle || null;

        // ---- PHASE 1: Immediate operations (soft-delete) ----
        // These are fast, critical for preventing re-login and freeing the handle.
        // Mark user as deleted so all read paths can filter them out.
        await ddb.send(new UpdateCommand({
          TableName: USERS_TABLE,
          Key: { pk: `USER#${userId}` },
          UpdateExpression: 'SET deletedAt = :now, #st = :deleted',
          ExpressionAttributeNames: { '#st': 'status' },
          ExpressionAttributeValues: { ':now': Date.now(), ':deleted': 'DELETED' },
        }));

        // Release the handle immediately so it can be reused
        if (userHandle) {
          await ddb.send(new DeleteCommand({
            TableName: USERS_TABLE,
            Key: { pk: `HANDLE#${userHandle}` },
          }));
        }

        // Revoke authentication so deleted user can't make API calls
        try {
          if (USER_POOL_ID) {
            await cognito.send(new AdminDeleteUserCommand({
              UserPoolId: USER_POOL_ID,
              Username: userId,
            }));
            console.log(`[DELETE /me] Deleted user from Cognito`);
          }
        } catch (e) {
          console.error('[DELETE /me] Failed to delete user from Cognito:', e);
        }

        // ---- PHASE 2: Batch cleanup of user data ----
        // Uses BatchWriteCommand (25 items/batch) instead of individual DeleteCommands.
        // NOTE: For production at scale, move this to an SQS-triggered Lambda to avoid
        // API Gateway's 29-second timeout. The soft-delete above ensures correctness
        // even if cleanup is deferred.
        const batchDelete = async (tableName, keys) => {
          if (!tableName || keys.length === 0) return;
          const chunks = [];
          for (let i = 0; i < keys.length; i += 25) {
            chunks.push(keys.slice(i, i + 25));
          }
          for (const chunk of chunks) {
            try {
              await ddb.send(new BatchWriteCommand({
                RequestItems: {
                  [tableName]: chunk.map(key => ({ DeleteRequest: { Key: key } })),
                },
              }));
            } catch (e) {
              console.error(`[DELETE /me] BatchWrite failed for ${tableName}:`, e);
            }
          }
        };

        // 1. Delete all user's posts (query by partition key - no scan needed)
        try {
          const postsResult = await ddb.send(new QueryCommand({
            TableName: POSTS_TABLE,
            KeyConditionExpression: 'pk = :p',
            ExpressionAttributeValues: { ':p': `USER#${userId}` },
            ProjectionExpression: 'pk, sk',
          }));
          await batchDelete(POSTS_TABLE, (postsResult.Items || []).map(p => ({ pk: p.pk, sk: p.sk })));
          console.log(`[DELETE /me] Deleted ${(postsResult.Items || []).length} posts`);
        } catch (e) {
          console.error('[DELETE /me] Failed to delete posts:', e);
        }

        // 2. Delete user's comments (scan required - no userId partition key on COMMENTS_TABLE)
        try {
          if (COMMENTS_TABLE) {
            const commentsResult = await ddb.send(new ScanCommand({
              TableName: COMMENTS_TABLE,
              FilterExpression: 'userId = :uid',
              ExpressionAttributeValues: { ':uid': userId },
              ProjectionExpression: 'pk, sk',
            }));
            await batchDelete(COMMENTS_TABLE, (commentsResult.Items || []).map(c => ({ pk: c.pk, sk: c.sk })));
            console.log(`[DELETE /me] Deleted ${(commentsResult.Items || []).length} comments`);
          }
        } catch (e) {
          console.error('[DELETE /me] Failed to delete comments:', e);
        }

        // 3. Delete user's reactions
        try {
          if (REACTIONS_TABLE) {
            const reactionsResult = await ddb.send(new ScanCommand({
              TableName: REACTIONS_TABLE,
              FilterExpression: 'sk = :sk',
              ExpressionAttributeValues: { ':sk': `USER#${userId}` },
              ProjectionExpression: 'pk, sk, emoji',
            }));
            const reactions = reactionsResult.Items || [];

            // Decrement counters in parallel (best-effort)
            await Promise.all(reactions.filter(r => r.emoji).map(r =>
              ddb.send(new UpdateCommand({
                TableName: REACTIONS_TABLE,
                Key: { pk: r.pk, sk: `COUNT#${r.emoji}` },
                UpdateExpression: 'ADD #c :neg',
                ExpressionAttributeNames: { '#c': 'count' },
                ExpressionAttributeValues: { ':neg': -1 },
              })).catch(e => console.error('[DELETE /me] Failed to decrement reaction count:', e))
            ));

            await batchDelete(REACTIONS_TABLE, reactions.map(r => ({ pk: r.pk, sk: r.sk })));
            console.log(`[DELETE /me] Deleted ${reactions.length} reactions`);
          }
        } catch (e) {
          console.error('[DELETE /me] Failed to delete reactions:', e);
        }

        // 4. Delete follow relationships (both directions)
        try {
          if (FOLLOWS_TABLE) {
            // People this user follows (query by partition key)
            const followingResult = await ddb.send(new QueryCommand({
              TableName: FOLLOWS_TABLE,
              KeyConditionExpression: 'pk = :p',
              ExpressionAttributeValues: { ':p': userId },
              ProjectionExpression: 'pk, sk',
            }));
            await batchDelete(FOLLOWS_TABLE, (followingResult.Items || []).map(f => ({ pk: f.pk, sk: f.sk })));

            // People following this user (scan required - no GSI on sk)
            const followersResult = await ddb.send(new ScanCommand({
              TableName: FOLLOWS_TABLE,
              FilterExpression: 'sk = :sk',
              ExpressionAttributeValues: { ':sk': userId },
              ProjectionExpression: 'pk, sk',
            }));
            await batchDelete(FOLLOWS_TABLE, (followersResult.Items || []).map(f => ({ pk: f.pk, sk: f.sk })));
            console.log(`[DELETE /me] Deleted follow relationships`);
          }
        } catch (e) {
          console.error('[DELETE /me] Failed to delete follows:', e);
        }

        // 5. Delete notifications (received + sent)
        try {
          if (NOTIFICATIONS_TABLE) {
            // Notifications TO this user (query by partition key)
            const notifResult = await ddb.send(new QueryCommand({
              TableName: NOTIFICATIONS_TABLE,
              KeyConditionExpression: 'pk = :p',
              ExpressionAttributeValues: { ':p': `USER#${userId}` },
              ProjectionExpression: 'pk, sk',
            }));
            await batchDelete(NOTIFICATIONS_TABLE, (notifResult.Items || []).map(n => ({ pk: n.pk, sk: n.sk })));

            // Notifications FROM this user (scan required)
            const sentNotifResult = await ddb.send(new ScanCommand({
              TableName: NOTIFICATIONS_TABLE,
              FilterExpression: 'fromUserId = :uid',
              ExpressionAttributeValues: { ':uid': userId },
              ProjectionExpression: 'pk, sk',
            }));
            await batchDelete(NOTIFICATIONS_TABLE, (sentNotifResult.Items || []).map(n => ({ pk: n.pk, sk: n.sk })));
            console.log(`[DELETE /me] Deleted notifications`);
          }
        } catch (e) {
          console.error('[DELETE /me] Failed to delete notifications:', e);
        }

        // 6. Delete invites
        try {
          if (INVITES_TABLE) {
            let invites = [];
            try {
              const invitesResult = await ddb.send(new QueryCommand({
                TableName: INVITES_TABLE,
                IndexName: 'byUserId',
                KeyConditionExpression: 'userId = :uid',
                ExpressionAttributeValues: { ':uid': userId },
                ProjectionExpression: 'code',
              }));
              invites = invitesResult.Items || [];
            } catch (e) {
              console.log('[DELETE /me] GSI not available, using scan for invites');
              const invitesResult = await ddb.send(new ScanCommand({
                TableName: INVITES_TABLE,
                FilterExpression: 'userId = :uid',
                ExpressionAttributeValues: { ':uid': userId },
                ProjectionExpression: 'code',
              }));
              invites = invitesResult.Items || [];
            }
            await batchDelete(INVITES_TABLE, invites.map(i => ({ code: i.code })));
            console.log(`[DELETE /me] Deleted ${invites.length} invites`);
          }
        } catch (e) {
          console.error('[DELETE /me] Failed to delete invites:', e);
        }

        // 7. Delete user record (the soft-deleted marker)
        try {
          await ddb.send(new DeleteCommand({
            TableName: USERS_TABLE,
            Key: { pk: `USER#${userId}` },
          }));
          console.log(`[DELETE /me] Deleted user records`);
        } catch (e) {
          console.error('[DELETE /me] Failed to delete user records:', e);
        }

        console.log(`[DELETE /me] Account deletion completed for user ${userId}`);
        return ok({ success: true, message: 'Account deleted successfully' });

      } catch (err) {
        console.error('[DELETE /me] Error during account deletion:', err);
        return bad('Failed to delete account', 500);
      }
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

  const items = (r.Items || []).map(it => ({
    id: it.id,
    userId: it.userId,
    userHandle: it.userHandle || 'unknown',
    text: it.text || '',
    createdAt: it.createdAt || 0,
    parentCommentId: it.parentCommentId || null,
  }));

  // Fetch avatarKey for each comment author
  try {
    const userIds = [...new Set(items.map(it => it.userId).filter(Boolean))];
    if (userIds.length > 0) {
      const summaries = await fetchUserSummaries(userIds);
      const avatarMap = Object.fromEntries(
        summaries.map(u => [u.userId, u.avatarKey || null])
      );

      // Add avatarKey to each comment
      for (const item of items) {
        if (item.userId && avatarMap[item.userId]) {
          item.avatarKey = avatarMap[item.userId];
        }
      }
    }
  } catch (e) {
    console.error('Failed to fetch comment avatars:', e);
  }

  return ok({ items });
}

    // POST /posts/{id}/comments  body: { text, parentCommentId? }
    if (method === 'POST' && path.startsWith('/comments/')) {
      if (!COMMENTS_TABLE) return ok({ message: 'Comments not enabled' }, 501);
      if (!userId) return ok({ message: 'Unauthorized' }, 401);
      const postId = path.split('/')[2];
      const body = JSON.parse(event.body || '{}');
      const text = String(body.text || '').trim().slice(0, 500);
      const parentCommentId = body.parentCommentId || null;
      if (!text) return ok({ message: 'Text required' }, 400);

      // Verify the post exists and user has permission to comment
      let post = null;
      try {
        const postQuery = await ddb.send(new QueryCommand({
          TableName: POSTS_TABLE,
          IndexName: 'byId',
          KeyConditionExpression: 'id = :id',
          ExpressionAttributeValues: { ':id': postId },
          Limit: 1,
        }));
        post = (postQuery.Items || [])[0];
        if (!post) {
          return ok({ message: 'Post not found' }, 404);
        }

        // Check if user has permission to comment on this post
        // Allow if:
        // 1) user owns the post
        // 2) user follows the post author
        // 3) user is mentioned in the post
        // 4) user already has comments on this post (already participating)
        const isOwnPost = post.userId === userId;
        const followsAuthor = await isFollowing(userId, post.userId);

        // Check if user is mentioned in the post
        const postText = post.text || '';
        const handle = await getHandleForUserId(userId);
        const isMentioned = handle && postText.toLowerCase().includes(`@${handle.toLowerCase()}`);

        // Check if user already has comments on this post
        let isParticipating = false;
        if (!isOwnPost && !followsAuthor && !isMentioned) {
          const existingComments = await ddb.send(new QueryCommand({
            TableName: COMMENTS_TABLE,
            KeyConditionExpression: 'pk = :pk',
            FilterExpression: 'userId = :userId',
            ExpressionAttributeValues: {
              ':pk': `POST#${postId}`,
              ':userId': userId
            },
            Limit: 1
          }));
          isParticipating = (existingComments.Items || []).length > 0;
        }

        if (!isOwnPost && !followsAuthor && !isMentioned && !isParticipating) {
          return ok({ message: 'You must follow this user to comment on their posts' }, 403);
        }
      } catch (e) {
        console.error('Failed to verify post access:', e);
        return ok({ message: 'Failed to verify post access' }, 500);
      }

      // If replying to a comment, verify parent exists and get its author
      let parentComment = null;
      if (parentCommentId) {
        try {
          // Query all comments for this post and find the parent by id
          // Note: FilterExpression with Limit doesn't work as expected - it limits first, then filters
          const qr = await ddb.send(new QueryCommand({
            TableName: COMMENTS_TABLE,
            KeyConditionExpression: 'pk = :pk',
            ExpressionAttributeValues: {
              ':pk': `POST#${postId}`
            }
          }));
          const allComments = qr.Items || [];
          parentComment = allComments.find(c => c.id === parentCommentId);
          if (!parentComment) {
            return ok({ message: 'Parent comment not found' }, 404);
          }
        } catch (e) {
          console.error('Failed to fetch parent comment:', e);
          return ok({ message: 'Failed to verify parent comment' }, 500);
        }
      }

      // Moderate comment content using Amazon Bedrock
      const moderation = await moderateContent(text);
      if (!moderation.safe) {
        console.log(`[Moderation] Comment blocked for user ${userId}: ${moderation.reason}`);
        return bad(`Content blocked: ${moderation.reason || 'Content violates our community guidelines'}`, 403);
      }

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

      // Add parentCommentId if this is a reply
      if (parentCommentId) {
        item.parentCommentId = parentCommentId;
      }

      await ddb.send(new PutCommand({ TableName: COMMENTS_TABLE, Item: item }));

      // NEW: notify parent comment author if this is a reply, otherwise notify post owner
      if (parentComment && parentComment.userId && parentComment.userId !== userId) {
        try {
          await createNotification(parentComment.userId, 'reply', userId, postId, 'replied to your comment');
        } catch (e) { console.error('notify parent comment author failed', e); }
      } else if (!parentCommentId) {
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
      }

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

      const response = { id, userHandle: handle, text, createdAt: now };
      if (parentCommentId) {
        response.parentCommentId = parentCommentId;
      }
      return ok(response);
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

      // First, cascade-delete all replies to this comment
      try {
        const repliesQr = await ddb.send(new QueryCommand({
          TableName: COMMENTS_TABLE,
          KeyConditionExpression: 'pk = :p',
          FilterExpression: 'parentCommentId = :parentId',
          ExpressionAttributeValues: {
            ':p': `POST#${postId}`,
            ':parentId': id
          },
          Limit: 100
        }));

        const replies = repliesQr.Items || [];
        for (const reply of replies) {
          // Delete each reply
          await ddb.send(new DeleteCommand({
            TableName: COMMENTS_TABLE,
            Key: { pk: reply.pk, sk: reply.sk }
          }));

          // Delete notifications from each reply
          try {
            const replyText = String(reply.text || '');
            const mentionRegex = /@([a-z0-9_]+)/gi;
            const mentions = [...replyText.matchAll(mentionRegex)].map(m => m[1].toLowerCase());
            for (const h of mentions) {
              const mid = await userIdFromHandle(h);
              if (mid && mid !== reply.userId) {
                await deleteNotifications(mid, 'mention', reply.userId, postId);
              }
            }
          } catch (e) { console.error('cleanup reply mention notifs failed', e); }
        }
      } catch (e) { console.error('cascade delete replies failed', e); }

      // Delete the comment itself
      await ddb.send(new DeleteCommand({
        TableName: COMMENTS_TABLE,
        Key: { pk: item.pk, sk: item.sk },
        ConditionExpression: 'attribute_exists(pk) AND attribute_exists(sk)'
      }));

      // Cascade-delete notifications created by this comment
      try {
        // If this is a reply, remove the 'reply' notification to parent comment author
        if (item.parentCommentId) {
          try {
            // Find parent comment to get its author
            const parentQr = await ddb.send(new QueryCommand({
              TableName: COMMENTS_TABLE,
              KeyConditionExpression: 'pk = :p',
              FilterExpression: 'id = :id',
              ExpressionAttributeValues: {
                ':p': `POST#${postId}`,
                ':id': item.parentCommentId
              },
              Limit: 1
            }));
            const parentComment = (parentQr.Items || [])[0];
            if (parentComment && parentComment.userId && parentComment.userId !== userId) {
              await deleteNotifications(parentComment.userId, 'reply', userId, postId);
            }
          } catch (e) { console.error('cleanup reply notif failed', e); }
        } else {
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
        }

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
      const startTime = Date.now();

      try {
        const qr = await ddb.send(new QueryCommand({
          TableName: REACTIONS_TABLE,
          KeyConditionExpression: 'pk = :p',
          ExpressionAttributeValues: { ':p': `POST#${postId}` },
          ProjectionExpression: 'sk, #c, emoji',
          ExpressionAttributeNames: { '#c': 'count' },
          ConsistentRead: false, // Use eventually consistent for better performance
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

        // Fallback query for current user's reaction if not found
        if (userId && mine === null) {
          try {
            const ur = await ddb.send(new GetCommand({
              TableName: REACTIONS_TABLE,
              Key: { pk: `POST#${postId}`, sk: `USER#${userId}` },
              ProjectionExpression: 'emoji',
              ConsistentRead: false, // Use eventually consistent for better performance
            }));
            mine = ur.Item?.emoji || null;
          } catch (err) {
            console.error('[GET /reactions] Fallback user reaction query failed:', err);
            // Continue with mine=null instead of failing entire request
          }
        }

        const wantWho = (event?.queryStringParameters?.who === '1');
        let who = undefined;
        if (wantWho) {
          try {
            const uqr = await ddb.send(new QueryCommand({
              TableName: REACTIONS_TABLE,
              KeyConditionExpression: 'pk = :p AND begins_with(sk, :u)',
              ExpressionAttributeValues: { ':p': `POST#${postId}`, ':u': 'USER#' },
              ProjectionExpression: 'sk, emoji',
              ConsistentRead: false, // Use eventually consistent for better performance
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

            // Graceful degradation: if user profile fetch fails, return reactions without user details
            try {
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
            } catch (err) {
              console.error('[GET /reactions] Failed to fetch user summaries:', err);
              // Return reaction data without user profiles instead of failing
              who = {};
              for (const [e, ids] of Object.entries(byEmoji)) {
                who[e] = ids.map(uid => ({ userId: uid, handle: null, avatarKey: null }));
              }
            }
          } catch (err) {
            console.error('[GET /reactions] Who query failed:', err);
            // Return without "who" data instead of failing entire request
            who = undefined;
          }
        }

        const duration = Date.now() - startTime;
        console.log(`[GET /reactions/${postId}] Completed in ${duration}ms`);
        return ok({ counts, my: mine ? [mine] : [], ...(wantWho ? { who } : {}) });

      } catch (err) {
        console.error('[GET /reactions] Main query failed:', err);
        // Return empty reactions instead of 500 error
        return ok({ counts: {}, my: [], message: 'Failed to fetch reactions' });
      }
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
        // User is REMOVING their reaction - decrement count and delete
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
        // NOTE: No notification sent for reaction removal
      } else {
        // User is ADDING a new reaction or CHANGING to a different emoji
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

        // Notify post owner about NEW reaction (only when adding, not removing)
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
      }

      // Return the caller's current reaction so the UI can refresh accurately
      const self = await ddb.send(new GetCommand({
        TableName: REACTIONS_TABLE,
        Key: { pk: `POST#${postId}`, sk: `USER#${userId}` },
        ProjectionExpression: 'emoji',
        ConsistentRead: true
      }));
      return ok({ ok: true, my: self.Item?.emoji ? [self.Item.emoji] : [] });
    }

    // ===== Polls API =====

    // GET /polls/{postId} - Get poll results, or ?voters={optionId} to get voters (author only)
    if (method === 'GET' && path.startsWith('/polls/')) {
      const postId = path.split('/')[2];
      if (!postId) return bad('Missing postId', 400);

      const optionId = event?.queryStringParameters?.voters;

      // GET /polls/{postId}?voters={optionId} - return voters for a specific option (author only)
      if (optionId) {
        if (!userId) return bad('Unauthorized', 401);
        if (!POLLS_TABLE) return bad('Polls not configured', 501);

        try {
          const qr = await ddb.send(new QueryCommand({
            TableName: POSTS_TABLE,
            IndexName: 'byId',
            KeyConditionExpression: 'id = :id',
            ExpressionAttributeValues: { ':id': postId },
            Limit: 1,
          }));
          const post = (qr.Items || [])[0];
          if (!post || !post.poll) return bad('Poll not found', 404);
          if (post.userId !== userId) return bad('Forbidden', 403);

          const validOption = post.poll.options.find(o => o.id === optionId);
          if (!validOption) return bad('Option not found', 404);

          const voteRows = [];
          let lastKey;
          do {
            const resp = await ddb.send(new QueryCommand({
              TableName: POLLS_TABLE,
              KeyConditionExpression: 'pk = :pk AND begins_with(sk, :votePrefix)',
              ExpressionAttributeValues: {
                ':pk': `POST#${postId}`,
                ':votePrefix': 'VOTE#',
              },
              ExclusiveStartKey: lastKey,
            }));
            for (const item of (resp.Items || [])) {
              if (item.optionId === optionId) voteRows.push(item);
            }
            lastKey = resp.LastEvaluatedKey;
          } while (lastKey);

          const voterIds = voteRows.map(v => v.sk.replace('VOTE#', ''));
          const summaries = await fetchUserSummaries(voterIds);
          const summaryMap = Object.fromEntries(summaries.map(s => [s.userId, s]));
          const voters = voterIds.map(id => ({
            userId: id,
            handle: summaryMap[id]?.handle || null,
            avatarKey: summaryMap[id]?.avatarKey || null,
          }));

          return ok({ voters });
        } catch (e) {
          console.error('[GET /polls?voters] Failed:', e);
          return bad('Failed to load voters', 500);
        }
      }

      // Look up the post to get poll metadata
      try {
        const qr = await ddb.send(new QueryCommand({
          TableName: POSTS_TABLE,
          IndexName: 'byId',
          KeyConditionExpression: 'id = :id',
          ExpressionAttributeValues: { ':id': postId },
          Limit: 1,
        }));
        const post = (qr.Items || [])[0];
        if (!post || !post.poll) return bad('Poll not found', 404);

        // If POLLS_TABLE exists, hydrate with live vote data; otherwise return static poll
        const poll = await hydratePoll(post, userId) || {
          question: post.poll.question,
          options: post.poll.options.map(o => ({ id: o.id, text: o.text })),
          totalVotes: post.poll.totalVotes || 0,
          userVotedOptionId: null,
        };
        return ok({ poll });
      } catch (e) {
        console.error('[GET /polls] Failed:', e);
        return bad('Failed to fetch poll', 500);
      }
    }

    // POST /polls/{postId}/vote - Vote on a poll
    if (method === 'POST' && path.match(/^\/polls\/[^\/]+\/vote$/)) {
      if (!POLLS_TABLE) return bad('Voting is not yet configured. Please add the POLLS_TABLE environment variable to your Lambda.', 501);
      if (!userId) return bad('Unauthorized', 401);

      const postId = path.split('/')[2];
      const body = JSON.parse(event.body || '{}');
      const optionId = String(body.optionId || '').trim();
      if (!optionId) return bad('Missing optionId', 400);

      try {
        // Look up the post to validate the poll and option
        const qr = await ddb.send(new QueryCommand({
          TableName: POSTS_TABLE,
          IndexName: 'byId',
          KeyConditionExpression: 'id = :id',
          ExpressionAttributeValues: { ':id': postId },
          Limit: 1,
        }));
        const post = (qr.Items || [])[0];
        if (!post || !post.poll) return bad('Poll not found', 404);

        const validOption = post.poll.options.find(o => o.id === optionId);
        if (!validOption) return bad('Invalid option', 400);

        // Check if user already voted
        const existingVote = await ddb.send(new GetCommand({
          TableName: POLLS_TABLE,
          Key: { pk: `POST#${postId}`, sk: `VOTE#${userId}` },
          ConsistentRead: true,
        }));

        if (existingVote.Item) {
          const oldOptionId = existingVote.Item.optionId;
          if (oldOptionId === optionId) {
            // Same option — just return current poll state
            const poll = await hydratePoll(post, userId);
            return ok({ poll });
          }

          // Change vote: update vote record, decrement old, increment new
          await ddb.send(new PutCommand({
            TableName: POLLS_TABLE,
            Item: {
              pk: `POST#${postId}`,
              sk: `VOTE#${userId}`,
              optionId,
              votedAt: Date.now(),
            },
          }));

          // Decrement old option
          await ddb.send(new UpdateCommand({
            TableName: POLLS_TABLE,
            Key: { pk: `POST#${postId}`, sk: `OPTION#${oldOptionId}` },
            UpdateExpression: 'ADD voteCount :neg',
            ExpressionAttributeValues: { ':neg': -1 },
          }));

          // Increment new option
          await ddb.send(new UpdateCommand({
            TableName: POLLS_TABLE,
            Key: { pk: `POST#${postId}`, sk: `OPTION#${optionId}` },
            UpdateExpression: 'ADD voteCount :one',
            ExpressionAttributeValues: { ':one': 1 },
          }));
        } else {
          // First vote
          await ddb.send(new PutCommand({
            TableName: POLLS_TABLE,
            Item: {
              pk: `POST#${postId}`,
              sk: `VOTE#${userId}`,
              optionId,
              votedAt: Date.now(),
            },
            ConditionExpression: 'attribute_not_exists(pk)',
          }));

          // Increment the option vote count
          await ddb.send(new UpdateCommand({
            TableName: POLLS_TABLE,
            Key: { pk: `POST#${postId}`, sk: `OPTION#${optionId}` },
            UpdateExpression: 'ADD voteCount :one',
            ExpressionAttributeValues: { ':one': 1 },
          }));
        }

        // Return updated poll
        const poll = await hydratePoll(post, userId);
        // Force results since we just voted
        if (poll && !poll.results) {
          poll.userVotedOptionId = optionId;
          poll.totalVotes = (poll.totalVotes || 0) + 1;
          poll.results = post.poll.options.map(o => {
            const count = (o.id === optionId) ? 1 : 0;
            return {
              id: o.id,
              text: o.text,
              voteCount: count,
              percentage: poll.totalVotes > 0 ? Math.round((count / poll.totalVotes) * 100) : 0,
            };
          });
        }

        return ok({ poll });
      } catch (e) {
        console.error('[POST /polls/vote] Failed:', e);
        return bad('Failed to vote', 500);
      }
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

      const qs = event?.queryStringParameters || {};
      const limit = Math.min(parseInt(qs.limit) || 20, 100); // Default 20, max 100
      const offset = Math.max(parseInt(qs.offset) || 0, 0);

      try {
        if (FOLLOWS_TABLE) {
          // Fetch followed users, capped at 200 to keep the fan-out manageable.
          // At 200 follows × 50 posts each the worst case is 10,000 items to sort
          // in memory -- well within Lambda's timeout.  For users following 200+
          // accounts, consider fan-out-on-write (precomputed timelines) instead.
          const MAX_FEED_FOLLOWS = 200;
          const followIds = new Set();
          let followLastKey = undefined;
          do {
            const following = await ddb.send(new QueryCommand({
              TableName: FOLLOWS_TABLE,
              KeyConditionExpression: 'pk = :me',
              ExpressionAttributeValues: { ':me': userId },
              ProjectionExpression: 'sk',
              ConsistentRead: false,
              Limit: MAX_FEED_FOLLOWS - followIds.size,
              ...(followLastKey ? { ExclusiveStartKey: followLastKey } : {}),
            }));
            for (const item of (following.Items || [])) {
              if (item.sk) followIds.add(item.sk);
            }
            followLastKey = following.LastEvaluatedKey;
          } while (followLastKey && followIds.size < MAX_FEED_FOLLOWS);
          followIds.add(userId);

          if (followIds.size > 0) {
            const followIdArray = Array.from(followIds);

            // Batch queries in groups of 25 for higher throughput
            const batchSize = 25;
            const results = [];
            for (let i = 0; i < followIdArray.length; i += batchSize) {
              const batch = followIdArray.slice(i, i + batchSize);
              const batchResults = await Promise.all(batch.map(fid =>
                ddb.send(new QueryCommand({
                  TableName: POSTS_TABLE,
                  KeyConditionExpression: 'pk = :p',
                  ExpressionAttributeValues: { ':p': `USER#${fid}` },
                  ScanIndexForward: false,
                  ConsistentRead: false,
                })).then(r => r.Items || []).catch(e => {
                  console.error(`[Feed] Failed to fetch posts for user ${fid}:`, e);
                  return [];
                })
              ));
              batchResults.forEach(items => items.forEach(i => results.push(i)));
            }
            results.sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0));

            let items = results.slice(offset, offset + limit).map(i => ({
              id: i.id, userId: i.userId, username: i.username || 'unknown',
              handle: i.handle || null,
              text: i.text || '', imageKey: i.imageKey || null,
              imageAspectRatio: i.imageAspectRatio || null,
              images: i.images || null,
              avatarKey: i.avatarKey || null,
              spotifyEmbed: i.spotifyEmbed || null,
              location: i.location || null,
              poll: i.poll || null,
              createdAt: i.createdAt,
            }));

            // Filter out posts from blocked users
            if (BLOCKS_TABLE) {
              try {
                const blockedUserIds = await getBlockedUserIds(userId);
                if (blockedUserIds.length > 0) {
                  items = items.filter(post => !blockedUserIds.includes(post.userId));
                }
              } catch (e) {
                console.error('[Feed] Failed to filter blocked users:', e);
              }
            }

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

            // OPTIMIZATION: Batch fetch comment previews for all posts in parallel (fixes N+1 problem)
            try {
              // Fetch comments for all posts in parallel
              const commentResults = await Promise.all(items.map(post =>
                ddb.send(new QueryCommand({
                  TableName: COMMENTS_TABLE,
                  KeyConditionExpression: 'pk = :p',
                  ExpressionAttributeValues: { ':p': `POST#${post.id}` },
                  ScanIndexForward: true,
                  Limit: 4, // Fetch 4 to detect if there are more than 3
                  ConsistentRead: false, // Eventually consistent for better performance
                })).then(r => ({
                  postId: post.id,
                  items: r.Items || [],
                  count: r.Count || (r.Items || []).length,
                })).catch(e => {
                  console.error(`[Feed] Failed to fetch comments for post ${post.id}:`, e);
                  return { postId: post.id, items: [], count: 0 };
                })
              ));

              // Build a map of postId -> comments and collect all comment author userIds
              const commentMap = {};
              const allCommentUserIds = new Set();
              for (const result of commentResults) {
                const comments = result.items.slice(0, 3).map(it => ({
                  id: it.id,
                  userId: it.userId,
                  handle: it.userHandle || null,
                  text: it.text || '',
                  createdAt: it.createdAt || 0,
                }));
                commentMap[result.postId] = { comments, count: result.count };
                comments.forEach(c => { if (c.userId) allCommentUserIds.add(c.userId); });
              }

              // Batch fetch all comment author avatars in one call
              const commentAuthorUserIds = Array.from(allCommentUserIds);
              let commentAuthorAvatarMap = {};
              if (commentAuthorUserIds.length > 0) {
                try {
                  const summaries = await fetchUserSummaries(commentAuthorUserIds);
                  commentAuthorAvatarMap = Object.fromEntries(
                    summaries.map(u => [u.userId, u.avatarKey || null])
                  );
                } catch (e) {
                  console.error('Failed to fetch comment avatars:', e);
                }
              }

              // Attach comments to posts
              for (const post of items) {
                const data = commentMap[post.id] || { comments: [], count: 0 };
                // Attach avatars to comments
                for (const comment of data.comments) {
                  if (comment.userId && commentAuthorAvatarMap[comment.userId]) {
                    comment.avatarKey = commentAuthorAvatarMap[comment.userId];
                  }
                }
                post.comments = data.comments;
                post.commentCount = data.count;
              }
            } catch (e) {
              console.error('Failed to fetch comment previews for feed:', e);
              // Continue without comment previews if this fails
            }

            // Hydrate polls: check if current user has voted on any poll posts
            if (POLLS_TABLE && userId) {
              try {
                const pollPosts = items.filter(p => p.poll);
                if (pollPosts.length > 0) {
                  const voteChecks = await Promise.all(pollPosts.map(p =>
                    ddb.send(new GetCommand({
                      TableName: POLLS_TABLE,
                      Key: { pk: `POST#${p.id}`, sk: `VOTE#${userId}` },
                      ProjectionExpression: 'optionId',
                      ConsistentRead: false,
                    })).then(r => ({ postId: p.id, optionId: r.Item?.optionId || null }))
                      .catch(() => ({ postId: p.id, optionId: null }))
                  ));

                  // For voted polls, fetch full results
                  for (const vc of voteChecks) {
                    if (vc.optionId) {
                      const post = pollPosts.find(p => p.id === vc.postId);
                      if (post) {
                        const hydrated = await hydratePoll(post, userId);
                        if (hydrated) post.poll = hydrated;
                      }
                    }
                  }
                }
              } catch (e) {
                console.error('Failed to hydrate polls for feed:', e);
              }
            }

            return ok({ items });
          }
        }

        // Fallback: no FOLLOWS_TABLE configured, just show user's own posts
        // (replaces hot FEED GSI partition query that would throttle at scale)
        const ownPosts = await ddb.send(new QueryCommand({
          TableName: POSTS_TABLE,
          KeyConditionExpression: 'pk = :p',
          ExpressionAttributeValues: { ':p': `USER#${userId}` },
          ScanIndexForward: false,
          Limit: limit + offset,
          ConsistentRead: false,
        }));

        let items = (ownPosts.Items || []).slice(offset, offset + limit).map(i => ({
          id: i.id, userId: i.userId, username: i.username || 'unknown',
          handle: i.handle || null,
          text: i.text || '', imageKey: i.imageKey || null,
          imageAspectRatio: i.imageAspectRatio || null,
          images: i.images || null,
          avatarKey: i.avatarKey || null,
          spotifyEmbed: i.spotifyEmbed || null,
          location: i.location || null,
          poll: i.poll || null,
          createdAt: i.createdAt,
        }));

        // Filter out posts from blocked users
        if (BLOCKS_TABLE) {
          try {
            const blockedUserIds = await getBlockedUserIds(userId);
            if (blockedUserIds.length > 0) {
              items = items.filter(post => !blockedUserIds.includes(post.userId));
            }
          } catch (e) {
            console.error('[Feed] Failed to filter blocked users:', e);
          }
        }

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

        // Fetch comment previews in parallel (matches primary feed path pattern)
        try {
          const commentResults = await Promise.all(items.map(post =>
            ddb.send(new QueryCommand({
              TableName: COMMENTS_TABLE,
              KeyConditionExpression: 'pk = :p',
              ExpressionAttributeValues: { ':p': `POST#${post.id}` },
              ScanIndexForward: true,
              Limit: 4,
              ConsistentRead: false,
            })).then(r => ({
              postId: post.id,
              items: r.Items || [],
              count: r.Count || (r.Items || []).length,
            })).catch(e => {
              console.error(`[Feed fallback] Failed to fetch comments for post ${post.id}:`, e);
              return { postId: post.id, items: [], count: 0 };
            })
          ));

          const commentMap = {};
          const allCommentUserIds = new Set();
          for (const result of commentResults) {
            const comments = result.items.slice(0, 3).map(it => ({
              id: it.id, userId: it.userId,
              handle: it.userHandle || null,
              text: it.text || '', createdAt: it.createdAt || 0,
            }));
            commentMap[result.postId] = { comments, count: result.count };
            comments.forEach(c => { if (c.userId) allCommentUserIds.add(c.userId); });
          }

          let commentAuthorAvatarMap = {};
          const commentAuthorUserIds = Array.from(allCommentUserIds);
          if (commentAuthorUserIds.length > 0) {
            try {
              const summaries = await fetchUserSummaries(commentAuthorUserIds);
              commentAuthorAvatarMap = Object.fromEntries(
                summaries.map(u => [u.userId, u.avatarKey || null])
              );
            } catch (e) { console.error('[Feed fallback] comment avatars failed:', e); }
          }

          for (const post of items) {
            const data = commentMap[post.id] || { comments: [], count: 0 };
            for (const c of data.comments) {
              if (c.userId && commentAuthorAvatarMap[c.userId]) c.avatarKey = commentAuthorAvatarMap[c.userId];
            }
            post.comments = data.comments;
            post.commentCount = data.count;
          }
        } catch (e) {
          console.error('Failed to fetch comment previews for feed (fallback):', e);
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

      // Moderate content using Amazon Bedrock (text and image)
      const textContent = String(body.text || '').slice(0, 500);
      const imageKey = body.imageKey || null;
      const moderation = await moderateContent(textContent, imageKey);
      if (!moderation.safe) {
        console.log(`[Moderation] Post blocked for user ${userId}: ${moderation.reason}`);
        return bad(`Content blocked: ${moderation.reason || 'Content violates our community guidelines'}`, 403);
      }

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
        text: textContent,
        createdAt: now,
      };

      // Support multi-image posts (new format) with optional photo tags
      if (body.images && Array.isArray(body.images) && body.images.length > 0) {
        item.images = body.images.map((img, index) => {
          const imageItem = {
            key: img.key,
            aspectRatio: img.aspectRatio || 1,
            width: img.width || null,
            height: img.height || null,
            order: img.order !== undefined ? img.order : index,
          };
          // Include photo tags if present
          if (img.tags && Array.isArray(img.tags) && img.tags.length > 0) {
            imageItem.tags = img.tags.map(tag => ({
              userId: String(tag.userId),
              handle: tag.handle ? String(tag.handle) : null,
              x: Number(tag.x) || 0,
              y: Number(tag.y) || 0,
            }));
          }
          return imageItem;
        });
      }
      // Backward compatibility: support legacy single image format
      else if (body.imageKey) {
        item.imageKey = body.imageKey;
        if (body.imageAspectRatio) item.imageAspectRatio = body.imageAspectRatio;
      }

      // Support location tagging
      if (body.location && typeof body.location === 'object' && body.location.name) {
        item.location = {
          name: String(body.location.name).slice(0, 200),
          address: body.location.address ? String(body.location.address).slice(0, 500) : null,
          latitude: Number(body.location.latitude) || 0,
          longitude: Number(body.location.longitude) || 0,
          placeId: body.location.placeId ? String(body.location.placeId) : null,
        };
      }

      // Support polls
      if (body.poll && typeof body.poll === 'object') {
        const pollQuestion = String(body.poll.question || '').slice(0, 140);
        const pollOptions = (body.poll.options || [])
          .filter(o => typeof o === 'string' && o.trim())
          .slice(0, 5)
          .map((text, idx) => {
            const optionId = `opt_${idx}`;
            return { id: optionId, text: String(text).slice(0, 80) };
          });

        if (pollQuestion && pollOptions.length >= 2) {
          item.poll = {
            question: pollQuestion,
            options: pollOptions,
            totalVotes: 0,
          };

          // Write poll option items to POLLS_TABLE for vote counting (non-blocking)
          if (POLLS_TABLE) {
            try {
              for (const opt of pollOptions) {
                await ddb.send(new PutCommand({
                  TableName: POLLS_TABLE,
                  Item: {
                    pk: `POST#${id}`,
                    sk: `OPTION#${opt.id}`,
                    optionId: opt.id,
                    text: opt.text,
                    voteCount: 0,
                  },
                }));
              }
            } catch (e) {
              console.error('[POST /posts] Failed to write poll options to POLLS_TABLE:', e);
              // Continue - poll metadata is still saved on the post item itself
            }
          }
        }
      }

      // Support Spotify embeds
      if (body.spotifyEmbed && typeof body.spotifyEmbed === 'object') {
        item.spotifyEmbed = {
          type: body.spotifyEmbed.type,
          spotifyId: body.spotifyEmbed.spotifyId,
          spotifyUrl: body.spotifyEmbed.spotifyUrl,
          title: body.spotifyEmbed.title,
          thumbnailUrl: body.spotifyEmbed.thumbnailUrl,
          thumbnailWidth: body.spotifyEmbed.thumbnailWidth || null,
          thumbnailHeight: body.spotifyEmbed.thumbnailHeight || null,
        };
      }

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

      // Notify users tagged in photos
      try {
        if (item.images && Array.isArray(item.images)) {
          const taggedUserIds = new Set();
          for (const img of item.images) {
            if (img.tags && Array.isArray(img.tags)) {
              for (const tag of img.tags) {
                if (tag.userId && tag.userId !== userId && !taggedUserIds.has(tag.userId)) {
                  taggedUserIds.add(tag.userId);
                  await createNotification(tag.userId, 'photo_tag', userId, id, 'tagged you in a photo');
                }
              }
            }
          }
        }
      } catch (e) { console.error('notify photo tags failed', e); }

      // Return the full post object (matching GET /posts/{id} format)
      return ok({
        id: item.id,
        userId: item.userId,
        username: item.username,
        handle: item.handle,
        text: item.text,
        imageKey: item.imageKey || null,
        imageAspectRatio: item.imageAspectRatio || null,
        images: item.images || null,
        avatarKey: item.avatarKey,
        spotifyEmbed: item.spotifyEmbed || null,
        location: item.location || null,
        poll: item.poll || null,
        createdAt: item.createdAt,
      });
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
        // Note: ACL removed - bucket uses "Bucket owner enforced" Object Ownership
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
        // Note: ACL removed - bucket uses "Bucket owner enforced" Object Ownership
      });
      const url = await getSignedUrl(s3, put, { expiresIn: 60 });
      return ok({ url, key });
    }

    // ----- DELETE /media/{key} - Delete unused uploaded media -----
    if (method === 'DELETE' && path.startsWith('/media/')) {
      if (!userId) return bad('Unauthorized', 401);

      // Extract the key from the path (everything after /media/) and decode it
      const encodedKey = path.substring('/media/'.length);
      if (!encodedKey) return bad('Missing media key', 400);

      const key = decodeURIComponent(encodedKey);

      // Verify user owns this media - key should start with u/{userId}/ or a/{userId}/
      const userPrefix = `u/${userId}/`;
      const avatarPrefix = `a/${userId}/`;

      if (!key.startsWith(userPrefix) && !key.startsWith(avatarPrefix)) {
        console.log(`[DELETE /media] User ${userId} attempted to delete unauthorized key: ${key}`);
        return bad('Forbidden: You can only delete your own media', 403);
      }

      // Delete from S3
      try {
        await s3.send(new DeleteObjectCommand({
          Bucket: MEDIA_BUCKET,
          Key: key,
        }));
        console.log(`[DELETE /media] Deleted S3 object: ${key} for user ${userId}`);
        return ok({ deleted: true, key });
      } catch (e) {
        console.error('[DELETE /media] Failed to delete S3 object:', e);
        return bad('Failed to delete media', 500);
      }
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

      // Delete photo from S3 if it exists
      if (post.imageKey && MEDIA_BUCKET) {
        try {
          await s3.send(new DeleteObjectCommand({
            Bucket: MEDIA_BUCKET,
            Key: post.imageKey,
          }));
          console.log(`[DELETE /posts] Deleted S3 object: ${post.imageKey}`);
        } catch (e) {
          console.error('[DELETE /posts] Failed to delete S3 object:', e);
          // Continue with post deletion even if S3 deletion fails
        }
      }

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
        imageAspectRatio: post.imageAspectRatio || null,
        images: post.images || null,
        avatarKey: post.avatarKey || null,
        spotifyEmbed: post.spotifyEmbed || null,
        location: post.location || null,
        poll: await hydratePoll(post, userId),
        createdAt: post.createdAt,
      });
    }

    // ----- /u/:handle, /u/:handle/followers, /u/:handle/following, /u/:handle/posts -----
    const userRoute = matchUserRoutes(path);
    if (method === 'GET' && userRoute) {
      if (!userId) return bad('Unauthorized', 401);

      const qs = event?.queryStringParameters || {};
      const limit = Math.min(parseInt(qs.limit) || 20, 100); // Default 20, max 100
      const offset = Math.max(parseInt(qs.offset) || 0, 0);

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

        // Check if this is the user's own profile
        const isSelf = userId === targetId;

        // Private profiles by default: only show posts if viewer is following or viewing own profile
        const canViewPosts = isSelf || iFollow;

        // Fetch posts with pagination
        let items = [];
        let isPrivate = false;

        if (canViewPosts) {
          const allPosts = await listPostsByUserId(targetId);
          items = allPosts.slice(offset, offset + limit);
        } else {
          // Profile is private and viewer is not following
          isPrivate = true;
        }

        // Hydrate profile posts with fresh avatar/handle
        try {
          const summaries = await fetchUserSummaries([targetId]);
          const freshAvatar = (summaries[0] && summaries[0].avatarKey) || null;
          const freshHandle = (summaries[0] && summaries[0].handle) || null;
          if (freshAvatar) { for (const it of items) it.avatarKey = freshAvatar; }
          if (freshHandle) { for (const it of items) { it.username = (it.username && it.username !== 'unknown') ? it.username : freshHandle; it.handle = freshHandle; } }
        } catch (e) { console.error('PROFILE avatar/handle hydrate failed', e); }

        // Fetch comment previews for all posts
        try {
          for (const post of items) {
            // First, get the total count of comments
            const countResult = await ddb.send(new QueryCommand({
              TableName: COMMENTS_TABLE,
              KeyConditionExpression: 'pk = :p',
              ExpressionAttributeValues: { ':p': `POST#${post.id}` },
              Select: 'COUNT',
              ConsistentRead: true,
            }));
            const totalCommentCount = countResult.Count || 0;

            // Then fetch first 4 comments for preview
            const commentsResult = await ddb.send(new QueryCommand({
              TableName: COMMENTS_TABLE,
              KeyConditionExpression: 'pk = :p',
              ExpressionAttributeValues: { ':p': `POST#${post.id}` },
              ScanIndexForward: true,
              Limit: 4, // Fetch 4 to detect if there are more than 3
              ConsistentRead: true,
            }));

            const allComments = (commentsResult.Items || []).map(it => ({
              id: it.id,
              userId: it.userId,
              handle: it.userHandle || null,
              text: it.text || '',
              createdAt: it.createdAt || 0,
            }));

            // Take only first 3 for preview
            const comments = allComments.slice(0, 3);

            // Fetch avatarKey for comment authors
            if (comments.length > 0) {
              try {
                const userIds = [...new Set(comments.map(c => c.userId).filter(Boolean))];
                if (userIds.length > 0) {
                  const summaries = await fetchUserSummaries(userIds);
                  const avatarMap = Object.fromEntries(
                    summaries.map(u => [u.userId, u.avatarKey || null])
                  );
                  for (const comment of comments) {
                    if (comment.userId && avatarMap[comment.userId]) {
                      comment.avatarKey = avatarMap[comment.userId];
                    }
                  }
                }
              } catch (e) {
                console.error('Failed to fetch comment avatars:', e);
              }
            }

            post.comments = comments;
            post.commentCount = totalCommentCount;
          }
        } catch (e) {
          console.error('Failed to fetch comment previews for profile:', e);
          // Continue without comment previews if this fails
        }

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
          isPrivate,
          isFollowPending, followStatus: (iFollow ? 'following' : (isFollowPending ? 'pending' : 'none')) });
      }

      if (userRoute.kind === 'followers') {
        if (!FOLLOWS_TABLE) return bad('Follows not enabled', 500);
        // Pagination parameters
        const qs = event?.queryStringParameters || {};
        const pageLimit = Math.min(parseInt(qs.limit) || 50, 100);
        const pageOffset = Math.max(parseInt(qs.offset) || 0, 0);

        // Try GSI first for efficient query, fallback to scan with pagination
        let followerIds = [];
        let totalCount = 0;
        try {
          // Use GSI on sk (target user) if available
          const gsiResult = await ddb.send(new QueryCommand({
            TableName: FOLLOWS_TABLE,
            IndexName: 'byFollowee',
            KeyConditionExpression: 'sk = :t',
            ExpressionAttributeValues: { ':t': targetId },
            ProjectionExpression: 'pk',
          }));
          const allFollowerIds = (gsiResult.Items || []).map(i => i.pk).filter(Boolean);
          totalCount = allFollowerIds.length;
          followerIds = allFollowerIds.slice(pageOffset, pageOffset + pageLimit);
        } catch (e) {
          // GSI not available, use scan with limit (not ideal but better than before)
          console.warn('[followers] GSI query failed, using paginated scan', e);
          const scan = await ddb.send(new ScanCommand({
            TableName: FOLLOWS_TABLE,
            FilterExpression: 'sk = :t',
            ExpressionAttributeValues: { ':t': targetId },
            ProjectionExpression: 'pk',
            Limit: 1000, // Cap to prevent timeout
          }));
          const allFollowerIds = (scan.Items || []).map(i => i.pk).filter(Boolean);
          totalCount = allFollowerIds.length;
          followerIds = allFollowerIds.slice(pageOffset, pageOffset + pageLimit);
        }

        const users = await fetchUserSummaries(followerIds);
        // Batch check which users the viewer follows (fixes N+1 problem)
        const viewerFollowsSet = await batchCheckFollowing(userId, followerIds);
        const items = users.map(u => ({
          handle: u.handle,
          fullName: u.fullName || null,
          avatarKey: u.avatarKey || null,
          userId: u.userId,
          isFollowing: viewerFollowsSet.has(u.userId),
        }));
        return ok({
          items,
          total: totalCount,
          limit: pageLimit,
          offset: pageOffset,
          hasMore: pageOffset + pageLimit < totalCount,
        });
      }

      if (userRoute.kind === 'following') {
        if (!FOLLOWS_TABLE) return bad('Follows not enabled', 500);
        // Pagination parameters
        const qs = event?.queryStringParameters || {};
        const pageLimit = Math.min(parseInt(qs.limit) || 50, 100);
        const pageOffset = Math.max(parseInt(qs.offset) || 0, 0);

        // Query by pk is efficient (uses partition key)
        const q = await ddb.send(new QueryCommand({
          TableName: FOLLOWS_TABLE,
          KeyConditionExpression: 'pk = :p',
          ExpressionAttributeValues: { ':p': targetId },
          ProjectionExpression: 'sk',
          ConsistentRead: true,
        }));
        const allFollowingIds = (q.Items || []).map(i => i.sk).filter(Boolean);
        const totalCount = allFollowingIds.length;
        const followingIds = allFollowingIds.slice(pageOffset, pageOffset + pageLimit);

        const users = await fetchUserSummaries(followingIds);
        // Batch check which users the viewer follows (fixes N+1 problem)
        const viewerFollowsSet = await batchCheckFollowing(userId, followingIds);
        const items = users.map(u => ({
          handle: u.handle,
          fullName: u.fullName || null,
          avatarKey: u.avatarKey || null,
          userId: u.userId,
          isFollowing: viewerFollowsSet.has(u.userId),
        }));
        return ok({
          items,
          total: totalCount,
          limit: pageLimit,
          offset: pageOffset,
          hasMore: pageOffset + pageLimit < totalCount,
        });
      }

      if (userRoute.kind === 'posts') {
        // Check if this is the user's own profile
        const isSelf = userId === targetId;

        // Check if viewer is following the profile owner
        const iFollow = await isFollowing(userId, targetId);

        // Private profiles by default: only show posts if viewer is following or viewing own profile
        const canViewPosts = isSelf || iFollow;

        let items = [];
        let isPrivate = false;

        if (canViewPosts) {
          const allPosts = await listPostsByUserId(targetId);
          items = allPosts.slice(offset, offset + limit);
        } else {
          // Profile is private and viewer is not following
          isPrivate = true;
        }
        try {
          const summaries = await fetchUserSummaries([targetId]);
          const fresh = (summaries[0] && summaries[0].avatarKey) || null;
          if (fresh) { for (const it of items) it.avatarKey = fresh; }
        } catch (e) { console.error('USER POSTS avatar hydrate failed', e); }

        // OPTIMIZATION: Batch fetch comment previews for all posts in parallel (fixes N+1 problem)
        try {
          if (items.length > 0) {
            // Fetch comments for all posts in parallel
            const commentResults = await Promise.all(items.map(post =>
              ddb.send(new QueryCommand({
                TableName: COMMENTS_TABLE,
                KeyConditionExpression: 'pk = :p',
                ExpressionAttributeValues: { ':p': `POST#${post.id}` },
                ScanIndexForward: true,
                Limit: 4,
                ConsistentRead: false,
              })).then(r => ({
                postId: post.id,
                items: r.Items || [],
                count: r.Count || (r.Items || []).length,
              })).catch(e => {
                console.error(`[UserPosts] Failed to fetch comments for post ${post.id}:`, e);
                return { postId: post.id, items: [], count: 0 };
              })
            ));

            // Build comment map and collect all comment author userIds
            const commentMap = {};
            const allCommentUserIds = new Set();
            for (const result of commentResults) {
              const comments = result.items.slice(0, 3).map(it => ({
                id: it.id,
                userId: it.userId,
                handle: it.userHandle || null,
                text: it.text || '',
                createdAt: it.createdAt || 0,
              }));
              commentMap[result.postId] = { comments, count: result.count };
              comments.forEach(c => { if (c.userId) allCommentUserIds.add(c.userId); });
            }

            // Batch fetch all comment author avatars
            const commentAuthorUserIds = Array.from(allCommentUserIds);
            let commentAuthorAvatarMap = {};
            if (commentAuthorUserIds.length > 0) {
              try {
                const summaries = await fetchUserSummaries(commentAuthorUserIds);
                commentAuthorAvatarMap = Object.fromEntries(
                  summaries.map(u => [u.userId, u.avatarKey || null])
                );
              } catch (e) {
                console.error('Failed to fetch comment avatars:', e);
              }
            }

            // Attach comments to posts
            for (const post of items) {
              const data = commentMap[post.id] || { comments: [], count: 0 };
              for (const comment of data.comments) {
                if (comment.userId && commentAuthorAvatarMap[comment.userId]) {
                  comment.avatarKey = commentAuthorAvatarMap[comment.userId];
                }
              }
              post.comments = data.comments;
              post.commentCount = data.count;
            }
          }
        } catch (e) {
          console.error('Failed to fetch comment previews for user posts:', e);
          // Continue without comment previews if this fails
        }

        return ok({ items, isPrivate });
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
          // Note: GSIs do not support ConsistentRead - only eventually consistent
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

      // Batch-check follow status for all results at once instead of N+1 individual queries
      const targetIds = items.map(it => it.userId).filter(Boolean);
      const followingSet = await batchCheckFollowing(userId, targetIds);

      const out = items
        .filter(it => it.handle && it.userId)
        .map(it => ({
          handle: it.handle,
          fullName: it.fullName || null,
          avatarKey: it.avatarKey || null,
          isFollowing: followingSet.has(it.userId),
        }));
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
      // Check if already following to avoid duplicate count updates
      const alreadyFollowing = await isFollowing(userId, targetId);
      if (!alreadyFollowing) {
        await ddb.send(new PutCommand({
          TableName: FOLLOWS_TABLE,
          Item: { pk: userId, sk: targetId },
        }));
        // Update denormalized counts
        await updateFollowCounts(userId, targetId, 1);
      }
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
      // Check if actually following to avoid duplicate count updates
      const wasFollowing = await isFollowing(userId, targetId);
      if (wasFollowing) {
        await ddb.send(new DeleteCommand({
          TableName: FOLLOWS_TABLE,
          Key: { pk: userId, sk: targetId },
        }));
        // Update denormalized counts
        await updateFollowCounts(userId, targetId, -1);
      }
      // Remove any existing 'follow' notification
      try { if (NOTIFICATIONS_TABLE && targetId !== userId) { await deleteNotifications(targetId, 'follow', userId, null); } } catch (e) { console.error('unfollow cleanup notify failed', e); }
      return ok({ ok: true });
    }

    // ----- Blocking endpoints -----
    if (route === 'POST /block') {
      if (!BLOCKS_TABLE) return bad('Blocking not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);

      const body = JSON.parse(event.body || '{}');
      const targetUserId = String(body.userId || '').trim();
      if (!targetUserId) return bad('Missing userId', 400);
      if (targetUserId === userId) return bad('Cannot block yourself', 400);

      // Create block record
      await ddb.send(new PutCommand({
        TableName: BLOCKS_TABLE,
        Item: {
          pk: `USER#${userId}`,
          sk: `BLOCKED#${targetUserId}`,
          blockedUserId: targetUserId,
          createdAt: Date.now(),
        },
      }));

      // Remove follow relationships in both directions
      if (FOLLOWS_TABLE) {
        try {
          await Promise.all([
            ddb.send(new DeleteCommand({
              TableName: FOLLOWS_TABLE,
              Key: { pk: userId, sk: targetUserId },
            })),
            ddb.send(new DeleteCommand({
              TableName: FOLLOWS_TABLE,
              Key: { pk: targetUserId, sk: userId },
            })),
          ]);
        } catch (e) {
          console.error('[Blocking] Failed to remove follow relationships:', e);
        }
      }

      return ok({ success: true, blocked: true });
    }

    if (route === 'POST /unblock') {
      if (!BLOCKS_TABLE) return bad('Blocking not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);

      const body = JSON.parse(event.body || '{}');
      const targetUserId = String(body.userId || '').trim();
      if (!targetUserId) return bad('Missing userId', 400);

      await ddb.send(new DeleteCommand({
        TableName: BLOCKS_TABLE,
        Key: { pk: `USER#${userId}`, sk: `BLOCKED#${targetUserId}` },
      }));

      return ok({ success: true, blocked: false });
    }

    if (route === 'GET /blocked') {
      if (!BLOCKS_TABLE) return bad('Blocking not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);

      const result = await ddb.send(new QueryCommand({
        TableName: BLOCKS_TABLE,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': `USER#${userId}` },
        ConsistentRead: true,
      }));

      const blockedUserIds = (result.Items || []).map(item => item.blockedUserId).filter(Boolean);
      const users = await fetchUserSummaries(blockedUserIds);

      return ok({
        items: users.map(u => ({
          userId: u.userId,
          handle: u.handle || null,
          fullName: u.fullName || null,
          avatarKey: u.avatarKey || null,
        }))
      });
    }

    // ----- Reporting endpoints -----
    if (route === 'POST /report') {
      if (!REPORTS_TABLE) return bad('Reporting not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);

      const body = JSON.parse(event.body || '{}');
      const contentType = String(body.contentType || '').trim(); // 'post' or 'comment'
      const contentId = String(body.contentId || '').trim();
      const reason = String(body.reason || '').trim().slice(0, 500);

      if (!contentType || !contentId) return bad('Missing contentType or contentId', 400);
      if (!['post', 'comment'].includes(contentType)) return bad('Invalid contentType', 400);
      if (!reason) return bad('Missing reason', 400);

      const reportId = randomUUID();
      const now = Date.now();

      // Get reported content details
      let reportedUserId = null;
      let contentText = null;

      if (contentType === 'post') {
        try {
          const postResult = await ddb.send(new QueryCommand({
            TableName: POSTS_TABLE,
            IndexName: 'byId',
            KeyConditionExpression: 'id = :id',
            ExpressionAttributeValues: { ':id': contentId },
            Limit: 1,
          }));
          const post = (postResult.Items || [])[0];
          if (post) {
            reportedUserId = post.userId;
            contentText = post.text;
          }
        } catch (e) {
          console.error('[Reporting] Failed to fetch post:', e);
        }
      } else if (contentType === 'comment') {
        try {
          const commentResult = await ddb.send(new ScanCommand({
            TableName: COMMENTS_TABLE,
            FilterExpression: 'id = :id',
            ExpressionAttributeValues: { ':id': contentId },
            Limit: 1,
            ConsistentRead: true,
          }));
          const comment = (commentResult.Items || [])[0];
          if (comment) {
            reportedUserId = comment.userId;
            contentText = comment.text;
          }
        } catch (e) {
          console.error('[Reporting] Failed to fetch comment:', e);
        }
      }

      // Create report record
      await ddb.send(new PutCommand({
        TableName: REPORTS_TABLE,
        Item: {
          pk: `REPORT#${reportId}`,
          sk: String(now),
          reportId,
          reporterId: userId,
          reportedUserId,
          contentType,
          contentId,
          contentText,
          reason,
          status: 'pending', // pending, reviewed, resolved
          createdAt: now,
        },
      }));

      return ok({ success: true, reportId });
    }

    if (route === 'GET /reports') {
      if (!REPORTS_TABLE) return bad('Reporting not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);
      if (!ADMIN_EMAILS.includes(email)) return bad('Forbidden - Admin only', 403);

      const qs = event?.queryStringParameters || {};
      const status = qs.status || 'pending';

      const result = await ddb.send(new ScanCommand({
        TableName: REPORTS_TABLE,
        FilterExpression: '#status = :status',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':status': status },
        ConsistentRead: true,
      }));

      const reports = (result.Items || []).sort((a, b) => (b.createdAt || 0) - (a.createdAt || 0));

      // Enrich with reporter and reported user info
      const allUserIds = new Set();
      reports.forEach(r => {
        if (r.reporterId) allUserIds.add(r.reporterId);
        if (r.reportedUserId) allUserIds.add(r.reportedUserId);
      });

      const users = await fetchUserSummaries(Array.from(allUserIds));
      const userMap = new Map(users.map(u => [u.userId, u]));

      const enrichedReports = reports.map(r => ({
        ...r,
        reporter: userMap.get(r.reporterId) || null,
        reportedUser: userMap.get(r.reportedUserId) || null,
      }));

      return ok({ items: enrichedReports });
    }

    if (method === 'POST' && path.startsWith('/reports/')) {
      if (!REPORTS_TABLE) return bad('Reporting not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);
      if (!ADMIN_EMAILS.includes(email)) return bad('Forbidden - Admin only', 403);

      const reportId = path.split('/')[2];
      const action = path.split('/')[3]; // 'action'

      if (action !== 'action') return bad('Invalid endpoint', 400);

      const body = JSON.parse(event.body || '{}');
      const actionType = String(body.action || '').trim(); // 'delete_content', 'ban_user', 'dismiss'

      if (!actionType) return bad('Missing action', 400);
      if (!['delete_content', 'ban_user', 'dismiss'].includes(actionType)) {
        return bad('Invalid action type', 400);
      }

      // Get the report
      const reportResult = await ddb.send(new QueryCommand({
        TableName: REPORTS_TABLE,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': `REPORT#${reportId}` },
        Limit: 1,
        ConsistentRead: true,
      }));

      const report = (reportResult.Items || [])[0];
      if (!report) return bad('Report not found', 404);

      // Perform the action
      if (actionType === 'delete_content') {
        if (report.contentType === 'post') {
          // Delete the post
          try {
            const postResult = await ddb.send(new QueryCommand({
              TableName: POSTS_TABLE,
              IndexName: 'byId',
              KeyConditionExpression: 'id = :id',
              ExpressionAttributeValues: { ':id': report.contentId },
              Limit: 1,
            }));
            const post = (postResult.Items || [])[0];
            if (post) {
              await ddb.send(new DeleteCommand({
                TableName: POSTS_TABLE,
                Key: { pk: post.pk, sk: post.sk },
              }));
            }
          } catch (e) {
            console.error('[Moderation] Failed to delete post:', e);
            return bad('Failed to delete post', 500);
          }
        } else if (report.contentType === 'comment') {
          // Delete the comment
          try {
            const commentResult = await ddb.send(new ScanCommand({
              TableName: COMMENTS_TABLE,
              FilterExpression: 'id = :id',
              ExpressionAttributeValues: { ':id': report.contentId },
              Limit: 1,
              ConsistentRead: true,
            }));
            const comment = (commentResult.Items || [])[0];
            if (comment) {
              await ddb.send(new DeleteCommand({
                TableName: COMMENTS_TABLE,
                Key: { pk: comment.pk, sk: comment.sk },
              }));
            }
          } catch (e) {
            console.error('[Moderation] Failed to delete comment:', e);
            return bad('Failed to delete comment', 500);
          }
        }
      } else if (actionType === 'ban_user') {
        // Ban the user by deleting their Cognito account
        if (report.reportedUserId) {
          try {
            await cognito.send(new AdminDeleteUserCommand({
              UserPoolId: USER_POOL_ID,
              Username: report.reportedUserId,
            }));
          } catch (e) {
            console.error('[Moderation] Failed to ban user:', e);
            return bad('Failed to ban user', 500);
          }
        }
      }

      // Update report status
      await ddb.send(new UpdateCommand({
        TableName: REPORTS_TABLE,
        Key: { pk: report.pk, sk: report.sk },
        UpdateExpression: 'SET #status = :status, reviewedBy = :userId, reviewedAt = :now, actionTaken = :action',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: {
          ':status': 'resolved',
          ':userId': userId,
          ':now': Date.now(),
          ':action': actionType,
        },
      }));

      return ok({ success: true, action: actionType });
    }

    // Check if user is blocked from viewing content
    if (route === 'GET /is-blocked') {
      if (!BLOCKS_TABLE) return bad('Blocking not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);

      const qs = event?.queryStringParameters || {};
      const targetUserId = qs.userId;
      if (!targetUserId) return bad('Missing userId', 400);

      const blocked = await hasBlockBetween(userId, targetUserId);
      return ok({ blocked });
    }

    // ===================== SCOOPS (Stories) =====================
    // SCOOPS_TABLE schema:
    //   Partition key: "USER#<userId>" (String) - attribute name is literally "USER#<userId>"
    //   Sort key: "SCOOP#<timestamp>#<uuid>" (String) - attribute name is literally "SCOOP#<timestamp>#<uuid>"
    //   GSI "byId": partition key = "id" (String) - enables O(1) lookups by scoop ID
    //   NOTE: You must create this GSI in the AWS console or via IaC:
    //     Index name: byId, Partition key: id (String), Projection: ALL
    const SCOOP_PK = 'USER#<userId>';
    const SCOOP_SK = 'SCOOP#<timestamp>#<uuid>';

    /**
     * Look up a scoop by its UUID using the "byId" GSI.
     * Falls back to a limited scan if the GSI doesn't exist yet.
     */
    async function getScoopById(scoopId) {
      // Try GSI query first (O(1) lookup)
      try {
        const gsiResult = await ddb.send(new QueryCommand({
          TableName: SCOOPS_TABLE,
          IndexName: 'byId',
          KeyConditionExpression: 'id = :id',
          ExpressionAttributeValues: { ':id': scoopId },
          Limit: 1,
        }));
        if (gsiResult.Items && gsiResult.Items.length > 0) {
          return gsiResult.Items[0];
        }
        return null;
      } catch (e) {
        // GSI may not exist yet - fall back to scan with a warning
        console.warn('[getScoopById] GSI "byId" query failed, falling back to scan. Create the GSI to fix this:', e.message);
        const scanResult = await ddb.send(new ScanCommand({
          TableName: SCOOPS_TABLE,
          FilterExpression: 'id = :id',
          ExpressionAttributeValues: { ':id': scoopId },
        }));
        return (scanResult.Items || [])[0] || null;
      }
    }

    // Get scoops feed - returns scoops from followed users grouped by user
    if (route === 'GET /scoops/feed') {
      if (!SCOOPS_TABLE) return bad('Scoops not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);

      const now = Date.now();
      const twentyFourHoursAgo = now - (24 * 60 * 60 * 1000);

      // Get users this person follows
      const followedUsers = [];
      if (FOLLOWS_TABLE) {
        const followsResult = await ddb.send(new QueryCommand({
          TableName: FOLLOWS_TABLE,
          KeyConditionExpression: 'pk = :pk',
          ExpressionAttributeValues: { ':pk': userId },
        }));
        for (const item of (followsResult.Items || [])) {
          if (item.sk) followedUsers.push(item.sk);
        }
      }

      // Query each followed user's scoops by partition key (replaces full-table scan)
      // Uses the same pattern as GET /scoops/me and GET /scoops/user/:id
      const allScoops = [];
      const batchSize = 10;
      for (let i = 0; i < followedUsers.length; i += batchSize) {
        const batch = followedUsers.slice(i, i + batchSize);
        const batchResults = await Promise.all(batch.map(fid =>
          ddb.send(new QueryCommand({
            TableName: SCOOPS_TABLE,
            KeyConditionExpression: '#pk = :pk AND #sk > :minSk',
            ExpressionAttributeNames: { '#pk': SCOOP_PK, '#sk': SCOOP_SK },
            ExpressionAttributeValues: {
              ':pk': `USER#${fid}`,
              ':minSk': `SCOOP#${twentyFourHoursAgo}`,
            },
            ScanIndexForward: true,
          })).then(r => (r.Items || []).filter(s => s.expiresAt > now))
            .catch(e => {
              console.error(`[ScoopsFeed] Failed to fetch scoops for user ${fid}:`, e);
              return [];
            })
        ));
        for (const items of batchResults) {
          allScoops.push(...items);
        }
      }

      // Group scoops by user
      const userScoopsMap = new Map();

      for (const s of allScoops) {
        const scoop = {
          id: s.id,
          pk: s[SCOOP_PK],
          sk: s[SCOOP_SK],
          userId: s.userId,
          handle: s.handle,
          avatarKey: s.avatarKey,
          mediaKey: s.mediaKey,
          mediaType: s.mediaType,
          mediaAspectRatio: s.mediaAspectRatio,
          textOverlays: s.textOverlays,
          createdAt: s.createdAt,
          expiresAt: s.expiresAt,
          viewCount: s.viewCount || 0,
          viewed: (s.viewers || []).includes(userId),
        };

        if (!userScoopsMap.has(s.userId)) {
          userScoopsMap.set(s.userId, {
            userId: s.userId,
            handle: s.handle,
            avatarKey: s.avatarKey,
            scoops: [],
            hasUnviewed: false,
            latestScoopAt: 0,
          });
        }

        const userEntry = userScoopsMap.get(s.userId);
        userEntry.scoops.push(scoop);
        if (!scoop.viewed) userEntry.hasUnviewed = true;
        if (scoop.createdAt > userEntry.latestScoopAt) {
          userEntry.latestScoopAt = scoop.createdAt;
        }
      }

      // Sort scoops within each user by createdAt (oldest first)
      for (const entry of userScoopsMap.values()) {
        entry.scoops.sort((a, b) => a.createdAt - b.createdAt);
      }

      // Sort by latest scoop and prioritize unviewed
      const items = Array.from(userScoopsMap.values())
        .sort((a, b) => {
          if (a.hasUnviewed !== b.hasUnviewed) return a.hasUnviewed ? -1 : 1;
          return b.latestScoopAt - a.latestScoopAt;
        });

      return ok({ items });
    }

    // Get my own scoops
    if (route === 'GET /scoops/me') {
      if (!SCOOPS_TABLE) return bad('Scoops not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);

      const now = Date.now();
      const twentyFourHoursAgo = now - (24 * 60 * 60 * 1000);

      // Query by partition key
      const result = await ddb.send(new QueryCommand({
        TableName: SCOOPS_TABLE,
        KeyConditionExpression: '#pk = :pk AND #sk > :sk',
        ExpressionAttributeNames: {
          '#pk': SCOOP_PK,
          '#sk': SCOOP_SK,
        },
        ExpressionAttributeValues: {
          ':pk': `USER#${userId}`,
          ':sk': `SCOOP#${twentyFourHoursAgo}`,
        },
        ScanIndexForward: true, // oldest first
      }));

      const items = (result.Items || [])
        .filter(s => s.expiresAt > now)
        .map(s => ({
          id: s.id,
          userId: s.userId,
          handle: s.handle,
          avatarKey: s.avatarKey,
          mediaKey: s.mediaKey,
          mediaType: s.mediaType,
          mediaAspectRatio: s.mediaAspectRatio,
          textOverlays: s.textOverlays,
          createdAt: s.createdAt,
          expiresAt: s.expiresAt,
          viewCount: s.viewCount || 0,
        }));

      return ok({ items });
    }

    // Get scoops for a specific user
    if (method === 'GET' && path.startsWith('/scoops/user/')) {
      if (!SCOOPS_TABLE) return bad('Scoops not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);

      const targetUserId = path.split('/')[3];
      if (!targetUserId) return bad('Missing userId', 400);

      const now = Date.now();
      const twentyFourHoursAgo = now - (24 * 60 * 60 * 1000);

      // Query by partition key
      const result = await ddb.send(new QueryCommand({
        TableName: SCOOPS_TABLE,
        KeyConditionExpression: '#pk = :pk AND #sk > :sk',
        ExpressionAttributeNames: {
          '#pk': SCOOP_PK,
          '#sk': SCOOP_SK,
        },
        ExpressionAttributeValues: {
          ':pk': `USER#${targetUserId}`,
          ':sk': `SCOOP#${twentyFourHoursAgo}`,
        },
        ScanIndexForward: true, // oldest first
      }));

      const items = (result.Items || [])
        .filter(s => s.expiresAt > now)
        .map(s => ({
          id: s.id,
          userId: s.userId,
          handle: s.handle,
          avatarKey: s.avatarKey,
          mediaKey: s.mediaKey,
          mediaType: s.mediaType,
          mediaAspectRatio: s.mediaAspectRatio,
          textOverlays: s.textOverlays,
          createdAt: s.createdAt,
          expiresAt: s.expiresAt,
          viewCount: s.viewCount || 0,
          viewed: (s.viewers || []).includes(userId),
        }));

      return ok({ items });
    }

    // Create a new scoop
    if (route === 'POST /scoops') {
      if (!SCOOPS_TABLE) return bad('Scoops not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);

      const body = JSON.parse(event.body || '{}');
      const { mediaKey, mediaType, mediaAspectRatio, textOverlays, trimParams } = body;

      if (!mediaKey) return bad('Missing mediaKey', 400);
      if (!mediaType || !['image', 'video'].includes(mediaType)) {
        return bad('Invalid mediaType', 400);
      }

      // Process video if trim params are provided
      let finalMediaKey = mediaKey;
      console.log(`[CreateScoop] Video processing check - mediaType: ${mediaType}, trimParams: ${JSON.stringify(trimParams)}, MEDIA_BUCKET: ${MEDIA_BUCKET ? 'set' : 'NOT SET'}`);

      if (mediaType === 'video' && trimParams && MEDIA_BUCKET) {
        const { startTime, endTime } = trimParams;
        console.log(`[CreateScoop] trimParams validation - startTime: ${startTime} (${typeof startTime}), endTime: ${endTime} (${typeof endTime})`);
        if (typeof startTime === 'number' && typeof endTime === 'number' && endTime > startTime) {
          console.log(`[CreateScoop] Processing video with trim: ${startTime}s - ${endTime}s`);
          try {
            finalMediaKey = await processVideo(MEDIA_BUCKET, mediaKey, startTime, endTime);
            console.log(`[CreateScoop] Video processed, new key: ${finalMediaKey}`);
          } catch (e) {
            console.error('[CreateScoop] Video processing failed:', e);
            // Continue with original key if processing fails
          }
        } else {
          console.log(`[CreateScoop] Skipping video processing - invalid trimParams`);
        }
      } else {
        if (mediaType !== 'video') {
          console.log(`[CreateScoop] Skipping video processing - mediaType is '${mediaType}', not 'video'`);
        } else if (!trimParams) {
          console.log(`[CreateScoop] Skipping video processing - no trimParams provided`);
        } else if (!MEDIA_BUCKET) {
          console.log(`[CreateScoop] Skipping video processing - MEDIA_BUCKET not configured`);
        }
      }

      const now = Date.now();
      const id = randomUUID();
      const expiresAt = now + (24 * 60 * 60 * 1000); // 24 hours from now

      // Get user info
      const handle = await getHandleForUserId(userId);
      const userInfo = await ddb.send(new GetCommand({
        TableName: USERS_TABLE,
        Key: { pk: `USER#${userId}` },
      }));
      const avatarKey = userInfo.Item?.avatarKey || null;

      // Create item with literal key attribute names
      const item = {
        [SCOOP_PK]: `USER#${userId}`,
        [SCOOP_SK]: `SCOOP#${now}#${id}`,
        id,
        userId,
        handle,
        avatarKey,
        mediaKey: finalMediaKey,
        mediaType,
        mediaAspectRatio: mediaAspectRatio || null,
        textOverlays: textOverlays || [],
        createdAt: now,
        expiresAt,
        viewCount: 0,
        viewers: [],
      };

      await ddb.send(new PutCommand({
        TableName: SCOOPS_TABLE,
        Item: item,
      }));

      return ok({
        id,
        userId,
        handle,
        avatarKey,
        mediaKey: finalMediaKey,
        mediaType,
        mediaAspectRatio,
        textOverlays,
        createdAt: now,
        expiresAt,
        viewCount: 0,
      });
    }

    // Get a single scoop by ID
    if (method === 'GET' && path.match(/^\/scoops\/[^\/]+$/) && !path.includes('/user/')) {
      if (!SCOOPS_TABLE) return bad('Scoops not enabled', 501);

      const scoopId = path.split('/')[2];
      if (!scoopId || scoopId === 'feed' || scoopId === 'me') {
        return bad('Not found', 404);
      }

      const scoop = await getScoopById(scoopId);
      if (!scoop) return bad('Scoop not found', 404);

      return ok({
        id: scoop.id,
        userId: scoop.userId,
        handle: scoop.handle,
        avatarKey: scoop.avatarKey,
        mediaKey: scoop.mediaKey,
        mediaType: scoop.mediaType,
        mediaAspectRatio: scoop.mediaAspectRatio,
        textOverlays: scoop.textOverlays,
        createdAt: scoop.createdAt,
        expiresAt: scoop.expiresAt,
        viewCount: scoop.viewCount || 0,
        viewed: userId ? (scoop.viewers || []).includes(userId) : false,
      });
    }

    // Delete a scoop
    if (method === 'DELETE' && path.startsWith('/scoops/')) {
      if (!SCOOPS_TABLE) return bad('Scoops not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);

      const scoopId = path.split('/')[2];
      if (!scoopId) return bad('Missing scoopId', 400);

      const scoop = await getScoopById(scoopId);
      if (!scoop) return bad('Scoop not found', 404);
      if (scoop.userId !== userId) return bad('Forbidden', 403);

      // Delete from S3
      if (scoop.mediaKey) {
        try {
          await s3.send(new DeleteObjectCommand({
            Bucket: MEDIA_BUCKET,
            Key: scoop.mediaKey,
          }));
        } catch (e) {
          console.warn('Failed to delete scoop media from S3:', e);
        }
      }

      // Delete from DynamoDB using the actual key values
      await ddb.send(new DeleteCommand({
        TableName: SCOOPS_TABLE,
        Key: {
          [SCOOP_PK]: scoop[SCOOP_PK],
          [SCOOP_SK]: scoop[SCOOP_SK],
        },
      }));

      return ok({ success: true });
    }

    // Mark a scoop as viewed
    if (method === 'POST' && path.match(/^\/scoops\/[^\/]+\/view$/)) {
      if (!SCOOPS_TABLE) return bad('Scoops not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);

      const scoopId = path.split('/')[2];
      if (!scoopId) return bad('Missing scoopId', 400);

      const scoop = await getScoopById(scoopId);
      if (!scoop) return bad('Scoop not found', 404);

      // Don't track views on own scoops
      if (scoop.userId === userId) {
        return ok({ success: true });
      }

      // Check if already viewed
      const viewers = scoop.viewers || [];
      if (viewers.includes(userId)) {
        return ok({ success: true });
      }

      // Add viewer and increment count
      await ddb.send(new UpdateCommand({
        TableName: SCOOPS_TABLE,
        Key: {
          [SCOOP_PK]: scoop[SCOOP_PK],
          [SCOOP_SK]: scoop[SCOOP_SK],
        },
        UpdateExpression: 'SET viewers = list_append(if_not_exists(viewers, :empty), :viewer), viewCount = if_not_exists(viewCount, :zero) + :one',
        ExpressionAttributeValues: {
          ':viewer': [userId],
          ':empty': [],
          ':zero': 0,
          ':one': 1,
        },
      }));

      return ok({ success: true });
    }

    // React to a scoop (toggle heart reaction)
    if (method === 'POST' && path.match(/^\/scoops\/[^\/]+\/react$/)) {
      if (!SCOOPS_TABLE) return bad('Scoops not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);

      const scoopId = path.split('/')[2];
      if (!scoopId) return bad('Missing scoopId', 400);

      const body = JSON.parse(event.body || '{}');
      const emoji = String(body.emoji || '❤️').trim().slice(0, 8);

      const scoop = await getScoopById(scoopId);
      if (!scoop) return bad('Scoop not found', 404);

      // Can't react to own scoop
      if (scoop.userId === userId) return bad('Cannot react to your own scoop', 400);

      const reactions = scoop.reactions || [];
      const existingIndex = reactions.findIndex(r => r.userId === userId);

      if (existingIndex >= 0) {
        // Remove existing reaction (toggle off)
        await ddb.send(new UpdateCommand({
          TableName: SCOOPS_TABLE,
          Key: {
            [SCOOP_PK]: scoop[SCOOP_PK],
            [SCOOP_SK]: scoop[SCOOP_SK],
          },
          UpdateExpression: `REMOVE reactions[${existingIndex}]`,
        }));
        return ok({ reacted: false, emoji: null });
      } else {
        // Add reaction
        const reaction = { userId, emoji, createdAt: Date.now() };
        await ddb.send(new UpdateCommand({
          TableName: SCOOPS_TABLE,
          Key: {
            [SCOOP_PK]: scoop[SCOOP_PK],
            [SCOOP_SK]: scoop[SCOOP_SK],
          },
          UpdateExpression: 'SET reactions = list_append(if_not_exists(reactions, :empty), :reaction)',
          ExpressionAttributeValues: {
            ':reaction': [reaction],
            ':empty': [],
          },
        }));

        // Notify scoop owner
        try {
          await createNotification(scoop.userId, 'scoop_reaction', userId, null, 'reacted to your scoop');
        } catch (e) { console.error('notify scoop reaction failed', e); }

        return ok({ reacted: true, emoji });
      }
    }

    // Reply to a scoop (send a comment)
    if (method === 'POST' && path.match(/^\/scoops\/[^\/]+\/reply$/)) {
      if (!SCOOPS_TABLE) return bad('Scoops not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);

      const scoopId = path.split('/')[2];
      if (!scoopId) return bad('Missing scoopId', 400);

      const body = JSON.parse(event.body || '{}');
      const text = String(body.text || '').trim().slice(0, 500);
      if (!text) return bad('Text required', 400);

      const scoop = await getScoopById(scoopId);
      if (!scoop) return bad('Scoop not found', 404);

      // Can't reply to own scoop
      if (scoop.userId === userId) return bad('Cannot reply to your own scoop', 400);

      // Moderate content
      const moderation = await moderateContent(text);
      if (!moderation.safe) {
        return bad(`Content blocked: ${moderation.reason || 'Content violates our community guidelines'}`, 403);
      }

      const handle = await getHandleForUserId(userId) || 'unknown';
      const id = randomUUID();
      const reply = { id, userId, handle, text, createdAt: Date.now() };

      await ddb.send(new UpdateCommand({
        TableName: SCOOPS_TABLE,
        Key: {
          [SCOOP_PK]: scoop[SCOOP_PK],
          [SCOOP_SK]: scoop[SCOOP_SK],
        },
        UpdateExpression: 'SET replies = list_append(if_not_exists(replies, :empty), :reply)',
        ExpressionAttributeValues: {
          ':reply': [reply],
          ':empty': [],
        },
      }));

      // Notify scoop owner
      try {
        await createNotification(scoop.userId, 'scoop_reply', userId, null, 'replied to your scoop');
      } catch (e) { console.error('notify scoop reply failed', e); }

      return ok({ id, text, createdAt: reply.createdAt });
    }

    // Get scoop viewers
    if (method === 'GET' && path.match(/^\/scoops\/[^\/]+\/viewers$/)) {
      if (!SCOOPS_TABLE) return bad('Scoops not enabled', 501);
      if (!userId) return bad('Unauthorized', 401);

      const scoopId = path.split('/')[2];
      if (!scoopId) return bad('Missing scoopId', 400);

      const scoop = await getScoopById(scoopId);
      if (!scoop) return bad('Scoop not found', 404);

      // Only owner can see viewers
      if (scoop.userId !== userId) return bad('Forbidden', 403);

      const viewerIds = scoop.viewers || [];
      const reactions = scoop.reactions || [];
      const replies = scoop.replies || [];

      // Build a map of userId -> reaction emoji for quick lookup
      const reactionMap = {};
      for (const r of reactions) {
        reactionMap[r.userId] = r.emoji;
      }

      // Batch-fetch viewer and reply user details instead of N+1 individual queries
      const allUserIds = [...new Set([...viewerIds, ...replies.map(r => r.userId)].filter(Boolean))];
      const userSummaries = allUserIds.length > 0 ? await fetchUserSummaries(allUserIds) : [];
      const userMap = {};
      for (const u of userSummaries) { userMap[u.userId] = u; }

      const items = viewerIds
        .filter(vid => userMap[vid])
        .map(vid => ({
          userId: vid,
          handle: userMap[vid].handle || null,
          avatarKey: userMap[vid].avatarKey || null,
          viewedAt: Date.now(),
          reaction: reactionMap[vid] || null,
        }));

      const enrichedReplies = replies.map(reply => ({
        id: reply.id,
        userId: reply.userId,
        handle: reply.handle || null,
        avatarKey: userMap[reply.userId]?.avatarKey || null,
        text: reply.text,
        createdAt: reply.createdAt,
      }));

      return ok({ items, replies: enrichedReplies });
    }

    // ----- default -----
    if (method === 'DELETE') {
      console.error(`[404] DELETE request not matched: route="${route}", path="${path}"`);
    }
    return bad('Not found', 404);

  } catch (err) {
    console.error(err);
    return bad('Server error', 500);
  }
};
