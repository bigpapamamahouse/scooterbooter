// auth.ts
import {
  AuthenticationDetails,
  CognitoUser,
  CognitoUserPool,
  CognitoUserAttribute,
} from 'amazon-cognito-identity-js';
import { CONFIG } from './config';

let poolInstance: CognitoUserPool | null = null;

function createPool(): CognitoUserPool {
  if (!poolInstance) {
    const userPoolId = CONFIG.userPoolId;
    const clientId = CONFIG.userPoolClientId;

    if (!userPoolId || !clientId) {
      throw new Error('Both UserPoolId and ClientId are required.');
    }

    poolInstance = new CognitoUserPool({
      UserPoolId: userPoolId,
      ClientId: clientId,
    });
  }
  return poolInstance;
}

function assertNonEmpty(value: string | undefined | null, field: string) {
  const v = (value ?? '').trim();
  if (!v) {
    const err = new Error(`${field} is required`);
    // @ts-ignore
    err.code = 'VALIDATION_EMPTY';
    throw err;
  }
  return v;
}

function makeUser(username: string) {
  return new CognitoUser({ Username: username, Pool: createPool() });
}

export function signUp(email: string, password: string, invite: string) {
  const e = assertNonEmpty(email, 'Email');
  const p = assertNonEmpty(password, 'Password');
  const i = assertNonEmpty(invite, 'Invite code');
  const attrs = [new CognitoUserAttribute({ Name: 'custom:invite', Value: i })];

  return new Promise((resolve, reject) => {
    createPool().signUp(e, p, attrs, [], (err, data) => (err ? reject(err) : resolve(data)));
  });
}

/** Renamed to avoid shadowing window.confirm() in the app */
export function confirmSignup(email: string, code: string) {
  const e = assertNonEmpty(email, 'Email');
  const c = assertNonEmpty(code, 'Confirmation code');
  const user = makeUser(e);

  return new Promise((resolve, reject) => {
    user.confirmRegistration(c, true, (err, res) => (err ? reject(err) : resolve(res)));
  });
}

/** Backwards-compat export (DEPRECATED). Prefer confirmSignup(). */
export const confirm = (...args: [string, string]) => confirmSignup(...args);

export function login(email: string, password: string) {
  const e = assertNonEmpty(email, 'Email');
  const p = assertNonEmpty(password, 'Password');

  const auth = new AuthenticationDetails({ Username: e, Password: p });
  const user = makeUser(e);

  return new Promise<{ idToken: string; accessToken: string; refreshToken: string; user: CognitoUser }>((resolve, reject) => {
    user.authenticateUser(auth, {
      onSuccess: (session) => {
        resolve({
          idToken: session.getIdToken().getJwtToken(),
          accessToken: session.getAccessToken().getJwtToken(),
          refreshToken: session.getRefreshToken().getToken(),
          user,
        });
      },
      onFailure: reject,
    });
  });
}

export function resend(email: string): Promise<void> {
  const pool = createPool();
  const user = new CognitoUser({ Username: email, Pool: pool });
  return new Promise((resolve, reject) => {
    user.resendConfirmationCode((err /*, result */) => {
      if (err) return reject(err);
      resolve();
    });
  });
}

export function resendConfirmation(email: string) {
  const e = assertNonEmpty(email, 'Email');
  return new Promise((resolve, reject) => {
    makeUser(e).resendConfirmationCode((err, result) => (err ? reject(err) : resolve(result)));
  });
}

export function logout(email: string) {
  const e = (email ?? '').trim();
  if (!e) return;
  makeUser(e).signOut();
}

// --- Forgot / Reset / Change password (Cognito User Pools) ---

export function readAccessTokenSync(): string | null {
  try {
    for (let i = 0; i < localStorage.length; i++) {
      const k = localStorage.key(i)!;
      if (k && (k.endsWith('.accessToken') || k.includes('accessToken'))) {
        const v = localStorage.getItem(k);
        if (v && v.split('.').length === 3) return v;
      }
    }
  } catch {}
  return null;
}
function __poolRegion(id?: string) {
  const s = String(id || CONFIG.userPoolId || '');
  return s.includes('_') ? s.split('_')[0] : s;
}
function __cognitoUrl() {
  const region = __poolRegion();
  if (!region) throw new Error('Missing Cognito region (USER_POOL_ID)');
  return `https://cognito-idp.${region}.amazonaws.com/`;
}
function __authHeader(target: string) {
  return {
    'Content-Type': 'application/x-amz-json-1.1',
    'X-Amz-Target': `AWSCognitoIdentityProviderService.${target}`,
  };
}

export async function forgotPassword(email: string) {
  const clientId = CONFIG.userPoolClientId;
  console.log('[forgotPassword] Debug info:', {
    clientId,
    fromEnv: import.meta.env.VITE_USER_POOL_CLIENT_ID,
    fromWindow: (window as any)?.CONFIG?.USER_POOL_CLIENT_ID,
    windowConfigExists: !!(window as any)?.CONFIG,
    windowConfigKeys: (window as any)?.CONFIG ? Object.keys((window as any).CONFIG) : []
  });
  if (!clientId) throw new Error('Missing Cognito ClientId');
  const r = await fetch(__cognitoUrl(), {
    method: 'POST',
    headers: __authHeader('ForgotPassword'),
    body: JSON.stringify({ ClientId: clientId, Username: email.trim().toLowerCase() }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function confirmForgotPassword(email: string, code: string, newPassword: string) {
  const clientId = CONFIG.userPoolClientId;
  if (!clientId) throw new Error('Missing Cognito ClientId');
  const r = await fetch(__cognitoUrl(), {
    method: 'POST',
    headers: __authHeader('ConfirmForgotPassword'),
    body: JSON.stringify({
      ClientId: clientId,
      Username: email.trim().toLowerCase(),
      ConfirmationCode: code.trim(),
      Password: newPassword,
    }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export async function changePassword(oldPassword: string, newPassword: string) {
  const AccessToken = readAccessTokenSync();
  if (!AccessToken) throw new Error('Not logged in (no access token found)');
  const r = await fetch(__cognitoUrl(), {
    method: 'POST',
    headers: __authHeader('ChangePassword'),
    body: JSON.stringify({ AccessToken, PreviousPassword: oldPassword, ProposedPassword: newPassword }),
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}
