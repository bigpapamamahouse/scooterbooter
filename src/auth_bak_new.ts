// auth.ts
import {
  AuthenticationDetails,
  CognitoUser,
  CognitoUserPool,
  CognitoUserAttribute,
} from 'amazon-cognito-identity-js';
import { CONFIG } from './config';

const pool = new CognitoUserPool({
  UserPoolId: CONFIG.userPoolId,
  ClientId: CONFIG.userPoolClientId,
});

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
  return new CognitoUser({ Username: username, Pool: pool });
}

export function signUp(email: string, password: string, invite: string) {
  const e = assertNonEmpty(email, 'Email');
  const p = assertNonEmpty(password, 'Password');
  const i = assertNonEmpty(invite, 'Invite code');
  const attrs = [new CognitoUserAttribute({ Name: 'custom:invite', Value: i })];

  return new Promise((resolve, reject) => {
    pool.signUp(e, p, attrs, [], (err, data) => (err ? reject(err) : resolve(data)));
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
