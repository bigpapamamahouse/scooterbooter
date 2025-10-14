
import { AuthenticationDetails, CognitoUser, CognitoUserPool, CognitoUserAttribute } from 'amazon-cognito-identity-js'
import { CONFIG } from './config'
const pool = new CognitoUserPool({ UserPoolId: CONFIG.userPoolId, ClientId: CONFIG.userPoolClientId })
export function signUp(email: string, password: string, invite: string) {
  const attrs = [new CognitoUserAttribute({ Name: 'custom:invite', Value: invite })]
  return new Promise((resolve, reject) => { pool.signUp(email, password, attrs, [], (err, data) => { if (err) reject(err); else resolve(data) }) })
}
export function confirm(email: string, code: string) {
  const user = new CognitoUser({ Username: email, Pool: pool })
  return new Promise((resolve, reject) => { user.confirmRegistration(code, true, (err, res) => err ? reject(err) : resolve(res)) })
}
export function login(email: string, password: string) {
  const auth = new AuthenticationDetails({ Username: email, Password: password })
  const user = new CognitoUser({ Username: email, Pool: pool })
  return new Promise<{ idToken: string; user: CognitoUser }>((resolve, reject) => {
    user.authenticateUser(auth, { onSuccess: (session) => resolve({ idToken: session.getIdToken().getJwtToken(), user }), onFailure: reject })
  })
}
export function logout(email: string) { const user = new CognitoUser({ Username: email, Pool: pool }); user.signOut() }
