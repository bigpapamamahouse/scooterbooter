export const CONFIG = {
  get API_BASE_URL() {
    return import.meta.env.VITE_API_URL || (window as any)?.CONFIG?.API_BASE_URL || "";
  },
  get apiUrl() {
    return import.meta.env.VITE_API_URL || (window as any)?.CONFIG?.apiUrl || "";
  },
  get userPoolId() {
    return import.meta.env.VITE_USER_POOL_ID || (window as any)?.CONFIG?.USER_POOL_ID || "";
  },
  get userPoolClientId() {
    return import.meta.env.VITE_USER_POOL_CLIENT_ID || (window as any)?.CONFIG?.USER_POOL_CLIENT_ID || "";
  }
};
