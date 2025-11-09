import React from "react";
// This wrapper tries to locate a token in a few common places to keep your app wiring simple.
import NotificationsDropdown from "./components/NotificationsDropdown";

type Props = {
  apiBaseUrl: string;
  token?: string | null;
};

function guessToken(): string | null {
  try {
    // common places your app might store it
    const t1 = localStorage.getItem("token");
    if (t1) return t1;
    const t2 = sessionStorage.getItem("token");
    if (t2) return t2;
  } catch {}
  return null;
}

export default function NavNotifications(props: Partial<Props>) {
  const apiBaseUrl = props.apiBaseUrl || (window as any).CONFIG?.API_BASE_URL || "/prod";
  const token = props.token ?? guessToken();

  return <NotificationsDropdown apiBaseUrl={apiBaseUrl} token={token} />;
}
