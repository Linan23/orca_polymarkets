import { useEffect } from "react";
import { useLocation } from "react-router-dom";

function titleForPath(pathname: string) {
  if (pathname === "/") return "Home | Orca";
  if (pathname === "/following") return "Following | Orca";
  if (pathname === "/leaderboard") return "Leaderboard | Orca";
  if (pathname === "/definitions") return "Definitions | Orca";
  if (pathname === "/about") return "About | Orca";
  if (pathname === "/login") return "Sign In | Orca";
  if (pathname.startsWith("/markets/")) return "Market Profile | Orca";
  if (pathname.startsWith("/users/")) return "Trader Profile | Orca";
  return "Orca";
}

export default function PageTitle() {
  const { pathname } = useLocation();

  useEffect(() => {
    document.title = titleForPath(pathname);
  }, [pathname]);

  return null;
}
