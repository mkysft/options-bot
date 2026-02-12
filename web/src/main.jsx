import React from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "./styles.css";
import { App } from "./App";

const THEME_STORAGE_KEY = "options-bot-theme";
const applyInitialTheme = () => {
  const root = document.documentElement;
  let stored = "";
  try {
    stored = window.localStorage.getItem(THEME_STORAGE_KEY) ?? "";
  } catch {
    stored = "";
  }
  const prefersDark = window.matchMedia?.("(prefers-color-scheme: dark)").matches;
  const theme = stored === "dark" || stored === "light" ? stored : prefersDark ? "dark" : "light";
  root.classList.toggle("dark", theme === "dark");
  root.style.colorScheme = theme;
};

applyInitialTheme();

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 10_000,
      retry: 1,
      refetchOnWindowFocus: false
    }
  }
});

const appTree = (
  <QueryClientProvider client={queryClient}>
    <App />
  </QueryClientProvider>
);

createRoot(document.getElementById("root")).render(
  import.meta.env.DEV ? appTree : <React.StrictMode>{appTree}</React.StrictMode>
);
