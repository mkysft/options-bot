import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { resolve } from "node:path";

export default defineConfig({
  root: __dirname,
  base: "/ui/",
  resolve: {
    alias: {
      "@": resolve(__dirname, "src")
    }
  },
  plugins: [react(), tailwindcss()],
  server: {
    host: "127.0.0.1",
    port: 5173,
    strictPort: true,
    proxy: {
      "/health": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/run-status": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/risk-status": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/kill-switch": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/account-summary": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/acceptance-gate": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/positions": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/ibkr-readiness": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/market-data-diagnostics": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/ibkr-status": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/ibkr-launch": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/orders": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/scan": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/score": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/decision": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/recommendations": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/api-request-logs": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/backtest": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/bot-policy": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/env-config": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/app": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/propose-order": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/approve-order": { target: "http://127.0.0.1:8000", changeOrigin: true },
      "/config": { target: "http://127.0.0.1:8000", changeOrigin: true }
    },
    hmr: {
      host: "127.0.0.1",
      port: 5173,
      clientPort: 5173
    }
  },
  build: {
    outDir: resolve(__dirname, "../src/ui-dist"),
    emptyOutDir: true,
    sourcemap: false
  }
});
