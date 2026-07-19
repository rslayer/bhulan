import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    port: 5173,
    proxy: {
      "/v1": {
        // 127.0.0.1 rather than "localhost" on purpose: on machines where
        // localhost resolves to IPv6 (::1) first, a backend bound to IPv4
        // 0.0.0.0/127.0.0.1 is unreachable through the proxy. Vite then falls
        // back to serving index.html, so API calls silently return HTML (or a
        // 404 on POST) instead of failing loudly. Override with
        // VITE_BACKEND_URL to point at a non-local backend.
        target: process.env.VITE_BACKEND_URL || "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
  },
});
