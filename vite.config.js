import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd());
  const configuredApiBaseUrl = env.VITE_API_BASE_URL?.replace(/\/$/, "");

  return {
    plugins: [
      react(),
      tailwindcss(),
    ],
    server: {
      headers: {
        // Required for SharedArrayBuffer (used by Emscripten pthreads in WASM)
        'Cross-Origin-Opener-Policy': 'same-origin',
        'Cross-Origin-Embedder-Policy': 'require-corp',
      },
      proxy: {
        '/api': {
          target: configuredApiBaseUrl, // Backend server URL
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api/, '')
        }
      }
    },
    worker: {
      format: 'es',
      plugins: () => []
    },
    optimizeDeps: {
      exclude: ['../../../bin/PLY.js']
    },
    assetsInclude: ['**/*.wasm'],
    build: {
      rollupOptions: {
        output: {
          assetFileNames: (assetInfo) => {
            if (assetInfo.name?.endsWith('.wasm')) {
              return 'assets/[name][extname]';
            }
            return 'assets/[name]-[hash][extname]';
          }
        }
      }
    }
  };
});

