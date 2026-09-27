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
        },
        // Proxy GCS signed URLs through the dev server to bypass browser CORS/COEP restrictions.
        // DRC file URLs are rewritten from their full GCS URL to /gcs-proxy/<path>?<query>
        // before being passed to the loader (see loaderManager.js rewriteGcsUrl helper).
        '/gcs-proxy': {
          target: 'https://storage.googleapis.com',
          changeOrigin: true,
          secure: true,
          rewrite: (path) => path.replace(/^\/gcs-proxy/, ''),
          configure: (proxy) => {
            proxy.on('proxyRes', (proxyRes) => {
              // Remove headers that would block the resource under COEP
              delete proxyRes.headers['cross-origin-resource-policy'];
              delete proxyRes.headers['cross-origin-embedder-policy'];
              // Inject permissive CORS so Three.js XHR succeeds
              proxyRes.headers['access-control-allow-origin'] = '*';
            });
          }
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

