import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const apiProxyTarget = process.env.VITE_PROXY_API_TARGET ?? 'http://127.0.0.1:8001'
const allowedHosts = ['orca.local', '139.147.9.248']

export default defineConfig({
  plugins: [react()],
  server: {
    allowedHosts,
    proxy: {
      '/api': {
        target: apiProxyTarget,
        changeOrigin: true,
      },
      '/health': {
        target: apiProxyTarget,
        changeOrigin: true,
      },
    },
  },
  preview: {
    allowedHosts,
  },
})
