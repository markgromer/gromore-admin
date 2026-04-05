import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  base: '/static/react/',
  build: {
    outDir: path.resolve(__dirname, '../webapp/static/react'),
    emptyOutDir: true,
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'src'),
    },
  },
  server: {
    proxy: {
      '/client': 'http://localhost:5000',
      '/static': 'http://localhost:5000',
    },
  },
})
