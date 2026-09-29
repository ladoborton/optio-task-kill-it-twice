import vue from '@vitejs/plugin-vue';
import { defineConfig } from 'vite';

// In the container nginx proxies /api to the api service; `npm run dev` does the same here.
export default defineConfig({
  plugins: [vue()],
  server: { port: 5173, proxy: { '/api': 'http://localhost:3000' } },
});
