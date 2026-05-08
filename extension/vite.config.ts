import { defineConfig } from 'vite';
import webExtension from 'vite-plugin-web-extension';

export default defineConfig({
    plugins: [
        webExtension({
            browser: 'firefox',
            manifest: 'manifest.json',
        }),
    ],
    build: {
        outDir: 'dist',
        emptyOutDir: true,
        sourcemap: true,
        target: 'firefox115',
    },
});
