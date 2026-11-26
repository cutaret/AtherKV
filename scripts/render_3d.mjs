import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { chromium } from 'playwright';
const ROOT = process.cwd();
const DOCS_DIR = path.join(ROOT, 'docs');
const OUT_DIR = path.join(DOCS_DIR, 'assets');
const TMP_DIR = path.join(ROOT, '.tmp-3d-frames');
const TOTAL_FRAMES = 200; // 10 seconds @ 20fps — seamless loop
const FPS = 20;
const WIDTH = 1200;
const HEIGHT = 675;
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
};
fs.mkdirSync(OUT_DIR, { recursive: true });
fs.rmSync(TMP_DIR, { recursive: true, force: true });
fs.mkdirSync(TMP_DIR, { recursive: true });
const server = http.createServer((req, res) => {
  const urlPath = (req.url || '/').split('?')[0];
  const filePath = path.join(DOCS_DIR, urlPath === '/' ? '3d.html' : urlPath);
  if (!filePath.startsWith(DOCS_DIR) || !fs.existsSync(filePath)) {
    res.writeHead(404);
    res.end('Not found');
    return;
  }
  const ext = path.extname(filePath);
  res.writeHead(200, { 'Content-Type': MIME[ext] || 'application/octet-stream' });
  fs.createReadStream(filePath).pipe(res);
});
await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
const { port } = server.address();
const targetURL = `http://127.0.0.1:${port}/3d.html?capture=1`;
const browser = await chromium.launch({
  headless: true,
  args: [
    '--use-gl=angle',
    '--use-angle=swiftshader',
    '--enable-unsafe-swiftshader',
    '--ignore-gpu-blocklist',
    '--no-sandbox',
  ],
});
try {
  const page = await browser.newPage({ viewport: { width: WIDTH, height: HEIGHT }, deviceScaleFactor: 1 });
  page.on('pageerror', (err) => {
    console.error('Page error:', err);
    process.exitCode = 1;
  });
  await page.goto(targetURL, { waitUntil: 'networkidle', timeout: 60000 });
  await page.waitForFunction(() => window.__AETHER_READY === true, { timeout: 30000 });
  console.log(`Capturing ${TOTAL_FRAMES} deterministic 3D frames at ${WIDTH}x${HEIGHT}...`);
  for (let i = 0; i < TOTAL_FRAMES; i++) {
    const t = i / TOTAL_FRAMES;
    await page.evaluate((val) => window.__setCaptureProgress(val), t);
    const frameFile = path.join(TMP_DIR, `frame_${String(i).padStart(4, '0')}.png`);
    await page.screenshot({ path: frameFile, type: 'png' });
    if ((i + 1) % 40 === 0) console.log(`  captured ${i + 1}/${TOTAL_FRAMES}`);
  }
} finally {
  await browser.close();
  server.close();
}
const inputPattern = path.join(TMP_DIR, 'frame_%04d.png');
const gifOut = path.join(OUT_DIR, 'aetherkv-3d.gif');
const mp4Out = path.join(OUT_DIR, 'aetherkv-3d.mp4');
// 1. High-clarity looping GIF (960px wide, two-pass custom palette, crisp dark gradients)
execFileSync('ffmpeg', [
  '-y',
  '-framerate', String(FPS),
  '-i', inputPattern,
  '-vf', `fps=${FPS},scale=960:-1:flags=lanczos,split[s0][s1];[s0]palettegen=max_colors=192:stats_mode=diff[p];[s1][p]paletteuse=dither=bayer:bayer_scale=3:diff_mode=rectangle`,
  '-loop', '0',
  gifOut,
], { stdio: 'inherit' });
// 2. Razor-sharp silent H.264 MP4 (ideal for LinkedIn if you want zero GIF compression)
execFileSync('ffmpeg', [
  '-y',
  '-framerate', String(FPS),
  '-i', inputPattern,
  '-c:v', 'libx264',
  '-pix_fmt', 'yuv420p',
  '-crf', '18',
  '-movflags', '+faststart',
  mp4Out,
], { stdio: 'inherit' });
fs.rmSync(TMP_DIR, { recursive: true, force: true });
console.log('Generated:', gifOut, `(${(fs.statSync(gifOut).size / 1024 / 1024).toFixed(2)} MiB)`);
console.log('Generated:', mp4Out, `(${(fs.statSync(mp4Out).size / 1024 / 1024).toFixed(2)} MiB)`);
