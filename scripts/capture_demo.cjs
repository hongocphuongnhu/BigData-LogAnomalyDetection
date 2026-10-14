// Quay dashboard that bang Playwright; fixture/demo duoc tao rieng, khong xoa checkpoint.
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const { chromium } = require('../data/raw/tools/node_modules/playwright');
const root = path.resolve(__dirname, '..');
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
function child(command, args, logfile) {
  const output = fs.openSync(logfile, 'w');
  const process = spawn(command, args, { cwd: root, windowsHide: true, stdio: ['ignore', output, output] });
  fs.closeSync(output);
  const done = new Promise((resolve, reject) => {
    process.on('error', reject);
    process.on('exit', code => code === 0 ? resolve() : reject(new Error(`${command}: exit ${code}`)));
  });
  return { process, done };
}
(async () => {
  const run = `demo-${Date.now()}`;
  const folder = path.join(root, 'data', 'stream', run);
  fs.mkdirSync(path.join(folder, 'incoming'), { recursive: true });
  const logfile = path.join(root, 'data/raw/setup', `${run}.log`);
  const stream = child('docker', ['compose', 'exec', '-T', 'spark-master', '/opt/spark/bin/spark-submit', '--master', 'spark://spark-master:7077', '--conf', 'spark.sql.shuffle.partitions=4', '--py-files', '/opt/spark/jobs/features_common.py', '/opt/spark/jobs/stream_detect.py', '--input-path', `/opt/spark/data/stream/${run}/incoming`, '--checkpoint-path', `/opt/spark/data/stream/${run}/checkpoint`, '--detector', 'rule', '--duration', '105'], logfile);
  for (let n = 0; n < 60 && !fs.readFileSync(logfile, 'utf8').includes('STREAM READY'); n++) {
    if (stream.process.exitCode !== null) throw new Error('Stream exited before ready');
    await wait(1000);
  }
  if (!fs.readFileSync(logfile, 'utf8').includes('STREAM READY')) throw new Error('Stream startup timeout');
  const browser = await chromium.launch({ channel: 'chrome', headless: true });
  const context = await browser.newContext({ viewport: { width: 1280, height: 800 }, recordVideo: { dir: path.join(root, 'data/raw/setup/video'), size: { width: 1280, height: 800 } } });
  const page = await context.newPage();
  try {
    const from = new Date().toISOString();
    await page.goto(`http://localhost:5601/app/dashboards#/view/nhu-anomaly-alerts?_g=(filters:!(),refreshInterval:(pause:!f,value:10000),time:(from:'${from}',to:now))`);
    await page.getByRole('heading', { name: 'Dashboards - Anomaly & Alerts' }).waitFor();
    const dismiss = page.getByRole('button', { name: 'Dismiss', exact: true });
    for (let n = 0; n < 3; n++) {
      if (await dismiss.count()) await dismiss.first().click().catch(() => {});
      await wait(500);
    }
    const sim = child('python', ['-B', '-m', 'generator.stream_simulate', '--attack', 'ddos', '--attack-after', '15', '--duration', '65', '--seed', '85', '--out-dir', path.join(folder, 'incoming'), '--manifest', path.join(folder, 'manifest.json')], path.join(root, 'data/raw/setup', `${run}-sim.log`));
    await sim.done;
    await wait(15000);
    await page.screenshot({ path: path.join(root, 'doc/images/demo-live-final.png'), fullPage: true });
    await stream.done;
    const video = page.video();
    await context.close();
    await video.saveAs(path.join(root, 'doc/demo-live.webm'));
    console.log('VIDEO OK: doc/demo-live.webm');
  } finally {
    await context.close().catch(() => {});
    await browser.close();
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
