// Dung docx-js va html2pptx; phan noi dung nam trong doc/ de de sua truoc thuyet trinh.
const fs = require('fs');
const path = require('path');
const os = require('os');
const tools = path.resolve(__dirname, '../data/raw/tools/node_modules');
const { Document, Packer, Paragraph, TextRun, HeadingLevel, TableOfContents,
  Footer, PageNumber, AlignmentType, ImageRun, PageBreak, LevelFormat } = require(path.join(tools, 'docx'));
const pptxgen = require(path.join(tools, 'pptxgenjs'));
const { chromium } = require(path.join(tools, 'playwright'));
const sharp = require(path.join(tools, 'sharp'));
const root = path.resolve(__dirname, '..');
const output = path.join(root, 'doc');
const slidesDir = path.join(root, 'data/raw/setup/slides-html-dinh');
fs.mkdirSync(slidesDir, { recursive: true });
// Hinh ket qua do analysis/*.py ve san; khong chup lai UI nhu phan Nhu.
const figuresDir = path.join(root, 'data/reports/figures');
const FIGURES = [
  ['nasa_status_distribution.png', 'Phân bố mã trạng thái HTTP của log NASA 07/1995'],
  ['nasa_requests_per_hour.png', 'Số request theo giờ của log NASA'],
  ['nasa_top_404_paths.png', 'Các đường dẫn 404 nhiều nhất trong log NASA'],
  ['model_silhouette_by_k.png', 'Silhouette theo k trên mẫu tập train simulator'],
  ['model_pca_clusters.png', 'Các cụm KMeans chiếu PCA 2D: tấn công gom thành cụm nhỏ'],
  ['model_comparison.png', 'So sánh phương pháp trên tập test simulator'],
  ['model_recall_by_attack.png', 'Recall theo từng kiểu tấn công'],
  ['model_pr_curve.png', 'Đường precision–recall của các phương pháp'],
  ['nasa_top_anomalies.png', '20 cửa sổ NASA có điểm bất thường cao nhất'],
];

async function makeWord() {
  const source = fs.readFileSync(path.join(output, 'bao-cao-phan-dinh.md'), 'utf8').split(/\r?\n/);
  const children = [];
  for (const line of source) {
    if (!line.trim()) continue;
    if (line.startsWith('# ')) {
      children.push(new Paragraph({ heading: HeadingLevel.TITLE, children: [new TextRun(line.slice(2))] }));
    } else if (line.startsWith('## ')) {
      if (!children.some(item => item._dinhToc)) {
        const toc = new TableOfContents('Mục lục', { hyperlink: true, headingStyleRange: '1-2' });
        toc._dinhToc = true;
        children.push(new Paragraph({ children: [new PageBreak()] }), toc,
          new Paragraph({ children: [new PageBreak()] }));
      }
      children.push(new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun(line.slice(3))] }));
    } else if (line.startsWith('### ')) {
      children.push(new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun(line.slice(4))] }));
    } else {
      children.push(new Paragraph({ ...(line.startsWith('- ') ? { numbering: { reference: 'bullets', level: 0 } } : {}),
        spacing: { after: 140, line: 276 }, children: [new TextRun(line.startsWith('- ') ? line.slice(2) : line)] }));
    }
  }
  children.push(new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun('9. Hình kết quả')] }));
  for (const [filename, caption] of FIGURES) {
    const imagePath = path.join(figuresDir, filename);
    if (!fs.existsSync(imagePath)) throw new Error(`Missing evidence image: ${filename}`);
    const info = await sharp(imagePath).metadata();
    const width = Math.min(600, 660 * info.width / info.height);
    children.push(new Paragraph({ pageBreakBefore: true, alignment: AlignmentType.CENTER,
      children: [new ImageRun({ type: 'png', data: fs.readFileSync(imagePath), transformation: { width, height: width * info.height / info.width },
        altText: { title: caption, description: caption, name: filename } })] }));
    children.push(new Paragraph({ alignment: AlignmentType.CENTER, children: [new TextRun({ text: caption, italics: true, size: 20 })] }));
  }
  const doc = new Document({ creator: 'Định', title: 'Log Anomaly Detection - Phần Định',
    styles: { default: { document: { run: { font: 'Arial', size: 23 } } }, paragraphStyles: [
      { id: 'Title', name: 'Title', basedOn: 'Normal', run: { size: 40, bold: true, color: '17324D' }, paragraph: { spacing: { before: 360, after: 300 } } },
      { id: 'Heading1', name: 'Heading 1', basedOn: 'Normal', next: 'Normal', quickFormat: true, run: { size: 30, bold: true }, paragraph: { spacing: { before: 260, after: 180 }, outlineLevel: 0 } },
      { id: 'Heading2', name: 'Heading 2', basedOn: 'Normal', next: 'Normal', quickFormat: true, run: { size: 26, bold: true }, paragraph: { spacing: { before: 220, after: 120 }, outlineLevel: 1 } }
    ] }, numbering: { config: [{ reference: 'bullets', levels: [{ level: 0, format: LevelFormat.BULLET, text: '•', alignment: AlignmentType.LEFT,
      style: { paragraph: { indent: { left: 360, hanging: 180 } } } }] }] },
    sections: [{ properties: { page: { size: { width: 11906, height: 16838 }, margin: { top: 1134, bottom: 1134, left: 1417, right: 1134 } } },
      footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [new TextRun('Định · Log Anomaly Detection · '), new TextRun({ children: [PageNumber.CURRENT] })] })] }) }, children }] });
  fs.writeFileSync(path.join(output, 'BaoCao_PhanDinh.docx'), await Packer.toBuffer(doc));
}

function escapeHtml(value) { return value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }
async function makeSlides() {
  const helper = process.env.HTML2PPTX_PATH || path.join(os.homedir(), '.Agents/skills/ak-document-skills/ak-pptx/scripts/html2pptx.js');
  // Helper mac dinh Chromium; Windows tren may nay da co Chrome, khong can tai browser thu hai.
  const originalLaunch = chromium.launch.bind(chromium);
  chromium.launch = options => originalLaunch({ ...options, channel: process.platform === 'win32' ? 'chrome' : options?.channel,
    env: { ...process.env, ...options?.env } });
  const html2pptx = require(helper);
  const items = JSON.parse(fs.readFileSync(path.join(output, 'slide-phan-dinh.json'), 'utf8'));
  const pptx = new pptxgen();
  pptx.layout = 'LAYOUT_16x9'; pptx.author = 'Định'; pptx.title = 'Log Anomaly Detection - Phần Định'; pptx.lang = 'vi-VN';
  for (const [index, item] of items.entries()) {
    const html = `<!doctype html><html lang="vi"><meta charset="utf-8"><style>
      *{box-sizing:border-box}body{width:720pt;height:405pt;margin:0;display:flex;font-family:Arial;background:#F5F7FA;color:#17324D}
      .page{width:100%;height:100%;padding:30pt 36pt;position:relative;border-top:8pt solid #277884}
      .eyebrow{font-size:10pt;color:#277884;letter-spacing:1pt;margin:0 0 12pt}h1{font-size:29pt;line-height:1.14;margin:0 0 24pt;max-width:640pt}
      .body{display:flex;gap:20pt}.text{width:${item.image ? '300pt' : '640pt'}}ul{padding-left:20pt;margin:0}li{font-size:${item.image ? '17pt' : '23pt'};line-height:1.3;margin-bottom:16pt}
      .placeholder{width:300pt;height:235pt;background:#E8EEF4}.footer{position:absolute;left:36pt;bottom:15pt;font-size:9pt;color:#566779}
      </style><body><div class="page"><p class="eyebrow">${escapeHtml(item.eyebrow)}</p><h1>${escapeHtml(item.title)}</h1><div class="body"><div class="text"><ul>${item.bullets.map(text => `<li>${escapeHtml(text)}</li>`).join('')}</ul></div>${item.image ? '<div id="evidence" class="placeholder"></div>' : ''}</div><p class="footer">ĐỊNH · 13/10/2026 · ${index + 1} / ${items.length}</p></div></body></html>`;
    const file = path.join(slidesDir, `slide-${index + 1}.html`);
    fs.writeFileSync(file, html);
    const { slide, placeholders } = await html2pptx(file, pptx, { tmpDir: path.join(root, 'data/raw/setup') });
    if (item.image) {
      const imagePath = path.join(output, item.image);
      const info = await sharp(imagePath).metadata();
      const box = placeholders.find(item => item.id === 'evidence');
      const ratio = Math.min(box.w / info.width, box.h / info.height);
      const w = info.width * ratio, h = info.height * ratio;
      slide.addImage({ path: imagePath, x: box.x + (box.w - w) / 2, y: box.y + (box.h - h) / 2, w, h });
    }
    slide.addNotes(item.notes);
  }
  await pptx.writeFile({ fileName: path.join(output, 'Slide_PhanDinh.pptx') });
}

async function makeReportPdf() {
  const source = fs.readFileSync(path.join(output, 'bao-cao-phan-dinh.md'), 'utf8').split(/\r?\n/);
  let html = '', list = false;
  for (const line of source) {
    if (!line.trim()) continue;
    if (!line.startsWith('- ') && list) { html += '</ul>'; list = false; }
    if (line.startsWith('- ')) {
      if (!list) { html += '<ul>'; list = true; }
      html += `<li>${escapeHtml(line.slice(2))}</li>`;
    } else if (line.startsWith('### ')) html += `<h3>${escapeHtml(line.slice(4))}</h3>`;
    else if (line.startsWith('## ')) html += `<h2>${escapeHtml(line.slice(3))}</h2>`;
    else if (line.startsWith('# ')) html += `<h1>${escapeHtml(line.slice(2))}</h1>`;
    else html += `<p>${escapeHtml(line)}</p>`;
  }
  if (list) html += '</ul>';
  html += '<h2>9. Hình kết quả</h2>';
  for (const [image, caption] of FIGURES) {
    const content = fs.readFileSync(path.join(figuresDir, image)).toString('base64');
    html += `<figure><img src="data:image/png;base64,${content}"><figcaption>${escapeHtml(caption)}</figcaption></figure>`;
  }
  const browser = await chromium.launch({ channel: 'chrome', headless: true });
  try {
    const page = await browser.newPage();
    await page.setContent(`<html lang="vi"><meta charset="utf-8"><style>@page{size:A4;margin:20mm}body{font-family:Arial;font-size:11.5pt;line-height:1.4;color:#172331}h1{font-size:24pt;color:#17324d}h2{font-size:16pt;margin-top:18pt}h3{font-size:13pt}h1,h2,h3{break-after:avoid}p,li{orphans:3;widows:3;overflow-wrap:anywhere}figure{break-before:page;margin:0;text-align:center}img{max-width:100%;max-height:235mm}figcaption{font-size:10pt;color:#566779}</style>${html}</html>`);
    await page.pdf({ path: path.join(output, 'BaoCao_PhanDinh_XemNhanh.pdf'), format: 'A4', printBackground: true, displayHeaderFooter: true,
      headerTemplate: '<span></span>', footerTemplate: '<div style="font-family:Arial;font-size:9px;text-align:center;width:100%">Định · Log Anomaly Detection · <span class="pageNumber"></span> / <span class="totalPages"></span></div>' });
  } finally { await browser.close(); }
}
(async () => {
  if (!process.argv.includes('--pdf-only')) { await makeWord(); await makeSlides(); }
  await makeReportPdf();
  console.log('DOCUMENT BUILD OK');
})().catch(error => { console.error(error); process.exitCode = 1; });
