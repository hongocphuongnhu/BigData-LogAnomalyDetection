// Ghep bao cao nhom tu doc/bao-cao-nhom.md ra doc/BaoCao_Nhom.docx theo quy dinh trinh bay cua Khoa CNTT HUIT
// (Mau 4): Unicode co 13, gian dong 1,5, le tren 3,5 / duoi 3 / trai 3,5 / phai 2 cm, so trang giua cuoi trang.
// Bia, header ten chuong, co chu tieu de/chu thich/muc luc lay theo bao cao mau cua nhom (DiepVien201.docx).
// Muc luc, danh muc bang, danh muc hinh la field TOC; chay scripts/update_docx_fields.ps1 de Word dien so trang.
const fs = require('fs');
const path = require('path');
const tools = path.resolve(__dirname, '../data/raw/tools/node_modules');
const {
  Document, Packer, Paragraph, TextRun, Tab, TabStopType, HeadingLevel, TableOfContents, Footer, PageNumber,
  AlignmentType, ImageRun, LevelFormat, Table, TableRow, TableCell, WidthType, ShadingType, BorderStyle,
  NumberFormat, SectionType, HighlightColor, VerticalAlign, PageBorderDisplay, PageBorderOffsetFrom, Header,
  LeaderType,
} = require(path.join(tools, 'docx'));
const sharp = require(path.join(tools, 'sharp'));

const root = path.resolve(__dirname, '..');
const docDir = path.join(root, 'doc');
const SOURCE = path.join(docDir, 'bao-cao-nhom.md');
const OUTPUT = path.join(docDir, 'BaoCao_Nhom.docx');

const cm = value => Math.round(value * 567);
const PAGE = { width: 11906, height: 16838 };
const MARGIN = { top: cm(3.5), bottom: cm(3), left: cm(3.5), right: cm(2) };
const TEXT_WIDTH = PAGE.width - MARGIN.left - MARGIN.right;
const FONT = 'Times New Roman';
const SIZE = 26;
const LINE = 360;
const FIRST_LINE = 720;
const LOGO = path.join(docDir, 'images', 'logo_huit.png');

// Danh muc xep ABC theo Mau 4: tac gia nuoc ngoai theo ho, khong co tac gia thi theo ten to chuc.
// So [n] trong than bai sinh tu thu tu nay, nen them tai lieu chi can chen dung vi tri ABC.
// Phan { i } in nghieng: ten tap chi, ten ky yeu, ten trang/tai lieu doc lap.
const ACCESSED = ', truy cập ngày 29/10/2026.';
const REFS = [
  ['httpd_logs', ['Apache Software Foundation, ', { i: 'Apache HTTP Server Version 2.4 Documentation: Log Files' }, ', https://httpd.apache.org/docs/2.4/logs.html' + ACCESSED]],
  ['parquet', ['Apache Software Foundation, ', { i: 'Apache Parquet Documentation: Overview' }, ', https://parquet.apache.org/docs/overview/' + ACCESSED]],
  ['hdfs_arch', ['Apache Software Foundation, ', { i: 'Hadoop 3.2.1 Documentation: HDFS Architecture' }, ', https://hadoop.apache.org/docs/r3.2.1/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html' + ACCESSED]],
  ['spark_clustering', ['Apache Software Foundation, ', { i: 'Spark 3.5.8 Documentation: Clustering' }, ', https://spark.apache.org/docs/3.5.8/ml-clustering.html' + ACCESSED]],
  ['spark_features', ['Apache Software Foundation, ', { i: 'Spark 3.5.8 Documentation: Extracting, transforming and selecting features' }, ', https://spark.apache.org/docs/3.5.8/ml-features.html' + ACCESSED]],
  ['spark_ss', ['Apache Software Foundation, ', { i: 'Spark 3.5.8 Documentation: Structured Streaming Programming Guide' }, ', https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html' + ACCESSED]],
  ['armbrust2018', ['Armbrust M., Das T., Torres J., Yavuz B., Zhu S., Xin R., Ghodsi A., Stoica I., Zaharia M. (2018), "Structured Streaming: A declarative API for real-time applications in Apache Spark", ', { i: 'Proceedings of the 2018 International Conference on Management of Data (SIGMOD \'18)' }, ', pp. 601-613. DOI: 10.1145/3183713.3190664.']],
  ['bahmani2012', ['Bahmani B., Moseley B., Vattani A., Kumar R., Vassilvitskii S. (2012), "Scalable k-means++", ', { i: 'Proceedings of the VLDB Endowment' }, ' 5 (7), pp. 622-633. DOI: 10.14778/2180912.2180915.']],
  ['chandola2009', ['Chandola V., Banerjee A., Kumar V. (2009), "Anomaly detection: A survey", ', { i: 'ACM Computing Surveys' }, ' 41 (3), pp. 1-58. DOI: 10.1145/1541880.1541882.']],
  ['medallion', ['Databricks, ', { i: 'What is a medallion architecture?' }, ', https://www.databricks.com/glossary/medallion-architecture' + ACCESSED]],
  ['davis2006', ['Davis J., Goadrich M. (2006), "The relationship between Precision-Recall and ROC curves", ', { i: 'Proceedings of the 23rd International Conference on Machine Learning (ICML \'06)' }, ', pp. 233-240. DOI: 10.1145/1143844.1143874.']],
  ['es_index', ['Elastic, ', { i: 'Elasticsearch Guide 8.19: Index API' }, ', https://www.elastic.co/guide/en/elasticsearch/reference/8.19/docs-index_.html' + ACCESSED]],
  ['es_keyword', ['Elastic, ', { i: 'Elasticsearch Guide 8.19: Keyword type family' }, ', https://www.elastic.co/guide/en/elasticsearch/reference/8.19/keyword.html' + ACCESSED]],
  ['es_mapping', ['Elastic, ', { i: 'Elasticsearch Guide 8.19: Mapping' }, ', https://www.elastic.co/guide/en/elasticsearch/reference/8.19/mapping.html' + ACCESSED]],
  ['es_hadoop', ['Elastic, ', { i: 'Elasticsearch for Apache Hadoop 8.19: Apache Spark support' }, ', https://www.elastic.co/guide/en/elasticsearch/hadoop/8.19/spark.html' + ACCESSED]],
  ['kb_dataview', ['Elastic, ', { i: 'Kibana Guide 8.19: Create a data view' }, ', https://www.elastic.co/guide/en/kibana/8.19/data-views.html' + ACCESSED]],
  ['kb_esquery', ['Elastic, ', { i: 'Kibana Guide 8.19: Create an Elasticsearch query rule' }, ', https://www.elastic.co/guide/en/kibana/8.19/rule-type-es-query.html' + ACCESSED]],
  ['kb_lens', ['Elastic, ', { i: 'Kibana Guide 8.19: Create visualizations with Lens' }, ', https://www.elastic.co/guide/en/kibana/8.19/lens.html' + ACCESSED]],
  ['kb_alerting', ['Elastic, ', { i: 'Elastic Docs: Set up Kibana alerting' }, ', https://www.elastic.co/docs/explore-analyze/alerting/alerts/alerting-setup' + ACCESSED]],
  ['rfc9110', ['Fielding R., Nottingham M., Reschke J. (2022), ', { i: 'HTTP Semantics, RFC 9110' }, ', Internet Engineering Task Force, https://www.rfc-editor.org/rfc/rfc9110.html.']],
  ['he2021', ['He S., He P., Chen Z., Yang T., Su Y., Lyu M.R. (2021), "A survey on automated log analysis for reliability engineering", ', { i: 'ACM Computing Surveys' }, ' 54 (6), pp. 1-37. DOI: 10.1145/3460345.']],
  ['he2003', ['He Z., Xu X., Deng S. (2003), "Discovering cluster-based local outliers", ', { i: 'Pattern Recognition Letters' }, ' 24 (9-10), pp. 1641-1650. DOI: 10.1016/S0167-8655(03)00003-5.']],
  ['nasa', ['Internet Traffic Archive, ', { i: 'NASA-HTTP: Two months of HTTP requests to the NASA Kennedy Space Center WWW server' }, ', https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html' + ACCESSED]],
  ['liu2008', ['Liu F.T., Ting K.M., Zhou Z.-H. (2008), "Isolation Forest", ', { i: '2008 Eighth IEEE International Conference on Data Mining' }, ', pp. 413-422. DOI: 10.1109/ICDM.2008.17.']],
  ['lloyd1982', ['Lloyd S. (1982), "Least squares quantization in PCM", ', { i: 'IEEE Transactions on Information Theory' }, ' 28 (2), pp. 129-137. DOI: 10.1109/TIT.1982.1056489.']],
  ['owasp_brute', ['OWASP Foundation, ', { i: 'Brute Force Attack' }, ', https://owasp.org/www-community/attacks/Brute_force_attack' + ACCESSED]],
  ['owasp_dos', ['OWASP Foundation, ', { i: 'Denial of Service' }, ', https://owasp.org/www-community/attacks/Denial_of_Service' + ACCESSED]],
  ['owasp_forced', ['OWASP Foundation, ', { i: 'Forced Browsing' }, ', https://owasp.org/www-community/attacks/Forced_browsing' + ACCESSED]],
  ['owasp_sqli', ['OWASP Foundation, ', { i: 'SQL Injection' }, ', https://owasp.org/www-community/attacks/SQL_Injection' + ACCESSED]],
  ['rousseeuw1987', ['Rousseeuw P.J. (1987), "Silhouettes: A graphical aid to the interpretation and validation of cluster analysis", ', { i: 'Journal of Computational and Applied Mathematics' }, ' 20, pp. 53-65. DOI: 10.1016/0377-0427(87)90125-7.']],
  ['shvachko2010', ['Shvachko K., Kuang H., Radia S., Chansler R. (2010), "The Hadoop Distributed File System", ', { i: '2010 IEEE 26th Symposium on Mass Storage Systems and Technologies (MSST)' }, ', pp. 1-10. DOI: 10.1109/MSST.2010.5496972.']],
  ['zaharia2016', ['Zaharia M., Xin R.S., Wendell P., Das T., Armbrust M., Dave A., Meng X., Rosen J., Venkataraman S., Franklin M.J., Ghodsi A., Gonzalez J., Shenker S., Stoica I. (2016), "Apache Spark: A unified engine for big data processing", ', { i: 'Communications of the ACM' }, ' 59 (11), pp. 56-65. DOI: 10.1145/2934664.']],
];
const REF_NUMBER = new Map(REFS.map(([key], index) => [key, index + 1]));

const ABBR = [
  ['API', 'Application Programming Interface, giao diện lập trình'],
  ['CBLOF', 'Cluster-Based Local Outlier Factor'],
  ['CLF', 'Common Log Format, định dạng access log của Apache'],
  ['DDoS', 'Distributed Denial of Service, tấn công từ chối dịch vụ phân tán'],
  ['F1', 'Trung bình điều hòa của precision và recall'],
  ['FN, FP, TN, TP', 'False negative, false positive, true negative, true positive'],
  ['FPR', 'False positive rate, tỷ lệ báo nhầm'],
  ['HDFS', 'Hadoop Distributed File System'],
  ['IP', 'Internet Protocol, ở đây là địa chỉ client'],
  ['JSONL', 'JSON Lines, mỗi dòng một đối tượng JSON'],
  ['MLlib', 'Thư viện học máy của Apache Spark'],
  ['PCA', 'Principal Component Analysis, phân tích thành phần chính'],
  ['PR-AUC', 'Diện tích dưới đường precision–recall'],
  ['SQLi', 'SQL injection, chèn câu lệnh SQL'],
  ['UTC', 'Coordinated Universal Time, giờ quốc tế'],
];

const cited = new Set();

// Tach trich dan [@a; @b] va o can dien [[...]]; nhieu nguon xep tang dan, moi so mot ngoac nhu Mau 4.
function inline(text, base = {}) {
  const runs = [];
  const pattern = /\[@([^\]]+)\]|\[\[([^\]]*)\]\]/g;
  let last = 0;
  for (const match of text.matchAll(pattern)) {
    if (match.index > last) runs.push(new TextRun({ ...base, text: text.slice(last, match.index) }));
    if (match[1]) {
      const numbers = match[1].split(';').map(item => item.trim().replace(/^@/, '')).map(key => {
        if (!REF_NUMBER.has(key)) throw new Error(`Trich dan khong co trong danh muc: ${key}`);
        cited.add(key);
        return REF_NUMBER.get(key);
      }).sort((a, b) => a - b);
      runs.push(new TextRun({ ...base, text: numbers.map(n => `[${n}]`).join(', ') }));
    } else {
      runs.push(new TextRun({ ...base, text: match[2], highlight: HighlightColor.YELLOW }));
    }
    last = match.index + match[0].length;
  }
  if (last < text.length) runs.push(new TextRun({ ...base, text: text.slice(last) }));
  return runs;
}

const body = (text, extra = {}) => new Paragraph({
  alignment: AlignmentType.JUSTIFIED, indent: { firstLine: FIRST_LINE }, spacing: { line: LINE, after: 120 },
  children: inline(text), ...extra,
});

function heading(text, level, pageBreak) {
  const map = { 1: HeadingLevel.HEADING_1, 2: HeadingLevel.HEADING_2, 3: HeadingLevel.HEADING_3 };
  // Bao cao mau danh so muc co dau cham cuoi: "1.1." va "3.2.1.".
  const label = level > 1 ? text.replace(/^(\d+(?:\.\d+)+)\s/, '$1. ') : text;
  return new Paragraph({ heading: map[level], pageBreakBefore: pageBreak, children: [new TextRun(label)] });
}

// Tieu de phan dau (loi cam doan, muc luc, danh muc): co 18 nhu style Title cua bao cao mau,
// co outline level 0 de vao muc luc ma khong bi tinh la chuong.
const frontTitle = (text, pageBreak) => new Paragraph({ style: 'FrontTitle', pageBreakBefore: pageBreak, children: [new TextRun(text)] });

// Khoi ky ten lech phai: dong dau dam, dong bat dau "_" in nghieng, con lai la ten.
function signature(line) {
  return line.slice(7).split(';').map(item => item.trim()).map((text, index) => new Paragraph({
    alignment: AlignmentType.CENTER, indent: { left: cm(8) }, spacing: { line: LINE, after: index === 0 ? 60 : 0, before: index === 0 ? 240 : 0 },
    children: [new TextRun({ text: text.replace(/^_/, ''), bold: index === 0, italics: text.startsWith('_') })],
  }));
}

function equation(line) {
  const [formula, number] = line.slice(2).split('||').map(item => item.trim());
  return new Paragraph({
    spacing: { line: LINE, before: 60, after: 120 }, keepLines: true,
    tabStops: [{ type: TabStopType.CENTER, position: Math.round(TEXT_WIDTH / 2) }, { type: TabStopType.RIGHT, position: TEXT_WIDTH }],
    children: [new TextRun({ children: [new Tab(), formula, new Tab(), number] })],
  });
}

async function figure(line) {
  const [, caption, rel] = line.match(/^!\[(.+)\]\((.+)\)$/);
  const file = path.resolve(docDir, rel);
  if (!fs.existsSync(file)) throw new Error(`Thieu hinh: ${rel}`);
  const info = await sharp(file).metadata();
  // 1 px = 15 twip o 96 dpi; gioi han chieu cao de hinh va chu thich nam chung mot trang.
  let width = Math.min(TEXT_WIDTH / 15 * 0.85, info.width);
  let height = width * info.height / info.width;
  if (height > 380) { height = 380; width = height * info.width / info.height; }
  return [
    new Paragraph({ alignment: AlignmentType.CENTER, keepNext: true, spacing: { before: 120 },
      children: [new ImageRun({ type: 'png', data: fs.readFileSync(file), transformation: { width, height },
        altText: { title: caption, description: caption, name: path.basename(file) } })] }),
    new Paragraph({ style: 'HinhCaption', children: inline(caption) }),
  ];
}

function table(caption, rows) {
  const cells = rows.map(row => row.replace(/^\||\|$/g, '').split('|').map(cell => cell.trim()));
  const columns = cells[0].length;
  // Do rong cot theo do dai chu, co san toi thieu de cot ngan khong bi ep xuong dong tung chu.
  const widths = Array.from({ length: columns }, (_, c) => Math.min(45, Math.max(cells[0][c].length + 2, 12, ...cells.map(r => Math.min(45, (r[c] || '').length)))));
  const total = widths.reduce((a, b) => a + b, 0);
  const columnWidths = widths.map(w => Math.floor(TEXT_WIDTH * w / total));
  columnWidths[columns - 1] += TEXT_WIDTH - columnWidths.reduce((a, b) => a + b, 0);
  const border = { style: BorderStyle.SINGLE, size: 4, color: '000000' };
  // O co nhieu y viet "<br>" thanh nhieu dong; dong bat dau "- " thut treo nhu gach dau dong cua bao cao mau.
  const cellParagraphs = (text, header) => text.split(/\s*<br>\s*/).map(part => {
    const dash = !header && part.startsWith('- ');
    return new Paragraph({
      alignment: header ? AlignmentType.CENTER : AlignmentType.LEFT,
      indent: dash ? { left: cm(0.6), hanging: cm(0.6) } : undefined,
      spacing: { line: 276, after: 0 },
      children: inline(dash ? `-\t${part.slice(2)}` : part, { size: 24, bold: header }),
      tabStops: dash ? [{ type: TabStopType.LEFT, position: cm(0.6) }] : undefined,
    });
  });
  const tableRows = cells.map((row, r) => new TableRow({
    tableHeader: r === 0, cantSplit: true,
    children: row.map((text, c) => new TableCell({
      width: { size: columnWidths[c], type: WidthType.DXA }, verticalAlign: VerticalAlign.CENTER,
      margins: { top: 40, bottom: 40, left: 80, right: 80 },
      borders: { top: border, bottom: border, left: border, right: border },
      children: cellParagraphs(text, r === 0),
    })),
  }));
  const numbered = /^Bảng \d/.test(caption);
  return [
    // "-" la bang khong chu thich (bang phan cong cong viec); slice(1) cua ::abbr van bo dung phan tu dau.
    caption === '-' ? new Paragraph({ spacing: { after: 0 }, children: [] })
      : new Paragraph({ style: numbered ? 'BangCaption' : 'CaptionPlain', keepNext: true, children: inline(caption) }),
    new Table({ width: { size: TEXT_WIDTH, type: WidthType.DXA }, columnWidths, rows: tableRows }),
    new Paragraph({ spacing: { after: 120 }, children: [] }),
  ];
}

// Thong tin bia nhom da dien tay trong Word truoc day; giu o day de build lai khong mat.
const COVER = {
  course: 'NHẬP MÔN BIG DATA',
  topic: 'CHỦ ĐỀ: PHÂN TÍCH DỮ LIỆU NHẬT KÝ HỆ THỐNG ĐỂ PHÁT HIỆN BẤT THƯỜNG',
  lecturer: 'Ngô Dương Hà',
  classCode: '14DHTH04 - 010110197108',
  members: [['Nguyễn Xuân Định', '2001230178'], ['Hồ Ngọc Phương Như', '2001230629']],
  date: 'Thành phố Hồ Chí Minh, tháng 11 năm 2026',
};

// Bia mot trang theo bao cao mau: khung hoa van, logo HUIT o giua, GVHD va thanh vien lech phai.
function cover() {
  const center = (text, run = {}, spacing = {}) => new Paragraph({ alignment: AlignmentType.CENTER,
    spacing: { line: LINE, after: 0, ...spacing }, children: inline(text, run) });
  const info = (text, extra = {}) => new Paragraph({ indent: { left: cm(5.5) }, spacing: { line: LINE, after: 0, before: 60 },
    ...extra, children: inline(text, { size: 28, bold: true }) });
  return [
    center('BỘ CÔNG THƯƠNG'),
    center('TRƯỜNG ĐẠI HỌC CÔNG THƯƠNG THÀNH PHỐ HỒ CHÍ MINH', { bold: true }),
    center('KHOA CÔNG NGHỆ THÔNG TIN', { bold: true }),
    center('ϰϰϰ', { bold: true }),
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 120, after: 240 },
      children: [new ImageRun({ type: 'png', data: fs.readFileSync(LOGO), transformation: { width: 142, height: 142 },
        altText: { title: 'Logo HUIT', description: 'Logo Trường Đại học Công Thương TP. HCM', name: 'logo_huit.png' } })] }),
    center('BÁO CÁO MÔN HỌC', { size: 48, bold: true }, { after: 120 }),
    center(COVER.course, { size: 40, bold: true }, { after: 60 }),
    center(COVER.topic, { size: 40, bold: true }, { after: 600 }),
    info(`GVHD: ${COVER.lecturer}`),
    info(`Mã lớp HP: ${COVER.classCode}`),
    info('Nhóm thực hiện:'),
    // Ghi de indent cua info(), neu khong so thu tu bi day lui ra truoc dong GVHD.
    ...COVER.members.map(([name, id]) => info(`${name} – ${id}`, { numbering: { reference: 'members', level: 0 },
      indent: { left: cm(6.2), hanging: cm(0.7) } })),
    center(COVER.date, { bold: true, italics: true }, { before: 1000 }),
  ];
}


function references() {
  const uncited = REFS.filter(([key]) => !cited.has(key)).map(([key]) => key);
  // Mau 4: chi liet ke tai lieu co trich dan trong bai.
  if (uncited.length) throw new Error(`Tai lieu khong duoc trich: ${uncited.join(', ')}`);
  const out = [new Paragraph({ spacing: { line: LINE, after: 120 }, children: [new TextRun({ text: 'Tiếng Anh', bold: true })] })];
  REFS.forEach(([, parts], index) => {
    const runs = [new TextRun(`[${index + 1}]. `)];
    for (const part of parts) runs.push(typeof part === 'string' ? new TextRun(part) : new TextRun({ text: part.i, italics: true }));
    out.push(new Paragraph({ alignment: AlignmentType.LEFT, spacing: { line: LINE, after: 80 },
      indent: { left: cm(1), hanging: cm(1) }, children: runs }));
  });
  return out;
}

async function build() {
  const source = fs.readFileSync(SOURCE, 'utf8').replace(/<!--[\s\S]*?-->/, '').split(/\r?\n/);
  const cover_ = [];
  const front = [];
  // Moi chuong la mot section rieng de header in dung ten chuong nhu bao cao mau.
  const chapters = [];
  let out = front;
  for (let i = 0; i < source.length; i++) {
    const line = source[i];
    if (!line.trim()) continue;
    if (line === '::cover') { cover_.push(...cover()); continue; }
    if (line === '::toc') {
      // \u dua tieu de phan dau (outline level 0) vao muc luc cung voi Heading 1-3.
      out.push(frontTitle('MỤC LỤC', out.length > 0),
        new TableOfContents('Mục lục', { hyperlink: true, headingStyleRange: '1-3', useAppliedParagraphOutlineLevel: true }));
      continue;
    }
    if (line === '::lot' || line === '::lof') {
      const lot = line === '::lot';
      // Muc 4 de Word dung style TOC 4 (chu thuong), khong lay kieu dam cua TOC 1.
      out.push(frontTitle(lot ? 'DANH MỤC BẢNG' : 'DANH MỤC HÌNH ẢNH', true),
        new TableOfContents(lot ? 'Danh mục bảng' : 'Danh mục hình', { hyperlink: true,
          stylesWithLevels: [{ styleName: lot ? 'BangCaption' : 'HinhCaption', level: 4 }] }));
      continue;
    }
    if (line === '::abbr') {
      out.push(frontTitle('DANH MỤC CHỮ VIẾT TẮT', true), ...table('CaptionPlain', ['| Chữ viết tắt | Nghĩa |', ...ABBR.map(([a, b]) => `| ${a} | ${b} |`)]).slice(1));
      continue;
    }
    if (line === '::refs') { out.push(...references()); continue; }
    if (line.startsWith('::sign ')) { out.push(...signature(line)); continue; }
    if (line.startsWith('# ') || (line.startsWith('#* ') && out !== front)) {
      const title = line.replace(/^#\*? /, '');
      chapters.push({ title, children: [heading(title, 1, false)] });
      out = chapters[chapters.length - 1].children;
      continue;
    }
    if (line.startsWith('#* ')) {
      out.push(frontTitle(line.slice(3), out.length > 0));
      continue;
    }
    if (line.startsWith('### ')) { out.push(heading(line.slice(4), 3, false)); continue; }
    if (line.startsWith('## ')) { out.push(heading(line.slice(3), 2, false)); continue; }
    if (line.startsWith('$$ ')) { out.push(equation(line)); continue; }
    if (line.startsWith('![')) { out.push(...await figure(line)); continue; }
    if (line.startsWith('TABLE ')) {
      const rows = [];
      while (source[i + 1] && source[i + 1].startsWith('|')) rows.push(source[++i]);
      out.push(...table(line.slice(6), rows));
      continue;
    }
    if (line.startsWith('- ')) {
      out.push(new Paragraph({ numbering: { reference: 'bullets', level: 0 }, alignment: AlignmentType.JUSTIFIED,
        spacing: { line: LINE, after: 60 }, children: inline(line.slice(2)) }));
      continue;
    }
    out.push(body(line));
  }

  const footer = () => new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [new TextRun({ children: [PageNumber.CURRENT] })] })] });
  // Header bao cao mau: ten chuong can trai, chu thuong khong dam; co 13 de ten chuong dai van nam tren mot dong.
  const header = title => new Header({ children: [new Paragraph({ style: 'Header', children: [new TextRun(title)] })] });
  const page = { size: PAGE, margin: MARGIN };
  // Vien hoa van "twistedLines1" chi o trang bia, giong bao cao mau.
  const art = space => ({ style: 'twistedLines1', size: 18, color: 'auto', space });
  const tocStyle = (level, run, left) => ({ id: `TOC${level}`, name: `toc ${level}`, basedOn: 'Normal', next: 'Normal',
    // after 60 thay vi 100 cua mau: muc luc vua khit 2 trang thi doan ket thuc field tran sang trang trang.
    run: { font: FONT, size: SIZE, ...run }, paragraph: { indent: { left, firstLine: 0 }, spacing: { after: 60 },
      tabStops: [{ type: TabStopType.RIGHT, position: TEXT_WIDTH, leader: LeaderType.DOT }] } });
  const doc = new Document({
    creator: 'Nguyễn Xuân Định, Hồ Ngọc Phương Như', title: 'Phân tích dữ liệu nhật ký hệ thống để phát hiện bất thường',
    styles: {
      default: { document: { run: { font: FONT, size: SIZE }, paragraph: { spacing: { line: LINE } } } },
      paragraphStyles: [
        { id: 'Heading1', name: 'Heading 1', basedOn: 'Normal', next: 'Normal', quickFormat: true, run: { font: FONT, size: 32, bold: true, color: '000000' },
          paragraph: { alignment: AlignmentType.CENTER, spacing: { before: 360, after: 240 }, keepNext: true, keepLines: true, outlineLevel: 0 } },
        { id: 'Heading2', name: 'Heading 2', basedOn: 'Normal', next: 'Normal', quickFormat: true, run: { font: FONT, size: 26, bold: true, color: '000000' },
          paragraph: { spacing: { before: 160, after: 80 }, keepNext: true, keepLines: true, outlineLevel: 1 } },
        { id: 'Heading3', name: 'Heading 3', basedOn: 'Normal', next: 'Normal', quickFormat: true, run: { font: FONT, size: 26, bold: true, color: '000000' },
          paragraph: { spacing: { before: 160, after: 80 }, keepNext: true, keepLines: true, outlineLevel: 2 } },
        { id: 'FrontTitle', name: 'Front Title', basedOn: 'Normal', next: 'Normal', run: { font: FONT, size: 36, bold: true },
          paragraph: { alignment: AlignmentType.CENTER, spacing: { before: 0, after: 240 }, keepNext: true, outlineLevel: 0 } },
        { id: 'Header', name: 'header', basedOn: 'Normal', run: { font: FONT, size: 26 },
          paragraph: { alignment: AlignmentType.LEFT, spacing: { line: 240, after: 0 } } },
        // Chu thich theo bao cao mau: bang nghieng co 11 o tren bang, hinh nghieng co 13 o duoi hinh.
        { id: 'BangCaption', name: 'BangCaption', basedOn: 'Normal', next: 'Normal', run: { font: FONT, size: 22, italics: true },
          paragraph: { alignment: AlignmentType.CENTER, spacing: { before: 120, after: 80 }, keepNext: true } },
        { id: 'HinhCaption', name: 'HinhCaption', basedOn: 'Normal', next: 'Normal', run: { font: FONT, size: 26, italics: true },
          paragraph: { alignment: AlignmentType.CENTER, spacing: { before: 60, after: 200, line: 240 } } },
        { id: 'CaptionPlain', name: 'CaptionPlain', basedOn: 'Normal', next: 'Normal', run: { font: FONT, size: 22, italics: true },
          paragraph: { alignment: AlignmentType.CENTER, spacing: { before: 120, after: 80 }, keepNext: true } },
        tocStyle(1, { bold: true }, 0),
        tocStyle(2, { bold: true, italics: true }, 260),
        tocStyle(3, {}, 576),
        tocStyle(4, {}, 0),
      ],
    },
    numbering: { config: [
      { reference: 'bullets', levels: [{ level: 0, format: LevelFormat.BULLET, text: '-', alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: cm(1.27), hanging: cm(0.63) } } } }] },
      { reference: 'members', levels: [{ level: 0, format: LevelFormat.DECIMAL, text: '%1.', alignment: AlignmentType.LEFT,
        style: { paragraph: { indent: { left: cm(6.2), hanging: cm(0.7) } } } }] },
      // so thu tu thanh vien nam o cm(5.5), thang hang voi dong GVHD
    ] },
    sections: [
      { properties: { page: { ...page, borders: { pageBorderTop: art(1), pageBorderBottom: art(1), pageBorderLeft: art(4), pageBorderRight: art(4),
          pageBorders: { display: PageBorderDisplay.FIRST_PAGE, offsetFrom: PageBorderOffsetFrom.TEXT } } } },
        children: cover_ },
      { properties: { type: SectionType.NEXT_PAGE, page: { ...page, pageNumbers: { start: 1, formatType: NumberFormat.LOWER_ROMAN } } },
        footers: { default: footer() }, children: front },
      ...chapters.map((chapter, index) => ({
        properties: { type: SectionType.NEXT_PAGE,
          page: { ...page, pageNumbers: index === 0 ? { start: 1, formatType: NumberFormat.DECIMAL } : { formatType: NumberFormat.DECIMAL } } },
        headers: { default: header(chapter.title) }, footers: { default: footer() }, children: chapter.children,
      })),
    ],
  });
  fs.writeFileSync(OUTPUT, await Packer.toBuffer(doc));
  console.log(JSON.stringify({ output: path.relative(root, OUTPUT), references: REFS.length, cited: cited.size }));
}

build().catch(error => { console.error(error); process.exitCode = 1; });
