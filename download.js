const axios = require("axios");
const { XMLParser } = require("fast-xml-parser");
const fs = require("fs");
const path = require("path");
const { pipeline } = require("stream/promises");

// 配置
const BASE_URL = "https://storage.sekai.best/sekai-jp-assets";
const PREFIX = "scenario/background/";
const OUTPUT_DIR = path.join(__dirname, "downloads");
const CONCURRENCY = 15; // 并发下载数
const MAX_RETRIES = 3; // 最大重试次数
const MAX_KEYS = 500; // 每次 API 请求返回的最大数量

// 命令行参数：--limit <数量> 限制下载数量（用于测试）
const limitArg = process.argv.indexOf("--limit");
const DOWNLOAD_LIMIT =
  limitArg !== -1 ? parseInt(process.argv[limitArg + 1], 10) : Infinity;

const parser = new XMLParser({
  isArray: (name) => ["CommonPrefixes", "Contents"].includes(name),
});

/**
 * 调用 S3 ListObjectsV2 API 获取目录内容
 */
async function listObjects(prefix, continuationToken) {
  const params = {
    "list-type": "2",
    delimiter: "/",
    "max-keys": MAX_KEYS,
    prefix,
  };
  if (continuationToken) {
    params["continuation-token"] = continuationToken;
  }

  const response = await axios.get(`${BASE_URL}/`, {
    params,
    responseType: "text",
  });

  return parser.parse(response.data).ListBucketResult;
}

/**
 * 获取指定前缀下的所有子目录（支持分页）
 */
async function getAllSubDirs(prefix) {
  const dirs = [];
  let token = undefined;

  do {
    const result = await listObjects(prefix, token);
    if (result.CommonPrefixes) {
      for (const cp of result.CommonPrefixes) {
        dirs.push(cp.Prefix);
      }
    }
    token = result.NextContinuationToken;
  } while (token);

  return dirs;
}

/**
 * 获取指定目录下的所有 PNG 文件
 */
async function getPngFilesInDir(prefix) {
  const files = [];
  let token = undefined;

  do {
    const result = await listObjects(prefix, token);
    if (result.Contents) {
      for (const content of result.Contents) {
        if (content.Key.endsWith(".png")) {
          files.push(content.Key);
        }
      }
    }
    token = result.NextContinuationToken;
  } while (token);

  return files;
}

/**
 * 获取所有背景图 PNG 文件列表（并发扫描目录）
 */
async function getAllPngFiles() {
  console.log("[1/3] 正在获取背景图目录列表...");
  const subDirs = await getAllSubDirs(PREFIX);
  console.log(`   找到 ${subDirs.length} 个子目录`);

  console.log("[2/3] 正在并发扫描每个目录中的 PNG 文件...");
  const allFiles = [];
  let scanned = 0;
  const total = subDirs.length;
  const SCAN_CONCURRENCY = 10; // 目录扫描并发数

  function printScanProgress() {
    process.stdout.write(
      `\r   已扫描 ${scanned}/${total} 个目录，找到 ${allFiles.length} 个 PNG 文件`,
    );
  }

  // 并发池扫描目录
  const pool = [];
  let dirIndex = 0;

  function nextScan() {
    if (dirIndex >= total) return Promise.resolve();
    const currentDir = subDirs[dirIndex++];

    return getPngFilesInDir(currentDir).then((files) => {
      allFiles.push(...files);
      scanned++;
      if (scanned % 20 === 0 || scanned === total) {
        printScanProgress();
      }
      return nextScan();
    });
  }

  for (let i = 0; i < Math.min(SCAN_CONCURRENCY, total); i++) {
    pool.push(nextScan());
  }
  await Promise.all(pool);

  printScanProgress();
  console.log(""); // 换行

  return allFiles;
}

// 下载逻辑

/**
 * 下载单个文件（支持重试）
 */
async function downloadFile(fileKey, retries = 0) {
  const fileName = path.basename(fileKey);
  const filePath = path.join(OUTPUT_DIR, fileName);

  // 跳过已下载的文件
  if (fs.existsSync(filePath)) {
    return { status: "skipped", file: fileName };
  }

  try {
    const url = `${BASE_URL}/${fileKey}`;
    const response = await axios.get(url, { responseType: "stream" });

    // 使用临时文件写入，完成后再重命名（避免部分下载的文件）
    const tmpPath = filePath + ".tmp";
    const writer = fs.createWriteStream(tmpPath);
    await pipeline(response.data, writer);
    fs.renameSync(tmpPath, filePath);

    return { status: "downloaded", file: fileName };
  } catch (err) {
    if (retries < MAX_RETRIES) {
      // 等待一段时间后重试
      await new Promise((r) => setTimeout(r, 1000 * (retries + 1)));
      return downloadFile(fileKey, retries + 1);
    }
    return { status: "failed", file: fileName, error: err.message };
  }
}

/**
 * 并发控制的批量下载
 */
async function downloadAll(fileKeys) {
  let completed = 0;
  let downloaded = 0;
  let skipped = 0;
  let failed = 0;
  const total = fileKeys.length;
  const failedFiles = [];

  function printProgress() {
    process.stdout.write(
      `\r  进度: ${completed}/${total} | 下载: ${downloaded} | 跳过: ${skipped} | 失败: ${failed}`,
    );
  }

  // 使用简单的并发池
  const pool = [];
  let index = 0;

  function next() {
    if (index >= total) return Promise.resolve();
    const currentIndex = index++;
    const key = fileKeys[currentIndex];

    return downloadFile(key).then((result) => {
      completed++;
      if (result.status === "downloaded") downloaded++;
      else if (result.status === "skipped") skipped++;
      else if (result.status === "failed") {
        failed++;
        failedFiles.push(result);
      }
      printProgress();
      return next();
    });
  }

  // 启动并发下载
  for (let i = 0; i < Math.min(CONCURRENCY, total); i++) {
    pool.push(next());
  }

  await Promise.all(pool);
  console.log(""); // 换行

  return { downloaded, skipped, failed, failedFiles };
}

// 主流程

async function main() {
  console.log("========================================");
  console.log("  Sekai 背景图批量下载工具");
  console.log("========================================\n");

  // 1. 创建输出目录
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  // 2. 获取所有 PNG 文件列表
  const allFiles = await getAllPngFiles();
  console.log(`\n共找到 ${allFiles.length} 个 PNG 文件`);

  // 3. 应用数量限制
  const filesToDownload =
    DOWNLOAD_LIMIT < Infinity ? allFiles.slice(0, DOWNLOAD_LIMIT) : allFiles;

  if (DOWNLOAD_LIMIT < Infinity) {
    console.log(`[注意] 限制下载数量: ${DOWNLOAD_LIMIT}`);
  }

  console.log(`下载目录: ${OUTPUT_DIR}`);
  console.log(`并发数: ${CONCURRENCY}\n`);

  // 4. 开始下载
  console.log("开始下载...\n");
  const startTime = Date.now();
  const result = await downloadAll(filesToDownload);
  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

  // 5. 输出结果
  console.log("\n========================================");
  console.log("  下载完成!");
  console.log("========================================");
  console.log(`  耗时: ${elapsed}s`);
  console.log(`  成功下载: ${result.downloaded}`);
  console.log(`  已跳过: ${result.skipped}`);
  console.log(`  下载失败: ${result.failed}`);

  if (result.failedFiles.length > 0) {
    console.log("\n失败文件列表:");
    for (const f of result.failedFiles) {
      console.log(`  - ${f.file}: ${f.error}`);
    }
  }
}

function waitForExit() {
  return new Promise((resolve) => {
    console.log("\n按回车键退出...");
    process.stdin.once("data", resolve);
  });
}

main()
  .catch((err) => {
    console.error("\n发生错误:", err.message);
  })
  .finally(() => waitForExit().then(() => process.exit()));
