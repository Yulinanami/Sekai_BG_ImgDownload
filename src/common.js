const axios = require("axios");
const cliProgress = require("cli-progress");
const fs = require("fs");
const https = require("https");
const path = require("path");
const { pipeline } = require("stream/promises");

// 配置
const CONCURRENCY = 8;
const MAX_RETRIES = 5;
const RETRY_FAILED_ROUNDS = 5;
const REQUEST_TIMEOUT = 30000;
const RETRY_CONCURRENCY = Math.max(3, Math.floor(CONCURRENCY / 3));
const PROGRESS_BAR_WIDTH = 24;

// 创建一个共用的请求客户端
const httpClient = axios.create({
  timeout: REQUEST_TIMEOUT,
  httpsAgent: new https.Agent({
    keepAlive: true,
    maxSockets: CONCURRENCY,
    maxFreeSockets: CONCURRENCY,
  }),
});

// 给文字加颜色
function colorize(code, text) {
  return `\u001b[${code}m${text}\u001b[0m`;
}

// 等一会儿
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// 创建进度条
function createProgressBar(format) {
  return new cliProgress.SingleBar({
    format,
    hideCursor: true,
    clearOnComplete: false,
    barsize: PROGRESS_BAR_WIDTH,
    barCompleteChar: "█",
    barIncompleteChar: "░",
  });
}

// 下载一个文件，失败了会自动重试
async function downloadFile(fileInfo, options = {}, retries = 0) {
  const { url, filePath, remoteSize } = fileInfo;
  const fileName = path.basename(filePath);
  const tmpPath = filePath + ".tmp";

  // 文件已经下载过了就跳过
  if (remoteSize && fs.existsSync(filePath)) {
    const localSize = fs.statSync(filePath).size;
    if (localSize > 0 && localSize === remoteSize) {
      return { status: "skipped", reason: "existing", file: fileName };
    }
  }

  try {
    const response = await httpClient.get(url, { responseType: "stream" });
    const contentLength = parseInt(response.headers["content-length"], 10);

    // 如果是空图（占位图）就跳过
    if (options.placeholderSizes && contentLength && options.placeholderSizes.has(contentLength)) {
      response.data.destroy();
      return { status: "skipped", reason: "placeholder" };
    }

    // 不知道文件大小的情况下，用响应头的大小来判断是否已存在
    if (!remoteSize && contentLength && fs.existsSync(filePath)) {
      const localSize = fs.statSync(filePath).size;
      if (localSize > 0 && localSize === contentLength) {
        response.data.destroy();
        return { status: "skipped", reason: "existing", file: fileName };
      }
    }

    // 目录不存在就创建
    const dir = path.dirname(filePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    // 先写到临时文件，下载完再改名
    const writer = fs.createWriteStream(tmpPath);
    await pipeline(response.data, writer);

    // 下载完后再检查一次是不是空图
    if (options.placeholderSizes) {
      const downloadedSize = fs.statSync(tmpPath).size;
      if (options.placeholderSizes.has(downloadedSize)) {
        fs.unlinkSync(tmpPath);
        return { status: "skipped", reason: "placeholder" };
      }
    }

    fs.renameSync(tmpPath, filePath);
    return { status: "downloaded" };
  } catch (err) {
    if (fs.existsSync(tmpPath)) {
      try {
        fs.unlinkSync(tmpPath);
      } catch {}
    }

    if (retries < MAX_RETRIES) {
      await sleep(1000 * (retries + 1) + Math.floor(Math.random() * 500));
      return downloadFile(fileInfo, options, retries + 1);
    }

    return {
      status: "failed",
      file: fileName,
      error: err.message,
    };
  }
}

// 把下载失败的文件重新试几轮
async function retryFailedFiles(failedItems, options = {}) {
  if (failedItems.length === 0) {
    return {
      downloaded: 0,
      skipped: 0,
      skippedExisting: 0,
      skippedPlaceholder: 0,
      failedFiles: [],
    };
  }

  let remaining = failedItems;
  let totalDownloaded = 0;
  let totalSkippedExisting = 0;
  let totalSkippedPlaceholder = 0;

  for (let round = 1; round <= RETRY_FAILED_ROUNDS; round++) {
    if (remaining.length === 0) break;

    await sleep(2000);

    let downloaded = 0;
    let skippedExisting = 0;
    let skippedPlaceholder = 0;
    const nextFailed = [];
    const pool = [];
    let index = 0;
    let completed = 0;

    const bar = createProgressBar(`${colorize(33, `  补重试 ${round}/${RETRY_FAILED_ROUNDS}: {bar}`)} ${colorize(33, "{percentage}%")} | ${colorize(33, "{value}/{total}")} | 下载: ${colorize(32, "{downloaded}")} | 跳过: ${colorize(34, "{skipped}")} | 失败: ${colorize(31, "{failed}")} | 用时 ${colorize(35, "{duration_formatted}")} | 剩余 ${colorize(36, "{eta_formatted}")}`);

    bar.start(remaining.length, 0, { downloaded: 0, skipped: 0, failed: 0 });

    // 取下一个失败文件重试
    function next() {
      if (index >= remaining.length) return Promise.resolve();
      const item = remaining[index++];
      const fileInfo = item._retry;

      return downloadFile(fileInfo, options).then((result) => {
        completed++;
        if (result.status === "downloaded") {
          downloaded++;
          totalDownloaded++;
        } else if (result.status === "skipped") {
          if (result.reason === "existing") {
            skippedExisting++;
            totalSkippedExisting++;
          } else if (result.reason === "placeholder") {
            skippedPlaceholder++;
            totalSkippedPlaceholder++;
          }
        } else {
          nextFailed.push({ ...result, _retry: fileInfo });
        }
        bar.update(completed, {
          downloaded,
          skipped: skippedExisting + skippedPlaceholder,
          failed: nextFailed.length,
        });
        return next();
      });
    }

    for (let i = 0; i < Math.min(RETRY_CONCURRENCY, remaining.length); i++) {
      pool.push(next());
    }
    await Promise.all(pool);
    bar.stop();

    remaining = nextFailed;
  }

  return {
    downloaded: totalDownloaded,
    skipped: totalSkippedExisting + totalSkippedPlaceholder,
    skippedExisting: totalSkippedExisting,
    skippedPlaceholder: totalSkippedPlaceholder,
    failedFiles: remaining,
  };
}

// 同时下载一批文件，最后汇总结果
async function downloadAll(fileTasks, options = {}) {
  let completed = 0;
  let downloaded = 0;
  let skipped = 0;
  let skippedExisting = 0;
  let skippedPlaceholder = 0;
  let failed = 0;
  const total = fileTasks.length;
  let failedFiles = [];

  const showPlaceholder = Boolean(options.placeholderSizes);
  const placeholderPart = showPlaceholder
    ? ` | 空图 ${colorize(36, "{skippedPlaceholder}")}`
    : "";

  const bar = createProgressBar(`${colorize(32, "  进度: {bar}")} ${colorize(33, "{percentage}%")} | ${colorize(33, "{value}/{total}")} | 下载: ${colorize(32, "{downloaded}")} | 跳过: ${colorize(34, "{skipped}")} (已存在 ${colorize(36, "{skippedExisting}")}${placeholderPart}) | 失败: ${colorize(31, "{failed}")} | 用时 ${colorize(35, "{duration_formatted}")} | 剩余 ${colorize(36, "{eta_formatted}")}`);

  if (total === 0) {
    console.log("没有需要下载的文件。");
    return {
      downloaded,
      skipped,
      skippedExisting,
      skippedPlaceholder,
      failed,
      failedFiles,
    };
  }

  bar.start(total, 0, {
    downloaded,
    skipped,
    skippedExisting,
    skippedPlaceholder,
    failed,
  });

  const pool = [];
  let index = 0;

  // 取下一个文件下载
  function next() {
    if (index >= total) return Promise.resolve();
    const fileInfo = fileTasks[index++];

    return downloadFile(fileInfo, options).then((result) => {
      completed++;
      if (result.status === "downloaded") downloaded++;
      else if (result.status === "skipped") {
        skipped++;
        if (result.reason === "existing") skippedExisting++;
        else if (result.reason === "placeholder") skippedPlaceholder++;
      } else {
        failed++;
        failedFiles.push({ ...result, _retry: fileInfo });
      }
      bar.update(completed, {
        downloaded,
        skipped,
        skippedExisting,
        skippedPlaceholder,
        failed,
      });
      return next();
    });
  }

  for (let i = 0; i < Math.min(CONCURRENCY, total); i++) {
    pool.push(next());
  }
  await Promise.all(pool);

  // 有失败的就重试
  if (failedFiles.length > 0) {
    console.log("\n开始统一重试失败文件...");
    const retryResult = await retryFailedFiles(failedFiles, options);
    downloaded += retryResult.downloaded;
    skipped += retryResult.skipped;
    skippedExisting += retryResult.skippedExisting;
    skippedPlaceholder += retryResult.skippedPlaceholder;
    failedFiles = retryResult.failedFiles;
    failed = failedFiles.length;
    bar.update(total, {
      downloaded,
      skipped,
      skippedExisting,
      skippedPlaceholder,
      failed,
    });
  }

  bar.stop();

  return {
    downloaded,
    skipped,
    skippedExisting,
    skippedPlaceholder,
    failed,
    failedFiles,
  };
}

// 等用户按回车再退出
function waitForExit() {
  return new Promise((resolve) => {
    console.log("\n按回车键退出...");
    const readline = require("readline");
    process.stdin.resume();
    const rl = readline.createInterface({ input: process.stdin });
    rl.once("line", () => {
      rl.close();
      resolve();
    });
  });
}

module.exports = {
  httpClient,
  colorize,
  sleep,
  createProgressBar,
  downloadAll,
  waitForExit,
};
