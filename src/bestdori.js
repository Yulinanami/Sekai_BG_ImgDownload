const path = require("path");
const readline = require("readline");
const { downloadAll } = require("./common");

// Bestdori 配置
const BASE_URL = "https://bestdori.com/assets/jp/bg";
const KNOWN_PLACEHOLDER_SIZES = new Set([14084]);
const DEFAULT_START = 0;
const DEFAULT_END = 391;

// 拼出文件名，比如 bg00010.png
function buildFilename(scenarioNumber, lastDigit) {
  const scenStr = String(scenarioNumber).padStart(3, "0");
  return `bg0${scenStr}${lastDigit}.png`;
}

// 拼出下载链接
function buildUrl(scenarioNumber, filename) {
  return `${BASE_URL}/scenario${scenarioNumber}_rip/${filename}`;
}

// 读取用户输入
function askQuestion(rl, prompt) {
  return new Promise((resolve) => rl.question(prompt, resolve));
}

// 让用户输入起止编号
async function promptRange(rl) {
  const startRaw = await askQuestion(rl, `请输入起始 scenario 编号（回车使用默认值 ${DEFAULT_START}）: `);
  let start = startRaw.trim() ? parseInt(startRaw.trim(), 10) : DEFAULT_START;
  if (isNaN(start) || start < 0) {
    console.log(`输入无效，使用默认值 ${DEFAULT_START}`);
    start = DEFAULT_START;
  }

  const endRaw = await askQuestion(rl, `请输入结束 scenario 编号（回车使用默认值 ${DEFAULT_END}）: `);
  let end = endRaw.trim() ? parseInt(endRaw.trim(), 10) : DEFAULT_END;
  if (isNaN(end) || end < 0) {
    console.log(`输入无效，使用默认值 ${DEFAULT_END}`);
    end = DEFAULT_END;
  }

  if (start > end) {
    [start, end] = [end, start];
    console.log(`起始大于结束，已交换为 ${start} - ${end}`);
  }

  return { start, end };
}

// 根据编号范围，列出所有要下载的文件
function buildFileList(start, end, outputDir, splitByScenario) {
  const tasks = [];
  for (let scen = start; scen <= end; scen++) {
    for (let d = 0; d <= 9; d++) {
      const filename = buildFilename(scen, d);
      const url = buildUrl(scen, filename);
      const filePath = splitByScenario
        ? path.join(outputDir, `scenario${scen}`, filename)
        : path.join(outputDir, filename);
      tasks.push({ url, filePath });
    }
  }
  return tasks;
}

// Bestdori 下载主流程
async function run(outputDir) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });

  try {
    console.log("按命名规则下载 Bestdori scenario 背景图（无需扫描网页）");
    console.log(`默认起止为 ${DEFAULT_START}-${DEFAULT_END}，可按提示输入覆盖。`);
    console.log(`已启用占位过滤：长度 ${[...KNOWN_PLACEHOLDER_SIZES]} bytes。`);

    const { start, end } = await promptRange(rl);

    const choiceRaw = await askQuestion(rl, "按 scenario 分目录保存? (回车默认关闭，输入Y/y开启): ");
    const splitByScenario = choiceRaw.trim().toLowerCase() === "y";

    rl.close();

    const fileTasks = buildFileList(start, end, outputDir, splitByScenario);

    console.log(`\n准备下载 scenario ${start}-${end}，每个尝试文件 bg0###(0-9).png`);
    console.log(`输出目录: ${outputDir}`);
    console.log(`按 scenario 分目录: ${splitByScenario}`);
    console.log("\n开始下载...\n");

    const startTime = Date.now();
    const result = await downloadAll(fileTasks, {
      placeholderSizes: KNOWN_PLACEHOLDER_SIZES,
    });
    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

    console.log("\n下载完成!");
    console.log(`耗时: ${elapsed}s`);
    console.log(`成功下载: ${result.downloaded}`);
    console.log(`已跳过: ${result.skipped} (已存在 ${result.skippedExisting} | 空图 ${result.skippedPlaceholder})`);
    console.log(`下载失败: ${result.failed}`);

    if (result.failedFiles.length > 0) {
      console.log("\n失败文件列表:");
      for (const f of result.failedFiles) {
        console.log(`  - ${f.file}: ${f.error}`);
      }
    }
  } finally {
    if (!rl.closed) rl.close();
  }
}

module.exports = { run };
