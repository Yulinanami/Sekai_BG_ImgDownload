# Sekai 背景图批量下载工具

从 [Sekai Viewer](https://sekai.best/asset_viewer/scenario/background) 批量下载 Project Sekai 的背景图（PNG 格式）。

## 原理

直接调用 Sekai Viewer 的 S3/MinIO 存储 API

## 使用方法

运行run.bat即可，或者自行按照以下步骤操作

### 安装依赖

```bash
npm install
```

### 下载所有背景图

```bash
node download.js
```

### 限制下载数量（测试用）

```bash
node download.js --limit 10
```

图片保存在 `./downloads/` 目录下。

## 配置

编辑 `download.js` 顶部的常量即可修改：

| 参数          | 默认值        | 说明         |
| ------------- | ------------- | ------------ |
| `CONCURRENCY` | 15            | 下载并发数   |
| `MAX_RETRIES` | 3             | 失败重试次数 |
| `OUTPUT_DIR`  | `./downloads` | 输出目录     |

## 技术栈

- [axios](https://github.com/axios/axios) — HTTP 请求
- [fast-xml-parser](https://github.com/NaturalIntelligence/fast-xml-parser) — S3 XML 响应解析
