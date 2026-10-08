# Reclient 0.172 生命周期维护

这里维护 0.172 系列的 RBE scanner 启动、失败回收、重试和退出修补。当前修补已通过真实 scanner 专项验收，并部署到 system 和 vendor 两套默认 `live/reproxy`。这不是完整 Android 构建或远端 RBE 执行验收。

当前版本为 `0.172.0.3cf60ba5-lifecycle-minimal.b305de73`。`lifecycle-minimal` 标识本仓库维护的生命周期修补，`b305de73` 是生产补丁 SHA256 的前八位，不是 Git 提交号。

## 基线和目录

生产二进制基线为 `0.172.0.3cf60ba5`。源码基线为上游提交 [`fcf667882d53b3c98d0392613b6cfe80a4884af2`](https://github.com/bazelbuild/reclient/commit/fcf667882d53b3c98d0392613b6cfe80a4884af2)，其 `GitOrigin-RevId` 为 `3cf60ba54e6e0e510da56d714142a949f86cd872`。

本目录是版本专用的补丁和维护记录，不是完整源码副本。仓库根目录的 0.186 源码保留原样；不要把本目录的 0.172 补丁直接应用到根目录。

- `VERSION`：已部署的自定义版本标记。
- `manifest.json`：基线、补丁、测试源码与二进制哈希。
- `patches/`：生产最小补丁和回归测试补丁。
- `tests/actual_scanner_test.go.txt`：真实 scanner 集成测试源文件。使用 `.txt` 后缀避免它被根目录的 Go 模块误当成一个独立测试包；准备工作区时恢复为 `.go`。
- `scripts/prepare.sh`：在新目录检出指定的 0.172 基线，检查哈希并应用补丁。
- `records/`：按日期保存的脱敏验收和部署摘要。
- `CHANGELOG.md`：版本变更历史。

## 修补范围

生产修改仅涉及 `internal/pkg/cppdependencyscanner/cppdepscanner.go` 和 `internal/pkg/cppdependencyscanner/depsscannerclient/depsscannerclient.go`：

- 启动失败时取消本次启动的 scanner，并等待明确退出确认。
- 回收未确认时阻止重试；父上下文已取消或错误不可重试时不再启动新实例。
- 每次启动使用权限为 `0700` 的独立 socket 目录，退出确认后只回收自身目录。
- 失败时关闭连接，避免取消状态或空 client 被当成就绪，并保留早退退出码。
- 正常关闭等待 scanner 退出。

这次部署只替换 system/vendor 各自的 `live/reproxy` 并更新 `live/version.txt`。`scandeps_server`、`bootstrap`、`rewrapper` 和其他预编译工具保持原文件；它们的自报版本仍为原版。这是单组件部署，不是所有工具统一重新打包的上游发行版。

## 准备和迭代

在 Linux 上安装 Git、Python 3、Go 和 `sha256sum`，然后运行：

```bash
bash 172_reclient/scripts/prepare.sh /absolute/path/to/new-reclient-172-workspace
cd /absolute/path/to/new-reclient-172-workspace
go test -race -count=5 -timeout=180s \
  -run '^(TestStartup|TestLifecycle)' \
  ./internal/pkg/cppdependencyscanner/depsscannerclient
TEST_REAL_SCANNER=/absolute/path/to/scandeps_server \
  go test -race -count=5 -timeout=180s \
  -run '^TestActualScannerStartupFailureAndRetry$' \
  ./internal/pkg/cppdependencyscanner/depsscannerclient
```

目标目录必须不存在。准备脚本不编译、不部署、不清理既有工作区，也不操作任何既有构建进程。真实 scanner 测试会启动并回收自己的测试子进程；请在独立测试目录运行。

源码声明的 Go 版本为 1.21.11，`-race` 需要 CGO 和 C 编译器。完整 reproxy 构建还需要上游构建流程生成的 clang flags 和版本嵌入文件。本次验收使用已有 Go 构建候选，不代表已验证官方 Bazel 发行流水线。根目录 README 中的完整构建步骤应在检出的 0.172 工作区执行；构建新候选时另行设置并核验程序实际自报版本，单改本目录 `VERSION` 不会改变二进制。

后续修改应基于固定的 0.172 源码基线维护补丁，更新 `VERSION`、哈希清单和日期记录，再做专项验收。新的补丁版本不要沿用本次二进制哈希或测试结论。完整原始证据保留在部署现场，公开仓库仅保存脱敏摘要。

## 当前验收边界

2026 年 10 月 8 日，system/vendor 原版 scanner 各完成五轮真实失败注入测试，均通过；启动和生命周期测试启用 `-race`、重复五次，通过。默认文件替换后，两树的实际默认 reproxy 均就绪、退出码为 0，scanner 退出确认码为 0，私有 socket 目录消失。

真实失败注入测试在首次 Capabilities RPC 成功后注入 `Unavailable`，确认旧子进程退出后，由测试显式执行第二次库启动；它不是外层 reproxy 自动重试循环的端到端故障注入。外层是否允许重试的条件由独立契约测试覆盖。整轮构建、远端认证和后端执行、历史绑定冲突的占用者归属均不在本次验收范围内。
