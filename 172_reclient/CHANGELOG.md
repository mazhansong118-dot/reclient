# Reclient 0.172 变更记录

## 2026 年 10 月 8 日

版本：`0.172.0.3cf60ba5-lifecycle-minimal.b305de73`。

纳入 scanner 启动失败回收、确认退出后才允许重试、独立 socket 目录和正常退出等待的最小生产修补。保留契约测试、真实子进程测试和真实 scanner 失败注入测试。

验收结果：生命周期与启动契约测试使用 `-race` 重复五次通过；system/vendor 原版 scanner 各五轮真实失败注入通过，旧进程退出后第二次启动无重叠且 socket 路径不同。

部署结果：已永久更新 system/vendor 默认 `live/reproxy` 和 `live/version.txt`，保留原文件备份；其他预编译工具未变。部署后默认路径运行验证通过，两树 reproxy 和 scanner 均正常退出，私有 scanner socket 目录回收。

完整构建、远端 RBE 执行以及外层自动重试循环的端到端故障注入尚未验收。首次验收脚本留下两条已退出的 reproxy 僵尸条目，scanner 无残留；未为清理条目重启共享容器。修正后的验收脚本回收了自己的 reproxy 子进程。
