# pi-harness-spike

一个 Agent 运行时原型，基于 [Pi](https://www.npmjs.com/package/@earendil-works/pi-agent-core) 的软件包，通过 ACP 对外提供服务。它用来验证几个最没把握的设计问题，**不能用于生产**。

## 验证的问题

**工具执行中，用户插话或取消怎么办。** 工具调用一旦发出就不再中止，也不再重发，因为中止不一定真能停下，重发可能让副作用执行两次。操作转入后台，模型先回应用户，迟到的结果再送回原来的会话。

**多个进程抢同一个会话文件，尤其在 NFS 上怎么办。** 会话日志只追加不改写，分段存储。新进程用 `link()` 领取递增的编号，再把所有更低的编号标记为「已作废」。这个标记不能撤销，也不存在能被改回旧值的共享指针。每次写入前后，进程都会重新检查自己的编号。

## 目录

| 路径 | 内容 |
|---|---|
| `src/fence.ts` | 写入权：领取编号、作废旧进程 |
| `src/sessionlog.ts` | 分段的追加日志及其恢复 |
| `src/ops.ts` | 后台操作：去重、迟到结果、重启后恢复 |
| `src/session.ts` | 一轮对话的主流程：插话、取消、上下文超限 |
| `src/compaction.ts` | 上下文压缩 |
| `src/guards.ts` | Pi 没有提供的模型层保护 |
| `test/` | 单元测试和 NFS 场景脚本 |

## 运行

需要 Node.js 20 或更高版本。

```bash
npm ci
npm run build
node --test test/*.test.mjs
```

NFS 场景脚本默认在单机上空跑，只能用来确认脚本本身能跑通：

```bash
NFS_DIR=/tmp/x node test/nfs_scenarios.mjs
```

要得出有意义的结论，需要把 `NFS_DIR` 指向真实 NFS 挂载点，并通过 `ACTOR_X`、`ACTOR_Y`、`ACTOR_Z` 把进程放到不同机器上。用法见脚本开头的注释。

启动服务本身需要一个 OpenAI 兼容的模型端点，通过 `KIT_BASE_URL`、`KIT_API_KEY`、`KIT_MODEL` 配置。

## 现状

- 存储设计**还没有在真实 NFS 上跨机器验证通过**，这是最大的未知项。
- 集成测试和对照实验依赖私有环境，没有放进这个仓库。这里只有单元测试和 NFS 场景脚本。
- 代码注释里的 `Review 7a`、`Gate 0` 是内部审查轮次和验收条件的编号，读代码时可以忽略。
- 暂未选择开源许可证。
