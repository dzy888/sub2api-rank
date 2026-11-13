# 受限管理 Key、监控与重新登录恢复

适用于常规主库网关的 OpenAI OAuth 代理副本。数据库迁移 `343_proxy_replica_relogin_recovery.sql` 随正常启动执行；已有暂停记录没有故障归属证明，不能追认自动恢复。

## 专用 Key 权限

| 用途 | 权限 |
| --- | --- |
| 账号监控 | `accounts.read` |
| 分组状态和模型 | `groups.models.read` |
| 代理候选 | 全站 `proxies.read`（不返回密码） |
| 查询副本创建任务 | `accounts.proxy_replicas.read` |
| 创建副本、代理/并发/WS 和激活 | `accounts.proxy_replicas.create` |
| 恢复能力协商及查询 | `accounts.proxy_replicas.recovery.read` |
| 准备并有条件恢复副本 | `accounts.proxy_replicas.recovery.resume` |

所有账号规则限定 `platforms=["openai"]`、`account_types=["oauth"]`，并明确选择分组。副本和源账号的全部关联分组都必须具备对应权限。源账号普通凭据、状态及删除等写入也检查关联副本范围。恢复权限不隐含创建、修改配置或清除其他错误的权限。

## 监控响应

`GET /api/v1/admin/accounts?page=1&page_size=1000` 和单账号读取包含 `proxy_id`、配置并发 `concurrency/configured_concurrency`、`current_concurrency`、`status`、`schedulable`、`expires_at`（Unix 秒或 null）、`auto_pause_on_expired`、`rate_limited_at`、`rate_limit_reset_at`、`overload_until`、`temp_unschedulable_until`。时间窗口字段沿用 RFC3339。分组只返回已授权的 `{id,status}`，分组模型接口也提供 `status`。

`auth_invalid_reason="requires_relogin"` 只对应明确的 OAuth 认证失效；兼容原推送扫描器的 `error_message` 只返回固定脱敏文本，不透传上游正文。`credential_owner_account_id`、`source_account_id` 及最小 `extra.credential_owner_account_id` 标识副本源账号。仅回读白名单 WS 配置和模型映射，不包含 Token、密码、2FA 或任意扩展字段。

实时并发按最多 250 个账号分批读取 Redis，并共享 5 秒总超时；缓存不可用、结果缺失或负值均返回 503，不当作 0。SQL 批量读取分组状态，避免逐账号查询。

列表最多 1000 条/页，附带 `snapshot_at` 和 `snapshot_consistency="live_page"`，禁用 HTTP 缓存。这是当前页的实时观察，多页之间及 SQL/Redis 之间没有原子快照保证。采购端应校验 `total`、去重并确认拿到完整列表；超出自身扫描上限、缺页或任何读取失败时停止自动采购。调度字段表示配置与临时窗口，不保证所有模型额度或动态调度条件均可用。

## 重新登录恢复协议 v1

以下路径均以前缀 `/api/v1/admin/accounts/{source_id}/proxy-replicas/relogin-recoveries` 为基准：

| 方法与路径 | 权限 |
| --- | --- |
| `GET /capabilities` | `accounts.proxy_replicas.recovery.read` |
| `POST /` | `accounts.proxy_replicas.recovery.resume` |
| `GET /{operation_id}` | `accounts.proxy_replicas.recovery.read` |
| `POST /{operation_id}/resume` | `accounts.proxy_replicas.recovery.resume` |

能力响应为 `{"version":1,"pause_ownership":true,"atomic_manual_override":true,"credential_generation":true,"durable_idempotency":true}`。准备请求使用 `Idempotency-Key`，正文 `{"version":1,"replicas":[{"account_id":456,"proxy_operation_id":"original-op","proxy_id":11,"concurrency":5,"ws_mode":"ctx_pool","group_ids":[7]}]}`。允许 1–3200 条唯一副本，正文最多 4 MiB；分组归一化为升序。准备只记录基线，不修改调度或凭据。

恢复 POST 必须使用原操作 ID 作为 `Idempotency-Key`，正文 `{"version":1}`。查询及恢复返回 `version/operation_id/source_account_id/status/credential_generation/source_auth_recovered/replicas`。逐项保留原配置，并附加 `schedulable/recovery_status/reason`。

任务状态为 `prepared/waiting/completed`；逐项状态为 `eligible/auth_failure`、`restored/restored`、`already_active/already_active`，或 `skipped` 加 `manual_pause/disabled/deleted/proxy_unavailable/config_changed/source_unavailable/no_owned_pause`。完成结果是完成时的持久审计快照，后续人工关闭不更改快照，也不会被重放重新开启。

明确的源 OAuth 认证故障在同一数据库事务内记录原本开启且可用的副本、故障批次、凭据版本与配置/调度意图版本，再关闭副本。显式调度操作（包括 false→false）、定时关闭、禁用、删除、配置与分组修改、性能暂停均撤销恢复依据。原本暂停、没有证明或后续改过配置的记录跳过。

真正替换认证凭据时产生随机不透明版本，响应不包含凭据或其摘要。查询/恢复时，仅当版本不同于准备基线且源账号可用，才使用该版本的凭据直接 GET OpenAI `/backend-api/wham/usage` 验证；不刷新 Token、不发送计费推理。验证在数据库锁外进行，成功按原版本比较写入 60 秒有效证明；并发更换凭据不会把旧验证赋给新版本。

恢复事务锁定源账号、任务、副本、代理及分组，重新核对暂停归属、版本、人工意图、到期和临时状态；只打开仍符合条件的副本。源账号保持关闭调度，不复制 Token、不修改代理/并发/WS/分组，不清理副本错误，不创建新账号，并在同一事务发布调度缓存失效事件。锁等待有界；失败回滚，可重新查询核对。任务有效期 15 分钟，过期恢复 POST 返回 409，GET 仍可读取。全局操作 ID 不能跨源账号或配置重用。

## 51 直推所需权限

根据 [51 公开接口文档](https://51tuiding.com/portal-api) 核对了字段与交付行为；公开文档没有提供其对接 Sub2API 的逐请求调用日志，因此真实入口组合仍需部署联调确认。

| 实际操作 | Sub2API 入口 | 需要的权限 |
| --- | --- | --- |
| 创建/导入 OAuth | `POST /accounts` 或 `/accounts/import/codex-session` | `accounts.create`、`accounts.credentials`；配置代理/WS、模型、并发/到期时分别加 `accounts.network`、`accounts.models.write`、`accounts.schedule` |
| 更新登录凭据和说明 | `PUT /accounts/{id}` 或 Codex 导入匹配更新 | `accounts.credentials`；名称备注加 `accounts.update` |
| 写并回读并发/WS | `PUT /accounts/{id}`、`GET /accounts/{id}` | `accounts.schedule`、`accounts.network`、`accounts.read` |
| 暂停/激活与清理临时到期 | `POST /accounts/{id}/schedulable`、`PUT /accounts/{id}` | `accounts.status`、`accounts.schedule` |
| 删除测试账号/到期移除 | `DELETE /accounts/{id}` | `accounts.delete` |
| 变更分组 | `PUT /accounts/{id}` | `accounts.groups` 及目标分组授权 |

51 的暂停交付预检可能先创建**无分组的合成凭据测试账号**，回读并立即删除；相应权限若仅限分组，该预检会拒绝。需由管理员明确决定是否给专用规则勾选“包含无分组”，不会自动扩大授权。只读 Key 不足以交付。

普通受限写入仅开放 WS 模式及布尔开关的 `extra` 白名单，并按字段权限检查，不能伪造副本归属。`expires_at:null` 或 `0` 清除到期时间。重新登录本身由 51 原流程执行；本恢复接口不下单、不扣积分。真实采购和真实 OAuth/代理联调尚需接入方验收。
