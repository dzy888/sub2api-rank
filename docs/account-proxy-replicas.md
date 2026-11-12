# OAuth 代理副本接口

一个源账号持有 OAuth 凭据，多条账号记录分别绑定代理与并发。源账号转为凭据维护主体，不能作为额外流量入口。此能力独立于普通账号复制接口，也不使用 Spark 的 `parent_account_id` 或 Spark 配额维度。

当前支持连接主数据库的常规网关中的独立 **OpenAI OAuth** 源账号及 OpenAI WS 模式；区域离线快照网关暂不支持代理副本。拒绝 API Key、PAT、Agent Identity、SIWC、多授权连接及 Spark 影子作为源账号。其他平台 OAuth 尚未接入此接口。

## 权限与请求

仅完整管理员身份可访问，支持现有管理员会话和全权限管理员 API Key。普通用户、观察员和受限管理员 Key 均不能调用。没有新增用户侧功能。

```http
POST /api/v1/admin/accounts/123/proxy-replicas
Idempotency-Key: harbor-example-operation
Content-Type: application/json

{
  "proxies": [
    {"proxy_id": 11, "concurrency": 5},
    {"proxy_id": 12, "concurrency": 8}
  ],
  "ws_mode": "ctx_pool",
  "activate_on_success": true
}
```

- 操作编号必填，长度 1–200，不允许路径分隔符或换行；操作编号在该实例内全局唯一，不能换源账号复用。
- 每次 1–100 个不同的代理，每个并发为 1–10000。代理必须存在、启用且未过期。
- `ws_mode` 必填，支持 `ctx_pool`、`passthrough`、`http_bridge`、`off`。
- `activate_on_success` 默认为 `false`；`true` 授权在核对后自动开启本次副本，无需再次确认。
- 分组、组内优先级、组内允许模型及账号模型映射继承创建时的源配置。副本并发及 WS 独立配置。
- 同一源账号不允许同时存在绑定同一代理的两条未删除副本。副本绑定的代理固定，禁止代理失效后的直连或备用代理回退。

成功返回现有 API 包装 `{ "code": 0, "message": "success", "data": ... }`，其中 `data` 为：

```json
{
  "operation_id": "harbor-example-operation",
  "source_account_id": 123,
  "status": "active",
  "replicas": [
    {
      "account_id": 456,
      "credential_owner_account_id": 123,
      "proxy_id": 11,
      "concurrency": 5,
      "ws_mode": "ctx_pool",
      "group_ids": [7],
      "schedulable": true,
      "status": "active"
    },
    {
      "account_id": 457,
      "credential_owner_account_id": 123,
      "proxy_id": 12,
      "concurrency": 8,
      "ws_mode": "ctx_pool",
      "group_ids": [7],
      "schedulable": true,
      "status": "active"
    }
  ]
}
```

这些响应使用专用 DTO，成功与失败结果均不包含 OAuth Token、完整凭据或上游响应体。

## 查询、重试与状态

```http
GET /api/v1/admin/accounts/123/proxy-replicas/harbor-example-operation
```

GET 返回同一种 `data`，只读，不创建或启用账号；源账号删除后仍可查询原操作。恢复通过再次 POST **相同操作编号和相同请求配置** 完成。

| 状态 | 含义 |
| --- | --- |
| `pending` | 已持久化操作身份，尚未完成事务；客户端可重试 POST |
| `failed` | 校验或配置失败，`error` 给出原因；修复后重试原 POST |
| `ready` | 配置完成，创建时没有授权自动调度 |
| `active` | 已完成操作，返回的副本当前均开启调度 |
| `paused` | 已完成操作中的副本后来被暂停、禁用或删除；重放不会重新开启 |

无效代理的 POST 会返回 `data.status=failed` 和对应代理失败项，HTTP 为 200，调用方必须检查业务状态。参数错误返回 400；操作编号换配置或换源账号返回 409；同源同代理已被另一操作占用也返回 409。查询不存在的操作返回 404。

代理列表先按 ID 排序后计算配置摘要，交换代理顺序视为同一配置；并发、代理集合、WS 或自动调度授权发生变化均视为不同配置。幂等身份保存在数据库，不依赖短期 HTTP 缓存 TTL。并发请求通过操作记录行锁串行执行。

操作先落库，再以事务执行全部副本的创建、关联分组、核对和激活。每条副本 INSERT 时即 `schedulable=false`；全部核对通过才统一开启。事务失败会回滚本次新增副本，避免暴露半配置账号；原操作身份保留，恢复只补尚未完成的工作。提交成功但客户端超时，重试直接读取原账号 ID。

完成后的 POST 重放只读现状。`activate_on_success=false` 的完成操作也不会在重试时自动开启；后续管理员如需开启，可使用已有账号调度管理接口。

## 凭据、额度与生命周期

凭据归属保存在账号的保留元数据 `extra.credential_owner_account_id`，带数据库索引和不可变关联约束；操作表及明细表保留创建来源和源账号 ID。普通账号创建/导入不能伪造此关联。

副本只持有模型映射等非令牌配置。HTTP、WS、后台账号检测及用量查询从源账号读取当前凭据；副本不持久化或刷新同一份 `refresh_token`。副本的手动刷新入口拒绝直接刷新，并要求操作源账号；源账号的手动刷新与请求路径、后台刷新共用协调器，按需刷新。刷新互斥键按源账号归一，Redis 锁不可用时停止源账号刷新，不降级为无分布式锁刷新。

源账号关闭调度不影响读取有效凭据。源账号 Token 消费走最新数据库凭据，不使用转换前的旧 Token 缓存；因此更新、撤销与删除不依赖副本缓存 TTL。副本使用独立代理，源账号刷新维护仍使用源账号的维护配置。

HTTP 与 WS 的发送前检查核对主库中的副本、源账号及代理。WS 复用连接的下一轮同样需要通过检查。源账号到期、禁用、软删除/物理删除或已被标记凭据失效时，副本不能开始新的上游请求；已经提交的请求不承诺撤回。源账号生命周期与代理失效的数据库触发器同步关闭相关副本并发布调度失效事件；发送前检查不等待缓存事件传播。源账号转为凭据主体时还会加主库调度隔离，防止转换前的缓存将它作为额外入口继续使用。

上游用量快照、429 共享额度冷却、RPM 和账号额度归属源账号；副本不复制一份独立的上游用量状态。源账号的真实额度及暂停阈值同样约束所有副本。代理传输错误仍归属对应代理。用量日志保留实际副本账号 ID，操作明细及凭据归属可追溯源账号；计费额度使用源账号身份。

接入方可以使用：51 返回源账号 ID → 自行分配代理 → POST → 确认 `data.status=active` 且所有副本 `schedulable=true` → 完成订单。跨订单代理轮询属于接入方职责。

## 验证

- 服务测试覆盖操作摘要、重复代理校验、源账号关闭调度后的凭据读取、源账号撤销/删除、当前凭据读取、并发只刷新一次、禁止直连回退及结果不含凭据。
- 处理器测试覆盖管理员授权和响应 DTO；调度投影测试确保凭据归属不会被缓存裁剪。
- PostgreSQL/Redis 集成测试覆盖 8 个并发重试只生成两条记录、继承分组模型限制、不同配置 409、暂停后重放不重启、失败代理修复恢复、代理禁用不改直连、源账号到期和删除后的失效。
- 使用合成凭据进行自动化测试，真实 OAuth 上游及真实代理请求仍需部署后的联调验收。
