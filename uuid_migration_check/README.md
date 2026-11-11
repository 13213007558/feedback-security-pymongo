# uuid_migration_check — UUID 表示迁移离线检查工具

针对「服务从历史 UUID 表示（subtype 3）迁到 STANDARD（subtype 4）后，部分账号查不到、
审计记录主体对不上」的场景，提供**纯离线**的检查与迁移计划生成。工具只做
`bson` 编解码分析，**从不连接数据库、不修改任何账号数据**。

## 背景

同一个 `uuid.UUID`，四种 `UuidRepresentation` 写出的 BSON 字节不同：

| 表示 | subtype | 字节序 |
|---|---|---|
| PYTHON_LEGACY | 3 | 原始字节 |
| JAVA_LEGACY | 3 | 两个 8 字节段各自反转并交换 |
| CSHARP_LEGACY | 3 | 前三个字段按小端重排 |
| STANDARD | 4 | 原始字节 |

后果：

- **查不到**：MongoDB 对 Binary 的比较包含 subtype 与字节内容。STANDARD 客户端
  用 subtype 4 查询，匹配不到 subtype 3 的历史行（反之亦然）。
- **主体变了**：同一段 subtype 3 字节，在 PYTHON/JAVA/CSHARP 三种 legacy 表示下
  解码出**三个不同的 UUID**。读审计记录时换了表示，actor 就"变了"。
- **subtype 3 字节本身不带来源信息**：没有元数据就无法确定该按哪种 legacy
  表示解读。本工具对这类数据只报告歧义，**绝不猜测改写**。

## 用法

```bash
# 一键演示：生成样例 -> 检查 -> 输出 report.json / plan.json
python -m uuid_migration_check demo --out uuid_check_out

# 分步执行
python -m uuid_migration_check generate --out out          # 生成演示 manifest
python -m uuid_migration_check check --manifest out/manifest.json \
    --report out/report.json --plan out/plan.json          # 检查并输出报告
```

`check` 在存在无法安全自动转换的记录时以退出码 1 结束，可接入 CI。

## 输入：带来源标注的 manifest

```json
{
  "samples": [
    {
      "id": "acct-001",
      "kind": "account",
      "source_representation": "PYTHON_LEGACY",
      "bson_hex": "..."
    },
    {"id": "acct-005", "kind": "account", "source_representation": null, "bson_hex": "..."}
  ]
}
```

- `source_representation` 允许 `PYTHON_LEGACY` / `JAVA_LEGACY` / `CSHARP_LEGACY` /
  `STANDARD` / `null`。`null` 表示来源未知。
- `bson_hex` 为整条 BSON 文档的十六进制（可用 `BSON.encode(...).hex()` 或
  `bsondump` 输出取得）。
- 标注与实际字节矛盾时（如声明 STANDARD 却是 subtype 3），记为
  `label_mismatch`，转入人工复核。

## 输出

### report.json（检查结果）

每条记录包含：

- `status`：`keep`（已是 subtype 4）/ `rewrite`（可安全转换）/
  `ambiguous`（subtype 3 无来源，拒绝猜测）/ `label_mismatch` / `no_uuid_fields`。
- `uuid_fields[].decode_matrix`：同一段字节在四种表示下分别解码成什么 UUID
  （`null` 表示该表示下仍是 Binary，不会被解码成 UUID）——直观解释"审计主体变了"。
- `uuid_fields[].query_bytes`：同一 UUID 按四种表示编码出的查询字节，以及哪种
  编码能命中现有存储字节（subtype + 内容完全一致才算命中）——直观解释"账号查不到"。
- `rewritten_bson_hex` + `rewrite_verified`：计划改写后的完整 BSON，并用 STANDARD
  重新解码校验 UUID 不变。
- `ejson_probes`：Extended JSON 导出/导入往返探针（见下）。
- `auto_convertible`：是否可安全自动转换。

报告级字段：

- `collisions`：迁移后会撞成同一 subtype 4 字节的身份组（同一 UUID 以多种
  历史字节形式共存）。涉及记录一律移出自动转换。
- `summary.unsafe_records`：**具体列出无法安全自动转换的记录 id** 及原因。

### plan.json（迁移计划，仅数据，不执行）

每条记录给出 `rewrite` / `keep` / `manual_review` / `skip` 动作；`rewrite`
附带逐字段的 `from_hex`/`to_hex` 与完整改写字节。执行前请满足
`preconditions`：先在备份/维护窗口操作、先部署能读 STANDARD 的应用代码、
先解决全部 `manual_review`。

## Extended JSON 导入导出风险（ejson_probes）

工具对每条记录做若干导出→再导入往返，对比字节与解码身份：

- `canonical_unspecified_roundtrip`：无损基线（Binary 原样导出）。
- `relaxed_standard_roundtrip`：subtype 3 会被写成 `$binary` 并在再导入时
  变成 subtype 4。对 PYTHON_LEGACY 数据 UUID 值不变（仅 subtype 变化）；
  但对**来源未知**的 subtype 3 数据，`bson.json_util` 会按 PYTHON_LEGACY
  字节序假设解码——JAVA/CSHARP 写入的身份会被**静默改错**。
- `relaxed_standard_export_legacy_import`：STANDARD 导出的 `$binary`/`$uuid`
  被 legacy 表示的导入端读回后，subtype 4 会退化为 subtype 3。
- `legacy_export_standard_import`：按声明的 legacy 表示导出、STANDARD 导入，
  本身就是一条可行的 EJSON 迁移路径（字节变化符合预期，UUID 值保持不变）。

任何 `bytes_preserved` 或 `uuids_preserved` 为 `false` 的探针都会把记录计入
`summary.ejson_hazard_records`。

## 运行测试

```bash
python -m pytest uuid_migration_check/tests -q   # 纯离线，无需 MongoDB
```

## 限制

- 工具不连接数据库；对真实库做全量核查时，可用 `mongodump` 导出 BSON 后
  逐条生成 manifest（`source_representation` 来自你的数据血缘/写入方元数据）。
- 查询字节对比使用完整编码字节；嵌套字段路径以点号拼接展示。
