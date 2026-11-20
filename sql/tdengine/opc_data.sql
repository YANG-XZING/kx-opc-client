-- 程序启动时会自动执行以下建库、建表（已存在则跳过），新点位自动 ALTER 加列，子表在首次写入时自动创建。
-- 本文件仅供 DBA 手工建库建表或核对。压缩方式按 config.yaml 的 compress / compress_level，默认 zstd / high。

CREATE DATABASE IF NOT EXISTS `kx_opc` PRECISION 'ms' KEEP 1095d DURATION 10d MAXROWS 40000 MINROWS 1000 COMP 2 CACHEMODEL 'last_row';

CREATE STABLE IF NOT EXISTS `kx_opc`.`opc_data` (
  `ts`             TIMESTAMP    COMPRESS 'zstd' LEVEL 'high',  -- 数据采集时间：服务端心跳变化到达客户端的时刻（毫秒）
  `HEART_DATETIME` VARCHAR(256) COMPRESS 'zstd' LEVEL 'high'   -- 服务端心跳；其余点位列由程序按点名自动添加，类型按 OPC 节点类型
) TAGS (                                                       -- 标签不支持列压缩
  `device_id` VARCHAR(64),        -- 炉台号，如 A12
  `device_ip` VARCHAR(64)         -- 炉台 IP，如 10.65.108.12
);

-- 新点位列（程序自动添加），同样带压缩选项：
-- ALTER STABLE `kx_opc`.`opc_data` ADD COLUMN `MELT_TEMP_PV` FLOAT COMPRESS 'zstd' LEVEL 'high';

-- 已有列改压缩方式（程序启动时自动对不一致的列执行；已有数据要 COMPACT 后才按新方式压缩）：
-- ALTER STABLE `kx_opc`.`opc_data` MODIFY COLUMN `MELT_TEMP_PV` COMPRESS 'zstd' LEVEL 'high';
-- 查看每列的压缩方式（compress、level 两列）：
-- DESCRIBE `kx_opc`.`opc_data`;

-- 子表示例（程序自动创建）：
-- CREATE TABLE IF NOT EXISTS `kx_opc`.`opc_10_65_108_12` USING `kx_opc`.`opc_data` TAGS ('A12', '10.65.108.12');

-- 列名规则：列名 = 点名，保留原始大小写（TDengine 中用反引号包裹的名称区分大小写，查询时也要加反引号）；
--   非字母数字下划线替换为 _，数字开头或与 ts/device_id/device_ip 同名时加 p_ 前缀

-- 查询示例
-- 某炉最近 1 小时：
--   SELECT ts, `MELT_TEMP_PV` FROM kx_opc.opc_data WHERE device_id = 'A12' AND ts > NOW - 1h;
-- 数值列按 OPC 类型建（FLOAT/DOUBLE 等），可以直接计算：
--   SELECT _wstart, AVG(`MELT_TEMP_PV`) FROM kx_opc.opc_data WHERE device_id = 'A12' AND ts > NOW - 1d INTERVAL(1m);
-- 每台炉最新一行：
--   SELECT device_id, LAST_ROW(*) FROM kx_opc.opc_data PARTITION BY device_id;
