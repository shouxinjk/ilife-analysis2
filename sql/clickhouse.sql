

-- 事实明细表
-- 使用ReplacingMergeTree，仅采用最后更新的数值
-- 包含用户及商品 事实数据
CREATE TABLE ilife.fact
(  
	itemKey String,
	platform String,
	category String,
	categoryId String,
	property String,
	propertyKey String,
	propertyId String,
	ovalue String,
	valueType Int16,
	labelType String,
	labelDict String,
	labelCategory String,
	labelTagCategory String,
	normalizeType String,
	multiValueFunc String,
	mvalue Float32,
	score Float32,
	alpha Float32,
	beta Float32,
	gamma Float32,
	delte Float32,
	epsilon Float32,
	zeta Float32,
	eta Float32,
	theta Float32,
	lambda String,
	mstatus Int16,
	nstatus Int16,
	ts DateTime 
)  
ENGINE = ReplacingMergeTree(ts)
PARTITION BY toYYYYMM(ts)
ORDER BY (itemKey,property)

-- 分析结果明细表
-- 使用ReplacingMergeTree，仅采用最后得到的分析结果
CREATE TABLE ilife.info
(
    `itemKey` String,
    `categoryId` String,
    `dimensionId` String,
    `dimensionKey` String,
    `dimensionType` Int16,
    `priority` Int16,
    `feature` Int16,
    `weight` Float32,
    `script` String,
    `score` Float32,
    `status` Int16,
    `ts` DateTime
)
ENGINE = ReplacingMergeTree(ts)
PARTITION BY toYYYYMM(ts)
ORDER BY (itemKey, categoryId,dimensionId)


-- 用户需要明细表：
-- 按照userKey、needId、actionCategory、actionType、objectType、objectKey唯一。
-- 对于操作发起源头不予考虑：多种源头对同一个user的同一个need进行变化仅记录最后一次
CREATE TABLE ilife.need
(
    `userKey` String,
    `needId` String,
    `needType` String,
    `needName` String,
    `needAlias` String,
    `weight` Float32,
    `actionCategory` String,
    `actionType` String,
    `subjectType` String,
    `subjectKey` String,
    `objectType` String,
    `objectKey` String,    
    `ts` DateTime
)
ENGINE = ReplacingMergeTree(ts)
PARTITION BY toYYYYMM(ts)
ORDER BY (userKey,needId,actionCategory,actionType,objectType,objectKey)


-- 用户需要汇总表
-- 通过创建物化视图，使用AggregatingMergeTree表引擎。示例如下：
-- SELECT userKey,needType,needId,needName,needAlias,sumMerge(weight) FROM ilife.need_agg GROUP BY userKey,needType,needId;
CREATE MATERIALIZED VIEW ilife.need_agg 
ENGINE = AggregatingMergeTree() 
PARTITION BY userKey 
ORDER BY (userKey, needType, needId) AS
SELECT userKey, needType, needId, needName, needAlias, sumState(weight) AS weight, uniqState(subjectKey) AS subjects, uniqState(objectKey) AS objects
FROM ilife.need
GROUP BY userKey, needType, needId,needName,needAlias



