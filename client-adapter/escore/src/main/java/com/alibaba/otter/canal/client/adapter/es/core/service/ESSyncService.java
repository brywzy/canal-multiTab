package com.alibaba.otter.canal.client.adapter.es.core.service;

import java.util.*;

import javax.sql.DataSource;

import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem.ColumnItem;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem.TableItem;
import com.alibaba.otter.canal.client.adapter.es.core.config.SqlParser;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;

/**
 * ES 同步 Service
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESSyncService {

    private static Logger logger = LoggerFactory.getLogger(ESSyncService.class);

    private ESTemplate    esTemplate;

    public ESSyncService(ESTemplate esTemplate){
        this.esTemplate = esTemplate;
    }

    public void sync(Collection<ESSyncConfig> esSyncConfigs, Dml dml) {
        long begin = System.currentTimeMillis();
        if (esSyncConfigs != null) {
            if (logger.isTraceEnabled()) {
                logger.trace("Destination: {}, database:{}, table:{}, type:{}, affected index count: {}",
                    dml.getDestination(),
                    dml.getDatabase(),
                    dml.getTable(),
                    dml.getType(),
                    esSyncConfigs.size());
            }

            for (ESSyncConfig config : esSyncConfigs) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Prepared to sync index: {}, destination: {}",
                        config.getEsMapping().get_index(),
                        dml.getDestination());
                }

                //TODO 判断是不是主表进行 update delete insert
//                if(config.getEsMapping().getMainTab().equals(dml.getTable())){
                    this.sync(config, dml);
//                }

                if (logger.isTraceEnabled()) {
                    logger.trace("Sync completed: {}, destination: {}",
                        config.getEsMapping().get_index(),
                        dml.getDestination());
                }
            }
            if (logger.isTraceEnabled()) {
                logger.trace("Sync elapsed time: {} ms, affected indexes count：{}, destination: {}",
                    (System.currentTimeMillis() - begin),
                    esSyncConfigs.size(),
                    dml.getDestination());
            }
            if (logger.isDebugEnabled()) {
                StringBuilder configIndexes = new StringBuilder();
                esSyncConfigs
                    .forEach(esSyncConfig -> configIndexes.append(esSyncConfig.getEsMapping().get_index()).append(" "));
                logger.debug("DML: {} \nAffected indexes: {}",
                    JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue),
                    configIndexes.toString());
            }
        }
    }

    public void sync(ESSyncConfig config, Dml dml) {
        try {
            // 如果是按时间戳定时更新则返回
            if (config.getEsMapping().isSyncByTimestamp()) {
                return;
            }

            long begin = System.currentTimeMillis();

            String type = dml.getType();
            if (type != null && type.equalsIgnoreCase("INSERT")) {
                insert(config, dml);
            } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                update(config, dml);
            } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                delete(config, dml);
            } else {
                return;
            }

            if (logger.isTraceEnabled()) {
                logger.trace("Sync elapsed time: {} ms,destination: {}, es index: {}",
                    (System.currentTimeMillis() - begin),
                    dml.getDestination(),
                    config.getEsMapping().get_index());
            }
        } catch (Throwable e) {
            logger.error("sync error, es index: {}, DML : {}", config.getEsMapping().get_index(), dml);
            throw new RuntimeException(e);
        }
    }

    /**
     * 插入操作dml
     *
     * @param config es配置
     * @param dml dml数据
     */
    private void insert(ESSyncConfig config, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();
        for (Map<String, Object> data : dataList) {
            if (data == null || data.isEmpty()) {
                continue;
            }

            if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
                // ------单表 & 所有字段都为简单字段------
                singleTableSimpleFiledInsert(config, dml, data);
            } else {
                // ------是主表 查询sql来插入------
                if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                    //TODO 如果这里主表执行语句的时候很慢，可以进行分拆执行然后拼接结果集
                    mainTableInsert(config, dml,schemaItem.getMainTable().getAlias(), data);
                }

                /*   从表的操作 交到从表 */
                // 从表的操作
                for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (tableItem.isMain()) {
                        continue;
                    }
                    if (!tableItem.getTableName().equals(dml.getTable())) {
                        continue;
                    }
                    // 关联条件出现在主表查询条件是否为简单字段
//                    boolean allFieldsSimple = true;
//                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
//                        if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
//                            allFieldsSimple = false;
//                            break;
//                        }
//                    }
//                    allFieldsSimple = true;
                    // 所有查询字段均为简单字段
//                    if (allFieldsSimple) {
                        // 不是子查询
                    if (!tableItem.isSubQuery()) {
                        // ------关联表简单字段插入------
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                            Object value = esTemplate.getValFromData(config.getEsMapping(),
                                data,
                                fieldItem.getFieldName(),
                                fieldItem.getColumn().getColumnName());
                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), value);
                        }

                        joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData);
                    } else {
                        // ------关联子表简单字段插入------
                        subTableSimpleFieldOperation(config, dml, data, null, tableItem);
                    }
//                    } else {
//                        // ------关联子表复杂字段插入 执行全sql更新es------
//                        wholeSqlOperation(config, dml, data, null, tableItem);
//                    }
                }

            }
        }
    }

    /**
     * 更新操作dml
     *
     * @param config es配置
     * @param dml dml数据
     */
    private void update(ESSyncConfig config, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        List<Map<String, Object>> oldList = dml.getOld();
        if (dataList == null || dataList.isEmpty() || oldList == null || oldList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();
        int i = 0;
        for (Map<String, Object> data : dataList) {
            Map<String, Object> old = oldList.get(i);
            if (data == null || data.isEmpty() || old == null || old.isEmpty()) {
                continue;
            }

            if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
                // ------单表 & 所有字段都为简单字段------
                singleTableSimpleFiledUpdate(config, dml, data, old);
            } else {
                // ------主表 查询sql来更新------
                if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                    ESMapping mapping = config.getEsMapping();
                    String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
                    FieldItem idFieldItem = schemaItem.getSelectFields().get(idFieldName);

                    boolean idFieldSimple = true;
                    if (idFieldItem.isMethod() || idFieldItem.isBinaryOp()) {
                        idFieldSimple = false;
                    }
                    //修改字段是否有复杂操作(函数，二进制)
                    boolean allUpdateFieldSimple = true;
                    out: for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
                        for (ColumnItem columnItem : fieldItem.getColumnItems()) {
                            if (old.containsKey(columnItem.getColumnName())) {
                                if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                                    allUpdateFieldSimple = false;
                                    break out;
                                }
                            }
                        }
                    }

                    // 不支持主键更新!!

                    // 判断是否有外键更新
                    boolean fkChanged = false;
                    for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                        if (tableItem.isMain()) {
                            continue;
                        }
                        //是否修改了关联字段
                        boolean changed = false;
                        for (List<FieldItem> fieldItems : tableItem.getRelationTableFields().values()) {
                            for (FieldItem fieldItem : fieldItems) {
                                if (old.containsKey(fieldItem.getColumn().getColumnName())) {
                                    fkChanged = true;
                                    changed = true;
                                    break;
                                }
                            }
                        }
                        // 如果外键有修改,则更新所对应该表的所有查询条件数据
                        if (changed) {
                            for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                                fieldItem.getColumnItems()
                                    .forEach(columnItem -> old.put(columnItem.getColumnName(), null));
                            }
                        }
                    }

                    // 判断主键和所更新的字段是否全为简单字段
                    if (idFieldSimple && allUpdateFieldSimple && !fkChanged) {
                        singleTableSimpleFiledUpdate(config, dml, data, old);
                    } else {
                        mainTableUpdate(config, dml, schemaItem.getMainTable().getAlias(), data, old);
                    }
                }

                // 从表的操作
                for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (tableItem.isMain()) {
                        continue;
                    }
                    if (!tableItem.getTableName().equals(dml.getTable())) {
                        continue;
                    }

                    // 关联条件出现在主表查询条件是否为简单字段
//                    boolean allFieldsSimple = true;
//                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
//                        if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
//                            allFieldsSimple = false;
//                            break;
//                        }
//                    }

                    //是否修改了关联字段
                    boolean changed = false;
                    for(SchemaItem.RelationFieldsPair relationFieldsPair:tableItem.getRelationFields()){
                        FieldItem fieldItem = relationFieldsPair.getRightFieldItem();
                        if (old.containsKey(fieldItem.getFieldName())) {
                            changed = true;

                            break;
                        }
                    }
                    /*
                    // 如果外键有修改,则更新所对应该表的所有查询条件数据
                    if (changed) {
                        for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                            fieldItem.getColumnItems()
                                    .forEach(columnItem -> old.put(columnItem.getColumnName(), null));
                        }
                    }*/

                    // 所有查询字段均为简单字段
//                    if (allFieldsSimple) {
                        // 不是子查询
                    if (!tableItem.isSubQuery()) {
                        // ------关联表简单字段更新------
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        Map<String, Object> esOldData = new LinkedHashMap<>();
                        //TODO on的内容顺序互换可能，需要两边都判断
                        for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                            if (old.containsKey(fieldItem.getColumn().getColumnName())) {
                                Object value = esTemplate.getValFromData(config.getEsMapping(),
                                    data,
                                    fieldItem.getFieldName(),
                                    fieldItem.getColumn().getColumnName());
                                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), value);
                                esOldData.put(fieldItem.getFieldName(),old.get(fieldItem.getColumn().getColumnName()));
                            }
                        }
//                            Map<String, String> sqlConditionFileds = config.getEsMapping().getSqlConditionFileds();
                        ESSyncUtil.putSqlConditionFiledByUpdate(esFieldData,config.getEsMapping(),tableItem.getAlias(),data);
                        if(changed) {
                            joinTableSecondaryUpdate(config, dml,  esOldData, esFieldData,tableItem.getRelationSelectFieldItems(),tableItem.getAlias(),schemaItem.getMainTable().getTableName());
                        }else{
                            joinTableSimpleFieldOperation(config, dml, data, tableItem, esFieldData);
                        }
                    } else {
                        // ------关联子表简单字段更新------
                        subTableSimpleFieldOperation(config, dml, data, old, tableItem);
                    }
//                    } else {
//                        // ------关联子表复杂字段更新 执行全sql更新es------
//                        wholeSqlOperation(config, dml, data, old, tableItem);
//                    }
                }
            }

            i++;
        }
    }

    /**
     * 修改了次关联表的关联查询条件的处理
     * @param config
     * @param dml
     * @param data
     * @param old
     * @param tableItem
     * @param esFieldData
     */
    private void joinTableSecondaryUpdate(ESSyncConfig config, Dml dml, Map<String, Object> old,
                                          Map<String, Object> esFieldData,List<FieldItem> relationSelectFieldItems,String tableAlias,String mainTab) {
        ESMapping mapping = config.getEsMapping();

        if (logger.isDebugEnabled()) {
            logger.trace("Join table update es index by foreign key, destination:{}, table: {}, index: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index());
        }
        esTemplate.secondaryUpdateByQuery(config, esFieldData, old,relationSelectFieldItems,tableAlias,mainTab);
    }

    /**
     * 删除操作dml
     *
     * @param config es配置
     * @param dml dml数据
     */
    private void delete(ESSyncConfig config, Dml dml) {
        List<Map<String, Object>> dataList = dml.getData();
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();

        for (Map<String, Object> data : dataList) {
            if (data == null || data.isEmpty()) {
                continue;
            }

            ESMapping mapping = config.getEsMapping();

            // ------是主表------
            if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                if (mapping.get_id() != null) {
                    FieldItem idFieldItem = schemaItem.getIdFieldItem(mapping);
                    // 主键为简单字段
                    if (!idFieldItem.isMethod() && !idFieldItem.isBinaryOp()) {
                        Object idVal = esTemplate.getValFromData(mapping,
                            data,
                            idFieldItem.getFieldName(),
                            idFieldItem.getColumn().getColumnName());

                        if (logger.isTraceEnabled()) {
                            logger.trace("Main table delete es index, destination:{}, table: {}, index: {}, id: {}",
                                config.getDestination(),
                                dml.getTable(),
                                mapping.get_index(),
                                idVal);
                        }
                        esTemplate.delete(mapping, idVal, null);
                    } else {
                        // ------主键带函数, 查询sql获取主键删除------
                        // FIXME 删除时反查sql为空记录, 无法获获取 id field 值
                        mainTableDelete(config, dml, data);
                    }
                } else {
                    FieldItem pkFieldItem = schemaItem.getIdFieldItem(mapping);
                    if (!pkFieldItem.isMethod() && !pkFieldItem.isBinaryOp()) {
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        Object pkVal = esTemplate.getESDataFromDmlData(mapping, data, esFieldData);

                        if (logger.isTraceEnabled()) {
                            logger.trace("Main table delete es index, destination:{}, table: {}, index: {}, pk: {}",
                                config.getDestination(),
                                dml.getTable(),
                                mapping.get_index(),
                                pkVal);
                        }
                        esFieldData.remove(pkFieldItem.getFieldName());
                        esFieldData.keySet().forEach(key -> esFieldData.put(key, null));
                        esTemplate.delete(mapping, pkVal, esFieldData);
                    } else {
                        // ------主键带函数, 查询sql获取主键删除------
                        mainTableDelete(config, dml, data);
                    }
                }

            }

            // 从表的操作
            for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                if (tableItem.isMain()) {
                    continue;
                }
                if (!tableItem.getTableName().equals(dml.getTable())) {
                    continue;
                }

                // 不是子查询
                if (!tableItem.isSubQuery()) {
                     // 删除前关联条件的旧值
                     Map<String,Object> old = new HashMap<>();
                     ESMapping esMapping = config.getEsMapping();
                     for (SchemaItem.RelationFieldsPair relationFieldsPair : tableItem.getRelationFields()){
                         FieldItem fieldItem = relationFieldsPair.getRightFieldItem();
                         String esKey = esMapping.getColumnMapping().get(fieldItem.getExpr());
                         old.put(esKey,data.get(fieldItem.getFieldName()));
                     }

                     joinTableSecondaryDelete(config, dml,  old, tableItem.getRelationSelectFieldItems(),tableItem.getAlias());

                } else {
                    // ------关联子表简单字段更新------
                    subTableSimpleFieldOperation(config, dml, data, null, tableItem);
                }

            }
        }
    }


    private void joinTableSecondaryDelete(ESSyncConfig config, Dml dml, Map<String, Object> old,
                                          List<FieldItem> relationSelectFieldItems,String tableAlias) {
        ESMapping mapping = config.getEsMapping();

        if (logger.isDebugEnabled()) {
            logger.trace("Join table update es index by foreign key, destination:{}, table: {}, index: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index());
        }
        esTemplate.secondaryDeleteByQuery(config, old,relationSelectFieldItems,tableAlias);
    }


    public void joinTableSecondaryDelete(ESSyncConfig config, Dml dml, Map<String, Object> data, TableItem tableItem ){
        ESMapping mapping = config.getEsMapping();
        for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
            for (FieldItem fieldItem : entry.getValue()) {
                if (fieldItem.getColumnItems().size() == 1) {
                    Object value = esTemplate.getValFromData(mapping,
                            data,
                            fieldItem.getFieldName(),
                            entry.getKey().getColumn().getColumnName());

//                    String fieldName = fieldItem.getFieldName();
                    String fieldName = fieldItem.getExpr();
                    // 判断是否是主键
                    if (fieldName.equals(mapping.get_id())) {
                        fieldName = "_id";
                    }
                }
            }
        }
    }


    /**
     * 单表简单字段insert
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     */
    private void singleTableSimpleFiledInsert(ESSyncConfig config, Dml dml, Map<String, Object> data) {
        ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        Object idVal = esTemplate.getESDataFromDmlData(mapping, data, esFieldData);

        if (logger.isTraceEnabled()) {
            logger.trace("Single table insert to es index, destination:{}, table: {}, index: {}, id: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                idVal);
        }
        esTemplate.insert(mapping, idVal, esFieldData);
    }

    /**
     * 主表(单表)复杂字段insert
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     */
    private void mainTableInsert(ESSyncConfig config, Dml dml,String tableAlias, Map<String, Object> data) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
//        String condition = ESSyncUtil.pkConditionSql(mapping, data);
        String condition = ESSyncUtil.appendSqlConditionFiled(mapping, data,tableAlias);
        sql = ESSyncUtil.appendCondition(sql, condition);
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Main table insert to es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.replace("\n", " "));
        }
        Util.sqlRS(ds, sql, rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    Object idVal = esTemplate.getESDataFromRS(mapping, rs, esFieldData);

                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "Main table insert to es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index(),
                            idVal);
                    }
                    esTemplate.insert(mapping, idVal, esFieldData);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    private void mainTableDelete(ESSyncConfig config, Dml dml, Map<String, Object> data) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
        String condition = ESSyncUtil.pkConditionSql(mapping, data);
        sql = ESSyncUtil.appendCondition(sql, condition);
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Main table delete es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.replace("\n", " "));
        }
        Util.sqlRS(ds, sql, rs -> {
            try {
                Map<String, Object> esFieldData = null;
                if (mapping.getPk() != null) {
                    esFieldData = new LinkedHashMap<>();
                    esTemplate.getESDataFromDmlData(mapping, data, esFieldData);
                    esFieldData.remove(mapping.getPk());
                    for (String key : esFieldData.keySet()) {
                        esFieldData.put(Util.cleanColumn(key), null);
                    }
                }
                while (rs.next()) {
                    Object idVal = esTemplate.getIdValFromRS(mapping, rs);

                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "Main table delete to es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index(),
                            idVal);
                    }
                    esTemplate.delete(mapping, idVal, esFieldData);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    /**
     * 关联表主表简单字段operation
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     * @param tableItem 当前表配置
     */
    private void joinTableSimpleFieldOperation(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                               TableItem tableItem, Map<String, Object> esFieldData) {
        ESMapping mapping = config.getEsMapping();

        Map<String, Object> paramsTmp = new LinkedHashMap<>();
        if (logger.isDebugEnabled()) {
            logger.trace("Join table update es index by foreign key, destination:{}, table: {}, index: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index());
        }
        esTemplate.updateByQuery(config, paramsTmp, esFieldData, tableItem.getAlias());
    }

    /**
     * 关联子查询, 主表简单字段operation
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     * @param old 单行old数据
     * @param tableItem 当前表配置
     */
    private void subTableSimpleFieldOperation(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                              Map<String, Object> old, TableItem tableItem) {
        ESMapping mapping = config.getEsMapping();

        MySqlSelectQueryBlock queryBlock = SqlParser.parseSQLSelectQueryBlock(tableItem.getSubQuerySql());

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ")
            .append(SqlParser.parse4SQLSelectItem(queryBlock))
            .append(" FROM ")
            .append(SqlParser.parse4FromTableSource(queryBlock));

        String whereSql = SqlParser.parse4WhereItem(queryBlock);
        if (whereSql != null) {
            sql.append(" WHERE ").append(whereSql);
        } else {
            sql.append(" WHERE 1=1 ");
        }

        List<Object> values = new ArrayList<>();

        for (FieldItem fkFieldItem : tableItem.getRelationTableFields().keySet()) {
            String columnName = fkFieldItem.getColumn().getColumnName();
            Object value = esTemplate.getValFromData(mapping, data, fkFieldItem.getFieldName(), columnName);
            sql.append(" AND ").append(columnName).append("=? ");
            values.add(value);
        }

        String groupSql = SqlParser.parse4GroupBy(queryBlock);
        if (groupSql != null) {
            sql.append(groupSql);
        }

        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Join table update es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.toString().replace("\n", " "));
        }
        Util.sqlRS(ds, sql.toString(), values, rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();

                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        if (old != null) {
                            out: for (FieldItem fieldItem1 : tableItem.getSubQueryFields()) {
                                for (ColumnItem columnItem0 : fieldItem.getColumnItems()) {
                                    if (fieldItem1.getFieldName().equals(columnItem0.getColumnName()))
                                        for (ColumnItem columnItem : fieldItem1.getColumnItems()) {
                                            if (old.containsKey(columnItem.getColumnName())) {
                                                Object val = esTemplate.getValFromRS(mapping,
                                                    rs,
                                                    fieldItem.getFieldName(),
                                                    fieldItem.getColumn().getColumnName());
                                                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                                                break out;
                                            }
                                        }
                                }
                            }
                        } else {
                            Object val = esTemplate.getValFromRS(mapping,
                                rs,
                                fieldItem.getFieldName(),
                                fieldItem.getColumn().getColumnName());
                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                        }
                    }

                    Map<String, Object> paramsTmp = new LinkedHashMap<>();
                    for (Map.Entry<FieldItem, List<FieldItem>> entry : tableItem.getRelationTableFields().entrySet()) {
                        for (FieldItem fieldItem : entry.getValue()) {
                            if (fieldItem.getColumnItems().size() == 1) {
                                Object value = esTemplate.getValFromRS(mapping,
                                    rs,
                                    fieldItem.getFieldName(),
                                    entry.getKey().getColumn().getColumnName());
                                String fieldName = fieldItem.getFieldName();
                                // 判断是否是主键
                                if (fieldName.equals(mapping.get_id())) {
                                    fieldName = "_id";
                                }
                                paramsTmp.put(fieldName, value);
                            }
                        }
                    }

                    if (logger.isDebugEnabled()) {
                        logger.trace("Join table update es index by query sql, destination:{}, table: {}, index: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index());
                    }
                    esTemplate.updateByQuery(config, paramsTmp, esFieldData,tableItem.getAlias());
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    /**
     * 关联(子查询), 主表复杂字段operation, 全sql执行
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     * @param tableItem 当前表配置
     */
    private void wholeSqlOperation(ESSyncConfig config, Dml dml, Map<String, Object> data, Map<String, Object> old,
                                   TableItem tableItem) {
        ESMapping mapping = config.getEsMapping();
        // 防止最后出现groupby 导致sql解析异常
        String[] sqlSplit = mapping.getSql().split("GROUP\\ BY(?!(.*)ON)");
        String sqlNoWhere = sqlSplit[0];

        String sqlGroupBy = "";

        if (sqlSplit.length > 1) {
            sqlGroupBy = "GROUP BY " + sqlSplit[1];
        }

        StringBuilder sql = new StringBuilder(sqlNoWhere + " WHERE ");

        for (FieldItem fkFieldItem : tableItem.getRelationTableFields().keySet()) {
            String columnName = fkFieldItem.getColumn().getColumnName();
            Object value = esTemplate.getValFromData(mapping, data, fkFieldItem.getFieldName(), columnName);
            ESSyncUtil.appendCondition(sql, value, tableItem.getAlias(), columnName);
        }
        int len = sql.length();
        sql.delete(len - 5, len);
        sql.append(sqlGroupBy);

        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Join table update es index by query whole sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.toString().replace("\n", " "));
        }
        Util.sqlRS(ds, sql.toString(), rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                        if (old != null) {
                            for (ColumnItem columnItem : fieldItem.getColumnItems()) {
                                if (old.containsKey(columnItem.getColumnName())) {
                                    Object val = esTemplate.getValFromRS(mapping,
                                            rs,
                                            fieldItem.getFieldName(),
                                            fieldItem.getFieldName());
                                    esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                                    break;
                                }
                            }
                        } else {
                            Object val = esTemplate
                                .getValFromRS(mapping, rs, fieldItem.getFieldName(), fieldItem.getFieldName());
                            esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), val);
                        }

                        esTemplate.update(mapping,rs.getObject(mapping.get_id()),esFieldData);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    /**
     * 单表简单字段update
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行data数据
     * @param old 单行old数据
     */
    private void singleTableSimpleFiledUpdate(ESSyncConfig config, Dml dml, Map<String, Object> data,
                                              Map<String, Object> old) {
        ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();

        Object idVal = esTemplate.getESDataFromDmlData(mapping, data, old, esFieldData);

        if (logger.isTraceEnabled()) {
            logger.trace("Main table update to es index, destination:{}, table: {}, index: {}, id: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                idVal);
        }
        esTemplate.update(mapping, idVal, esFieldData);
    }

    /**
     * 主表(单表)复杂字段update
     *
     * @param config es配置
     * @param dml dml信息
     * @param data 单行dml数据
     */
    private void mainTableUpdate(ESSyncConfig config, Dml dml,String tableAlias, Map<String, Object> data, Map<String, Object> old) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
        String condition = ESSyncUtil.appendSqlConditionFiled(mapping, data, tableAlias);
//        String condition = ESSyncUtil.pkConditionSql(mapping, data);
        sql = ESSyncUtil.appendCondition(sql, condition);
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        if (logger.isTraceEnabled()) {
            logger.trace("Main table update to es index by query sql, destination:{}, table: {}, index: {}, sql: {}",
                config.getDestination(),
                dml.getTable(),
                mapping.get_index(),
                sql.replace("\n", " "));
        }
        Util.sqlRS(ds, sql, rs -> {
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    Object idVal = esTemplate.getESDataFromRS(mapping, rs, old, esFieldData);

                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "Main table update to es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                            config.getDestination(),
                            dml.getTable(),
                            mapping.get_index(),
                            idVal);
                    }
                    esTemplate.update(mapping, idVal, esFieldData);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return 0;
        });
    }

    /**
     * 提交批次
     */
    public void commit() {
        esTemplate.commit();
    }
}
