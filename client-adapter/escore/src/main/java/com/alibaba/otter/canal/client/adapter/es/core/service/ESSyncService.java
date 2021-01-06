package com.alibaba.otter.canal.client.adapter.es.core.service;

import java.util.*;

import javax.sql.DataSource;

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

    public void sync(Collection<ESSyncConfig> esSyncConfigs, Dml dml, List<Dml> dmls) {
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
                this.sync(config, dml, dmls);
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
        }
    }

    public void sync(ESSyncConfig config, Dml dml, List<Dml> dmls) {
        try {
            // 如果是按时间戳定时更新则返回
            if (config.getEsMapping().isSyncByTimestamp()) {
                return;
            }

            long begin = System.currentTimeMillis();
            String type = dml.getType();
            if (dmls!=null) {
                if (type != null && type.equalsIgnoreCase("INSERT")) {
                    insertBatch(config, dml, dmls);
                } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                    updateBatch(config, dml, dmls);
                } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                    deleteBatch(config, dml, dmls);
                } else {
                    return;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("DML: {} \nAffected indexes: {}",
                            JSON.toJSONString(dmls, SerializerFeature.WriteMapNullValue),
                            config.getEsMapping().get_index());
                }

            }else{
                if (type != null && type.equalsIgnoreCase("INSERT")) {
                    insert(config, dml);
                } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
                    update(config, dml);
                } else if (type != null && type.equalsIgnoreCase("DELETE")) {
                    delete(config, dml);
                } else {
                    return;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("DML: {} \nAffected indexes: {}",
                            JSON.toJSONString(dml, SerializerFeature.WriteMapNullValue),
                            config.getEsMapping().get_index());
                }
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
    private void insertBatch(ESSyncConfig config, Dml dml, List<Dml> dmls) {
        ESMapping esMapping = config.getEsMapping();
        SchemaItem schemaItem = esMapping.getSchemaItem();

        List<Map<String, Object>> datas = new ArrayList<>();
        dmls.stream().forEach(dml1 -> datas.addAll(dml1.getData()));

        if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
            // ------单表 & 所有字段都为简单字段------
            singleTableSimpleFiledInsertBatch(config, dml, datas);
        }else {
            // ------是主表 查询sql来插入------
            if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
                mainTableInsertBatch(config, dml,schemaItem.getMainTable().getAlias(), datas);
            }else{
                // 从表的操作
                for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (!tableItem.getTableName().equals(dml.getTable())) {
                        continue;
                    }
                    // ------关联表简单字段插入------
                    List<Map<String, Object>> esFieldDataList = new ArrayList<>();
                    String alias = esMapping.getSqlConditionFields().get(tableItem.getAlias()).get("alias");
                    for (Map<String, Object> data : datas) {
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                            if (fieldItem.getFieldName().equals(alias)) {
                                Object value = esTemplate.getValFromData(config.getEsMapping(),
                                        data,
                                        fieldItem.getFieldName(),
                                        fieldItem.getColumn().getColumnName());
                                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), value);
                            }
                        }
                        esFieldDataList.add(esFieldData);
                    }
                    joinTableSimpleFieldOperationBatch(config, dml, tableItem, esFieldDataList);
                }
            }
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
                    mainTableInsert(config, dml,schemaItem.getMainTable().getAlias(), data);
                }else {
                    // 从表的操作
                    for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                        if (!tableItem.getTableName().equals(dml.getTable())) {
                            continue;
                        }
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

                            joinTableSimpleFieldOperation(config, dml, tableItem, esFieldData);
                        } else {
                            // ------关联子表简单字段插入------
                            subTableSimpleFieldOperation(config, dml, data, null, tableItem);
                        }
                    }
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
                    out:
                    for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
                        for (ColumnItem columnItem : fieldItem.getColumnItems()) {
                            if (old.containsKey(columnItem.getColumnName())) {
                                if (fieldItem.isMethod() || fieldItem.isBinaryOp()) {
                                    allUpdateFieldSimple = false;
                                    break out;
                                }
                            }
                        }
                    }
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
                } else {
                    // 从表的操作
                    for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                        if (!tableItem.getTableName().equals(dml.getTable())) {
                            continue;
                        }
                        //是否修改了关联字段
                        boolean changed = false;
                        for (SchemaItem.RelationFieldsPair relationFieldsPair : tableItem.getRelationFields()) {
                            FieldItem fieldItem = relationFieldsPair.getRightFieldItem();
                            if (old.containsKey(fieldItem.getFieldName())) {
                                changed = true;

                                break;
                            }
                        }
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
                                    esOldData.put(fieldItem.getFieldName(), old.get(fieldItem.getColumn().getColumnName()));
                                }
                            }
                            ESSyncUtil.putSqlConditionFiledByUpdate(esFieldData, config.getEsMapping(), tableItem.getAlias(), data);
                            if (changed) {
                                joinTableSecondaryUpdate(config, dml, esOldData, esFieldData, tableItem.getAlias());
                            } else {
                                joinTableSimpleFieldOperation(config, dml, tableItem, esFieldData);
                            }
                        } else {
                            // ------关联子表简单字段更新------
                            subTableSimpleFieldOperation(config, dml, data, old, tableItem);
                        }
                    }
                }
            }

            i++;
        }
    }

    private void updateBatch(ESSyncConfig config, Dml dml, List<Dml> dmls) {
        List<Map<String, Object>> dataList = new ArrayList<>();
        List<Map<String, Object>> oldList = new ArrayList<>();
        dmls.stream().forEach(dml1 -> dataList.addAll(dml1.getData()));
        dmls.stream().forEach(dml1 -> oldList.addAll(dml1.getOld()));

        if (dataList == null || dataList.isEmpty() || oldList == null || oldList.isEmpty()) {
            return;
        }
        //TODO : 修改的时候可能是同一个表 但是不同的字段的数据
        Map<String, Object> old = oldList.get(0);
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();

        if (schemaItem.getAliasTableItems().size() == 1 && schemaItem.isAllFieldsSimple()) {
            // ------单表 & 所有字段都为简单字段------
            singleTableSimpleFiledUpdateBatch(config, dml, dataList, oldList);
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

                    if (changed) {
                        for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                            fieldItem.getColumnItems()
                                    .forEach(columnItem -> old.put(columnItem.getColumnName(), null));
                        }
                    }
                }

                // 判断主键和所更新的字段是否全为简单字段
                if (idFieldSimple && allUpdateFieldSimple && !fkChanged) {
                    singleTableSimpleFiledUpdateBatch(config, dml, dataList, oldList);
                } else {
                    mainTableUpdateBatch(config, dml, schemaItem.getMainTable().getAlias(), dataList, old);
                }
            }else {
                // 从表的操作
                for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (!tableItem.getTableName().equals(dml.getTable())) {
                        continue;
                    }
                    //是否修改了关联字段
                    boolean changed = false;
                    for (SchemaItem.RelationFieldsPair relationFieldsPair : tableItem.getRelationFields()) {
                        FieldItem fieldItem = relationFieldsPair.getRightFieldItem();
                        if (old.containsKey(fieldItem.getFieldName())) {
                            changed = true;
                            break;
                        }
                    }
                     // 不是子查询
                    if (!tableItem.isSubQuery()) {
                        // ------关联表简单字段更新------
                        List<Map<String, Object>> esFieldDataList = new ArrayList<>();
                        List<Map<String, Object>> esOldDataList = new ArrayList<>();

                        for (int i = 0; i < dataList.size(); i++) {
                            Map<String, Object> esFieldData = new LinkedHashMap<>();
                            Map<String, Object> esOldData = new LinkedHashMap<>();

                            Map<String, Object> data = dataList.get(i);
                            Map<String, Object> oldData = oldList.get(i);
                            for (FieldItem fieldItem : tableItem.getRelationSelectFieldItems()) {
                                if (old.containsKey(fieldItem.getColumn().getColumnName())) {
                                    Object value = esTemplate.getValFromData(config.getEsMapping(),
                                            data,
                                            fieldItem.getFieldName(),
                                            fieldItem.getColumn().getColumnName());
                                    esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), value);
                                    esOldData.put(fieldItem.getFieldName(), oldData.get(fieldItem.getColumn().getColumnName()));
                                }
                            }
                            esFieldDataList.add(esFieldData);
                            esOldDataList.add(esOldData);
                        }
                        ESSyncUtil.putSqlConditionFiledByUpdateBatch(esFieldDataList,config.getEsMapping(),tableItem.getAlias(),dataList);
                        if(changed) {
                            joinTableSecondaryUpdateBatch(config, dml,  esOldDataList, esFieldDataList, tableItem.getAlias());
                        }else{
                            joinTableSimpleFieldOperationBatch(config, dml, tableItem, esFieldDataList);
                        }
                    }
                }
            }
        }

    }

    /**
     * 修改了次关联表的关联查询条件的处理
     * @param config
     * @param dml
     * @param old
     * @param esFieldData
     */
    private void joinTableSecondaryUpdate(ESSyncConfig config, Dml dml, Map<String, Object> old,
                                          Map<String, Object> esFieldData,String tableAlias) {
        ESMapping mapping = config.getEsMapping();

        if (logger.isDebugEnabled()) {
            logger.trace("Join table update es index by foreign key, destination:{}, table: {}, index: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index());
        }
        esTemplate.secondaryUpdateByQuery(config, esFieldData, old,tableAlias);
    }

    private void joinTableSecondaryUpdateBatch(ESSyncConfig config, Dml dml, List<Map<String, Object>> oldDataList,
                                          List<Map<String, Object>> esFieldDataList,String tableAlias) {
        ESMapping mapping = config.getEsMapping();

        if (logger.isDebugEnabled()) {
            logger.trace("Join table update es index by foreign key, destination:{}, table: {}, index: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index());
        }
        esTemplate.secondaryUpdateByQueryBatch(config, esFieldDataList, oldDataList,tableAlias);
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
            else {
                // 从表的操作
                for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                    if (!tableItem.getTableName().equals(dml.getTable())) {
                        continue;
                    }
                    // 不是子查询
                    if (!tableItem.isSubQuery()) {
                        // 删除前关联条件的旧值
                        Map<String, Object> old = new HashMap<>();
                        ESMapping esMapping = config.getEsMapping();
                        for (SchemaItem.RelationFieldsPair relationFieldsPair : tableItem.getRelationFields()) {
                            FieldItem fieldItem = relationFieldsPair.getRightFieldItem();
                            String esKey = esMapping.getColumnMapping().get(fieldItem.getExpr());
                            old.put(esKey, data.get(fieldItem.getFieldName()));
                        }
                        joinTableSecondaryDelete(config, dml, old, tableItem.getRelationSelectFieldItems(), tableItem.getAlias());
                    } else {
                        // ------关联子表简单字段更新------
                        subTableSimpleFieldOperation(config, dml, data, null, tableItem);
                    }
                }
            }
        }
    }

    private void deleteBatch(ESSyncConfig config, Dml dml, List<Dml> dmls) {
        List<Map<String, Object>> dataList = new ArrayList<>();
        dmls.stream().forEach(dml1 -> dataList.addAll(dml1.getData()));

        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        SchemaItem schemaItem = config.getEsMapping().getSchemaItem();
        ESMapping mapping = config.getEsMapping();
        // ------是主表------
        if (schemaItem.getMainTable().getTableName().equalsIgnoreCase(dml.getTable())) {
            FieldItem idFieldItem = schemaItem.getIdFieldItem(mapping);
            // 主键为简单字段
            if (!idFieldItem.isMethod() && !idFieldItem.isBinaryOp()) {
                List<Object> idValList = new ArrayList<>();
                for (Map<String,Object> data : dataList) {
                    Object idVal = esTemplate.getValFromData(mapping,
                            data,
                            idFieldItem.getFieldName(),
                            idFieldItem.getColumn().getColumnName());
                    idValList.add(idVal);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Main table delete es index, destination:{}, table: {}, index: {}, id: {}",
                                config.getDestination(),
                                dml.getTable(),
                                mapping.get_index(),
                                idVal);
                    }
                }
                esTemplate.deleteBatch(mapping, idValList);
            } else {
                // ------主键带函数, 查询sql获取主键删除------
                mainTableDeleteBatch(config, dml, dataList, schemaItem.getMainTable().getAlias());
            }
        }else {
            // 从表的操作
            for (TableItem tableItem : schemaItem.getAliasTableItems().values()) {
                if (!tableItem.getTableName().equals(dml.getTable())) {
                    continue;
                }
                // 不是子查询
                if (!tableItem.isSubQuery()) {
                    // 删除前关联条件的旧值
                    List<Map<String, Object>> oldDataList = new ArrayList<>();
                    for (Map<String, Object> data:dataList) {
                        Map<String, Object> old = new HashMap<>();
                        ESMapping esMapping = config.getEsMapping();
                        for (SchemaItem.RelationFieldsPair relationFieldsPair : tableItem.getRelationFields()) {
                            FieldItem fieldItem = relationFieldsPair.getRightFieldItem();
                            String esKey = esMapping.getColumnMapping().get(fieldItem.getExpr());
                            old.put(esKey, data.get(fieldItem.getFieldName()));
                        }
                        oldDataList.add(old);
                    }
                    joinTableSecondaryDeleteBatch(config, dml, oldDataList, tableItem.getAlias());
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

    private void joinTableSecondaryDeleteBatch(ESSyncConfig config, Dml dml, List<Map<String, Object>> oldDataList,String tableAlias) {
        ESMapping mapping = config.getEsMapping();

        if (logger.isDebugEnabled()) {
            logger.trace("Join table update es index by foreign key, destination:{}, table: {}, index: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index());
        }
        esTemplate.secondaryDeleteByQueryBatch(config, oldDataList,tableAlias);
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
     * 单表简单字段insert
     *
     * @param config es配置
     * @param dml dml信息
     * @param datas dml数据
     */
    private void singleTableSimpleFiledInsertBatch(ESSyncConfig config, Dml dml, List<Map<String, Object>> datas) {
        ESSyncConfig.ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        for(Map<String, Object> data:datas) {
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
        String sql = mapping.getSqlConditionFields().get(tableAlias).get("sql");
//        String condition = ESSyncUtil.pkConditionSql(mapping, data);
        String condition = ESSyncUtil.appendSqlConditionFieldByOriginal(mapping, data,tableAlias);
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

    /**
     * 主表(单表)复杂字段insert
     *
     * @param config es配置
     * @param dml dml信息
     * @param datas dml数据
     */
    private void mainTableInsertBatch(ESSyncConfig config, Dml dml,String tableAlias, List<Map<String, Object>> datas) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSqlConditionFields().get(tableAlias).get("sql");
        String condition = ESSyncUtil.appendSqlConditionFiledBatchByOriginal(mapping, datas,tableAlias);
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
     * 主表数据批量删除(主键带函数)
     * @param config
     * @param dml
     * @param dataList 要删除的数据
     * @param tabAlias
     */
    private void mainTableDeleteBatch(ESSyncConfig config, Dml dml, List<Map<String, Object>> dataList,String tabAlias) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
        String condition = ESSyncUtil.appendSqlConditionFiledBatchByOriginal(mapping,dataList,tabAlias);
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
                List<Object> idValList = new ArrayList<>();
                while (rs.next()) {
                    Object idVal = esTemplate.getIdValFromRS(mapping, rs);
                    idValList.add(idVal);
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                                "Main table delete to es index by query sql, destination:{}, table: {}, index: {}, id: {}",
                                config.getDestination(),
                                dml.getTable(),
                                mapping.get_index(),
                                idVal);
                    }

                }
                esTemplate.deleteBatch(mapping, idValList);
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
     * @param tableItem 当前表配置
     */
    private void joinTableSimpleFieldOperation(ESSyncConfig config, Dml dml,
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


    private void joinTableSimpleFieldOperationBatch(ESSyncConfig config, Dml dml,
                                               TableItem tableItem, List<Map<String, Object>> esFieldDataList) {
        ESMapping mapping = config.getEsMapping();

        if (logger.isDebugEnabled()) {
            logger.trace("Join table update es index by foreign key, destination:{}, table: {}, index: {}",
                    config.getDestination(),
                    dml.getTable(),
                    mapping.get_index());
        }
        esTemplate.updateByQueryBatch(config, esFieldDataList, tableItem.getAlias());
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
        String[] sqlSplit = mapping.getSqlConditionFields().get(tableItem.getAlias()).get("sql").split("GROUP\\ BY(?!(.*)ON)");
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
     * 主表单表更新
     * @param config
     * @param dml
     * @param dataList 更新后的新数据
     * @param oldList 更新前的旧数据
     */
    private void singleTableSimpleFiledUpdateBatch(ESSyncConfig config, Dml dml, List<Map<String, Object>> dataList,
                                                   List<Map<String, Object>> oldList) {
        ESMapping mapping = config.getEsMapping();
        Map<String, Object> esFieldData = new LinkedHashMap<>();

        for (int i=0; i<dataList.size(); i++) {
            Map<String, Object> data = dataList.get(i);
            Map<String, Object> old = oldList.get(i);
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
        commit();
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
        String condition = ESSyncUtil.appendSqlConditionFieldByOriginal(mapping, data, tableAlias);
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
     *
     * @param config
     * @param dml
     * @param tableAlias
     * @param dataList
     * @param oldData 主要用来判断修改的列
     */
    private void mainTableUpdateBatch(ESSyncConfig config, Dml dml,String tableAlias, List<Map<String, Object>> dataList,Map<String,Object> oldData) {
        ESMapping mapping = config.getEsMapping();
        String sql = mapping.getSql();
        String condition = ESSyncUtil.appendSqlConditionFiledBatchByOriginal(mapping, dataList, tableAlias);
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
                    Object idVal = esTemplate.getESDataFromRS(mapping, rs, oldData, esFieldData);
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
