package com.alibaba.otter.canal.client.adapter.es7x.support;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem.ColumnItem;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest.ESBulkResponse;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest.ESDeleteRequest;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest.ESIndexRequest;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESBulkRequest.ESUpdateRequest;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.es7x.support.ESConnection.ESSearchRequest;
import com.alibaba.otter.canal.client.adapter.es7x.support.ESConnection.ESMultiSearchRequest;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Util;

public class ES7xTemplate implements ESTemplate {

    private static final Logger                               logger         = LoggerFactory
        .getLogger(ESTemplate.class);

    private static final int                                  MAX_BATCH_SIZE = 1000;

    private ESConnection                                      esConnection;

    private ESBulkRequest                                     esBulkRequest;

    // es 字段类型本地缓存
    private static ConcurrentMap<String, Map<String, String>> esFieldTypes   = new ConcurrentHashMap<>();

    public ES7xTemplate(ESConnection esConnection){
        this.esConnection = esConnection;
        this.esBulkRequest = this.esConnection.new ES7xBulkRequest();
        esBulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
    }

    public ESBulkRequest getBulk() {
        return esBulkRequest;
    }

    public void resetBulkRequestBuilder() {
        this.esBulkRequest.resetBulk();
    }

    @Override
    public void insert(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {
        if (mapping.get_id() != null) {
            String parentVal = (String) esFieldData.remove("$parent_routing");
            if (mapping.isUpsert()) {
                ESUpdateRequest updateRequest = esConnection.new ES7xUpdateRequest(mapping.get_index(),
                    pkVal.toString()).setDoc(esFieldData).setDocAsUpsert(true);
                if (StringUtils.isNotEmpty(parentVal)) {
                    updateRequest.setRouting(parentVal);
                }
                getBulk().add(updateRequest);
            } else {
                ESIndexRequest indexRequest = esConnection.new ES7xIndexRequest(mapping.get_index(), pkVal.toString())
                    .setSource(esFieldData);
                if (StringUtils.isNotEmpty(parentVal)) {
                    indexRequest.setRouting(parentVal);
                }
                getBulk().add(indexRequest);
            }
            commitBulk();
        } else {
            ESSearchRequest esSearchRequest = this.esConnection.new ESSearchRequest(mapping.get_index())
                .setQuery(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                .size(100000);
            SearchResponse response = esSearchRequest.getResponse();

            for (SearchHit hit : response.getHits()) {
                ESUpdateRequest esUpdateRequest = this.esConnection.new ES7xUpdateRequest(mapping.get_index(),
                    hit.getId()).setDoc(esFieldData);
                getBulk().add(esUpdateRequest);
                commitBulk();
            }
        }
    }

    @Override
    public void update(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {
        Map<String, Object> esFieldDataTmp = new LinkedHashMap<>(esFieldData.size());
        esFieldData.forEach((k, v) -> esFieldDataTmp.put(Util.cleanColumn(k), v));
        append4Update(mapping, pkVal, esFieldDataTmp);
        commitBulk();
    }

    @Override
    public void updateByQuery(ESSyncConfig config, Map<String, Object> paramsTmp, Map<String, Object> esFieldData,String tableName) {
        ESMapping mapping = config.getEsMapping();
        List<String> mappingFields = mapping.getSecondaryTabRelation().get(tableName);
//        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
//        paramsTmp.forEach((fieldName, value) -> queryBuilder.must(QueryBuilders.termsQuery(fieldName, value)));
        // 查询sql批量更新
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        StringBuilder sql = new StringBuilder(mapping.getSqlConditionFields().get(tableName).get("sql") + " WHERE ");

        Map<String, Object> sqlCondition = ESSyncUtil.appendSqlConditionFieldMap(config.getEsMapping(), esFieldData, tableName);
        List<Object> values = new ArrayList<>();
        sqlCondition.forEach((fieldName, value) -> {
            sql.append(fieldName).append("=? AND ");
            values.add(value);
        });
        // TODO 直接外部包裹sql会导致全表扫描性能低, 待优化拼接内部where条件
        int len = sql.length();
        sql.delete(len - 4, len);
        Integer syncCount = (Integer) Util.sqlRS(ds, sql.toString(), values, rs -> {
            int count = 0;
            try {
                while (rs.next()) {
                    for (String field : mappingFields) {
                        esFieldData.put(field, rs.getObject(field));
                    }
                    Object idVal = getIdValFromRS(mapping, rs);
                    append4Update(mapping, idVal, esFieldData);
                    commitBulk();
                    count++;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return count;
        });
        if (logger.isTraceEnabled()) {
            logger.trace("Update ES by query affected {} records", syncCount);
        }
    }

    /**
     * 次表批量更新数据
     * @param config
     * @param esFieldDataList 修改的新数据
     * @param tableAlias
     */
    @Override
    public void updateByQueryBatch(ESSyncConfig config, List<Map<String, Object>> esFieldDataList,String tableAlias) {
        ESMapping mapping = config.getEsMapping();
        List<String> mappingFields = mapping.getSecondaryTabRelation().get(tableAlias);
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        StringBuilder sql = new StringBuilder(mapping.getSqlConditionFields().get(tableAlias).get("sql") + " WHERE ");
        String sqlCondition =  appendSqlConditionFiledBatch(config.getEsMapping(), esFieldDataList, tableAlias);
        sql.append(sqlCondition);
        Integer syncCount = (Integer) Util.sqlRS(ds, sql.toString(), null, rs -> {
            int count = 0;
            try {
                while (rs.next()) {
                    Map<String, Object> updateDate = new HashMap<>();
                    for (String field : mappingFields) {
                        updateDate.put(field, rs.getObject(field));
                    }
                    Object idVal = getIdValFromRS(mapping, rs);
                    append4Update(mapping, idVal, updateDate);
                    count++;
                }
                commit();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return count;
        });
        if (logger.isTraceEnabled()) {
            logger.trace("Update ES by query affected {} records", syncCount);
        }
    }

    /**
     * 修改次表的关联条件时的处理
     * @param config
     * @param esFieldData
     * @param old
     * @param tableAlias
     */
    @Override
    public void secondaryUpdateByQuery(ESSyncConfig config, Map<String, Object> esFieldData, Map<String, Object> old, String tableAlias) {
        ESMapping esMapping = config.getEsMapping();
        List<String> mappingFields = esMapping.getSecondaryTabRelation().get(tableAlias);
        // 查询sql批量更新
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        StringBuilder sql = new StringBuilder(esMapping.getSqlConditionFields().get(tableAlias).get("sql") + " WHERE ");
        Map<String, Object> sqlCondition = ESSyncUtil.appendSqlConditionFieldMap(config.getEsMapping(), esFieldData, tableAlias);
        List<Object> values = new ArrayList<>();
        sqlCondition.forEach((fieldName, value) -> {
            sql.append(fieldName).append("=? AND ");
            values.add(value);
        });
        int len = sql.length();
        sql.delete(len - 4, len);
        Integer syncCount = (Integer) Util.sqlRS(ds, sql.toString(), values, rs -> {
            int count = 0;
            try {
                while (rs.next()) {
                    Map<String,Object> esData = new HashMap<>();
                    for (String field : mappingFields) {
                        esData.put(field, rs.getObject(field));
                    }
                    Object idVal = getIdValFromRS(esMapping, rs);
                    append4Update(esMapping, idVal, esData);
                    commit();
                    count++;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return count;
        });

        String index = esMapping.get_index();
        ESSearchRequest esSearchRequest = this.esConnection.new ESSearchRequest(index);
        esSearchRequest.size(100000);
        BoolQueryBuilder mustQuery = QueryBuilders.boolQuery();
        for (Map.Entry<String, Object> entry : old.entrySet()){
            mustQuery.filter(QueryBuilders.termQuery(entry.getKey(),entry.getValue()));
        }
        esSearchRequest.setQuery(mustQuery);
        SearchResponse response = esSearchRequest.getResponse();
        SearchHit[] hits = response.getHits().getHits();

        if (hits.length==0){
            return;
        }
        for (SearchHit searchHit : hits){
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();

            for (String field:mappingFields) {
                sourceAsMap.put(field, null);
            }

            ESConnection.ES7xUpdateRequest esUpdateRequest = this.esConnection.new ES7xUpdateRequest(index, searchHit.getId());
            esUpdateRequest.setDoc(sourceAsMap);
            getBulk().add(esUpdateRequest);
        }
        commit();

        if (logger.isTraceEnabled()) {
            logger.trace("Update ES by query affected {} records", syncCount);
        }
    }

    /**
     * 次表更新，有关联字段被修改
     * @param config
     * @param esFieldDataList 关联字段更新后的新数据
     * @param oldDataList 关联字段更新前的旧数据
     * @param tableAlias
     */
    @Override
    public void secondaryUpdateByQueryBatch(ESSyncConfig config, List<Map<String, Object>> esFieldDataList, List<Map<String, Object>> oldDataList, String tableAlias) {
        ESMapping esMapping = config.getEsMapping();
        List<String> mappingFields = esMapping.getSecondaryTabRelation().get(tableAlias);
        // 查询sql批量更新
        DataSource ds = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        StringBuilder sql = new StringBuilder(esMapping.getSqlConditionFields().get(tableAlias).get("sql") + " WHERE ");
        String sqlCondition = appendSqlConditionFiledBatch(config.getEsMapping(), esFieldDataList, tableAlias);
        sql.append(sqlCondition);
        Integer syncCount = (Integer) Util.sqlRS(ds, sql.toString(), null, rs -> {
            int count = 0;
            try {
                while (rs.next()) {
                    Map<String,Object> esData = new HashMap<>();
                    for (String field : mappingFields) {
                        esData.put(field, rs.getObject(field));
                    }
                    Object idVal = getIdValFromRS(esMapping, rs);
                    append4Update(esMapping, idVal, esData);
                    commit();
                    count++;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return count;
        });

        String index = esMapping.get_index();
        ESMultiSearchRequest esMultiSearchRequest = this.esConnection.new ESMultiSearchRequest();
        for (Map<String,Object> oldData:oldDataList ) {
            ESSearchRequest esSearchRequest = this.esConnection.new ESSearchRequest(index);
            esSearchRequest.size(100000);
            BoolQueryBuilder mustQuery = QueryBuilders.boolQuery();
            for (Map.Entry<String, Object> entry : oldData.entrySet()) {
                mustQuery.filter(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
            }
            esSearchRequest.setQuery(mustQuery);
            esMultiSearchRequest.addMultiSearchRequest(esSearchRequest);
        }
        List<SearchHit> allSearchHit = esMultiSearchRequest.mSearch();
        if (allSearchHit.size()==0){
            return;
        }
        for (SearchHit searchHit : allSearchHit){
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            for (String field:mappingFields) {
                sourceAsMap.put(field, null);
            }
            ESConnection.ES7xUpdateRequest esUpdateRequest = this.esConnection.new ES7xUpdateRequest(index, searchHit.getId());
            esUpdateRequest.setDoc(sourceAsMap);
            getBulk().add(esUpdateRequest);
        }
        commit();

        if (logger.isTraceEnabled()) {
            logger.trace("Update ES by query affected {} records", syncCount);
        }
    }

    /**
     * 删除次表时的处理,不考虑一对多的场景
     * @param config
     * @param old
     * @param relationSelectFieldItems
     */
    @Override
    public void secondaryDeleteByQuery(ESSyncConfig config, Map<String, Object> old, List<FieldItem> relationSelectFieldItems,String tableAlias) {
        ESMapping esMapping = config.getEsMapping();
        String index = esMapping.get_index();
        ESSearchRequest esSearchRequest = this.esConnection.new ESSearchRequest(index);
        esSearchRequest.size(100000);
        BoolQueryBuilder mustQuery = QueryBuilders.boolQuery();
        for (Map.Entry<String, Object> entry : old.entrySet()){
            mustQuery.filter(QueryBuilders.termQuery(entry.getKey(),entry.getValue()));
        }
        esSearchRequest.setQuery(mustQuery);
        SearchResponse response = esSearchRequest.getResponse();
        SearchHit[] hits = response.getHits().getHits();
        if (hits.length==0){
            return;
        }
        List<String> mappingFields = esMapping.getSecondaryTabRelation().get(tableAlias);
        for (SearchHit searchHit : hits){

            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            for (String field: mappingFields) {
                sourceAsMap.put(field, null);
            }

            ESUpdateRequest esUpdateRequest = this.esConnection.new ES7xUpdateRequest(index, searchHit.getId());
            esUpdateRequest.setDoc(sourceAsMap);
            getBulk().add(esUpdateRequest);
        }
        commit();

        if (logger.isTraceEnabled()) {
            logger.trace("Update ES by query affected {} records", hits.length);
        }

    }

    /**
     * 次表删除
     * @param config
     * @param oldDataList 删除前的旧数据
     * @param tableAlias
     */
    @Override
    public void secondaryDeleteByQueryBatch(ESSyncConfig config, List<Map<String, Object>> oldDataList, String tableAlias) {
        ESMapping esMapping = config.getEsMapping();
        String index = esMapping.get_index();

        ESMultiSearchRequest esMultiSearchRequest = this.esConnection.new ESMultiSearchRequest();
        for (Map<String,Object> oldData:oldDataList ) {
            ESSearchRequest esSearchRequest = this.esConnection.new ESSearchRequest(index);
            esSearchRequest.size(100000);
            BoolQueryBuilder mustQuery = QueryBuilders.boolQuery();
            for (Map.Entry<String, Object> entry : oldData.entrySet()) {
                mustQuery.filter(QueryBuilders.termQuery(entry.getKey(), entry.getValue()));
            }
            esSearchRequest.setQuery(mustQuery);
            esMultiSearchRequest.addMultiSearchRequest(esSearchRequest);
        }
        List<SearchHit> searchHitList = esMultiSearchRequest.mSearch();
        if (searchHitList.size()==0) return;
        List<String> mappingFields = esMapping.getSecondaryTabRelation().get(tableAlias);
        for (SearchHit searchHit : searchHitList){
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            for (String field: mappingFields) {
                sourceAsMap.put(field, null);
            }
            ESUpdateRequest esUpdateRequest = this.esConnection.new ES7xUpdateRequest(index, searchHit.getId());
            esUpdateRequest.setDoc(sourceAsMap);
            getBulk().add(esUpdateRequest);
        }
        commit();

        if (logger.isTraceEnabled()) {
            logger.trace("Update ES by query affected {} records", searchHitList.size());
        }

    }

    @Override
    public void delete(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {
        if (mapping.get_id() != null) {
            ESDeleteRequest esDeleteRequest = this.esConnection.new ES7xDeleteRequest(mapping.get_index(),
                pkVal.toString());
            getBulk().add(esDeleteRequest);
            commitBulk();
        } else {
            ESSearchRequest esSearchRequest = this.esConnection.new ESSearchRequest(mapping.get_index())
                .setQuery(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                .size(100000);
            SearchResponse response = esSearchRequest.getResponse();
            for (SearchHit hit : response.getHits()) {
                ESUpdateRequest esUpdateRequest = this.esConnection.new ES7xUpdateRequest(mapping.get_index(),
                    hit.getId()).setDoc(esFieldData);
                getBulk().add(esUpdateRequest);
                commitBulk();
            }
        }
    }

    @Override
    public void deleteBatch(ESMapping mapping, List<Object> pkValList){
        for (Object pkVal : pkValList) {
            ESDeleteRequest esDeleteRequest = this.esConnection.new ES7xDeleteRequest(mapping.get_index(),
                    pkVal.toString());
            getBulk().add(esDeleteRequest);
        }
        commitBulk();
    }

    @Override
    public void commit() {
        if (getBulk().numberOfActions() > 0) {
            ESBulkResponse response = getBulk().bulk();
            if (response.hasFailures()) {
                response.processFailBulkResponse("ES sync commit error ");
            }
            resetBulkRequestBuilder();
        }
    }

    @Override
    public Object getValFromRS(ESMapping mapping, ResultSet resultSet, String fieldName,
                               String columnName) throws SQLException {
        fieldName = Util.cleanColumn(fieldName);
        columnName = Util.cleanColumn(columnName);
        String esType = getEsType(mapping, fieldName);

        Object value = resultSet.getObject(columnName);
        if (value instanceof Boolean) {
            if (!"boolean".equals(esType)) {
                value = resultSet.getByte(columnName);
            }
        }
        // 如果是对象类型
        if (mapping.getObjFields().containsKey(fieldName)) {
            return ESSyncUtil.convertToEsObj(value, mapping.getObjFields().get(fieldName));
        } else {
            return ESSyncUtil.typeConvert(value, esType);
        }
    }

    @Override
    public Object getESDataFromRS(ESMapping mapping, ResultSet resultSet,
                                  Map<String, Object> esFieldData) throws SQLException {
        SchemaItem schemaItem = mapping.getSchemaItem();
        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            Object value = getValFromRS(mapping, resultSet, fieldItem.getFieldName(), fieldItem.getFieldName());

            if (fieldItem.getFieldName().equals(idFieldName)) {
                resultIdVal = value;
            }

            if (!fieldItem.getFieldName().equals(mapping.get_id())
                && !mapping.getSkips().contains(fieldItem.getFieldName())) {
                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), value);
            }
        }

        // 添加父子文档关联信息
        putRelationDataFromRS(mapping, schemaItem, resultSet, esFieldData);

        return resultIdVal;
    }

    @Override
    public Object getIdValFromRS(ESMapping mapping, ResultSet resultSet) throws SQLException {
        SchemaItem schemaItem = mapping.getSchemaItem();
        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            Object value = getValFromRS(mapping, resultSet, fieldItem.getFieldName(), fieldItem.getFieldName());

            if (fieldItem.getFieldName().equals(idFieldName)) {
                resultIdVal = value;
                break;
            }
        }
        return resultIdVal;
    }

    @Override
    public Object getESDataFromRS(ESMapping mapping, ResultSet resultSet, Map<String, Object> dmlOld,
                                  Map<String, Object> esFieldData) throws SQLException {
        SchemaItem schemaItem = mapping.getSchemaItem();
        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            if (fieldItem.getFieldName().equals(idFieldName)) {
                resultIdVal = getValFromRS(mapping, resultSet, fieldItem.getFieldName(), fieldItem.getFieldName());
            }

            for (ColumnItem columnItem : fieldItem.getColumnItems()) {
                if (dmlOld.containsKey(columnItem.getColumnName())
                    && !mapping.getSkips().contains(fieldItem.getFieldName())) {
                    esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()),
                        getValFromRS(mapping, resultSet, fieldItem.getFieldName(), fieldItem.getFieldName()));
                    break;
                }
            }
        }

        // 添加父子文档关联信息
        putRelationDataFromRS(mapping, schemaItem, resultSet, esFieldData);

        return resultIdVal;
    }

    @Override
    public Object getValFromData(ESMapping mapping, Map<String, Object> dmlData, String fieldName, String columnName) {
        String esType = getEsType(mapping, fieldName);
        Object value = dmlData.get(columnName);
        if (value instanceof Byte) {
            if ("boolean".equals(esType)) {
                value = ((Byte) value).intValue() != 0;
            }
        }

        // 如果是对象类型
        if (mapping.getObjFields().containsKey(fieldName)) {
            return ESSyncUtil.convertToEsObj(value, mapping.getObjFields().get(fieldName));
        } else {
            return ESSyncUtil.typeConvert(value, esType);
        }
    }

    @Override
    public Object getESDataFromDmlData(ESMapping mapping, Map<String, Object> dmlData,
                                       Map<String, Object> esFieldData) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            String columnName = fieldItem.getColumnItems().iterator().next().getColumnName();
            Object value = getValFromData(mapping, dmlData, fieldItem.getFieldName(), columnName);

            if (fieldItem.getFieldName().equals(idFieldName)) {
                resultIdVal = value;
            }

            if (!fieldItem.getFieldName().equals(mapping.get_id())
                && !mapping.getSkips().contains(fieldItem.getFieldName())) {
                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()), value);
            }
        }

        // 添加父子文档关联信息
        putRelationData(mapping, schemaItem, dmlData, esFieldData);
        return resultIdVal;
    }

    @Override
    public Object getESDataFromDmlData(ESMapping mapping, Map<String, Object> dmlData, Map<String, Object> dmlOld,
                                       Map<String, Object> esFieldData) {
        SchemaItem schemaItem = mapping.getSchemaItem();
        String idFieldName = mapping.get_id() == null ? mapping.getPk() : mapping.get_id();
        Object resultIdVal = null;
        for (FieldItem fieldItem : schemaItem.getSelectFields().values()) {
            String columnName = fieldItem.getColumnItems().iterator().next().getColumnName();

            if (fieldItem.getFieldName().equals(idFieldName)) {
                resultIdVal = getValFromData(mapping, dmlData, fieldItem.getFieldName(), columnName);
            }

            if (dmlOld.containsKey(columnName) && !mapping.getSkips().contains(fieldItem.getFieldName())) {
                esFieldData.put(Util.cleanColumn(fieldItem.getFieldName()),
                        getValFromData(mapping, dmlData, fieldItem.getFieldName(), columnName));
            }
        }

        // 添加父子文档关联信息
        putRelationData(mapping, schemaItem, dmlOld, esFieldData);
        return resultIdVal;
    }

    /**
     * 如果大于批量数则提交批次
     */
    private void commitBulk() {
        if (getBulk().numberOfActions() >= MAX_BATCH_SIZE) {
            commit();
        }
    }

    private void append4Update(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData) {
        if (mapping.get_id() != null) {
            String parentVal = (String) esFieldData.remove("$parent_routing");
            if (mapping.isUpsert()) {
                ESUpdateRequest esUpdateRequest = this.esConnection.new ES7xUpdateRequest(mapping.get_index(),
                    pkVal.toString()).setDoc(esFieldData).setDocAsUpsert(true);
                if (StringUtils.isNotEmpty(parentVal)) {
                    esUpdateRequest.setRouting(parentVal);
                }
                getBulk().add(esUpdateRequest);
            } else {
                ESUpdateRequest esUpdateRequest = this.esConnection.new ES7xUpdateRequest(mapping.get_index(),
                    pkVal.toString()).setDoc(esFieldData);
                if (StringUtils.isNotEmpty(parentVal)) {
                    esUpdateRequest.setRouting(parentVal);
                }
                getBulk().add(esUpdateRequest);
            }
        } else {
            ESSearchRequest esSearchRequest = this.esConnection.new ESSearchRequest(mapping.get_index())
                .setQuery(QueryBuilders.termQuery(mapping.getPk(), pkVal))
                .size(100000);
            SearchResponse response = esSearchRequest.getResponse();
            for (SearchHit hit : response.getHits()) {
                ESUpdateRequest esUpdateRequest = this.esConnection.new ES7xUpdateRequest(mapping.get_index(),
                    hit.getId()).setDoc(esFieldData);
                getBulk().add(esUpdateRequest);
            }
        }
    }

    /**
     * 获取es mapping中的属性类型
     *
     * @param mapping mapping配置
     * @param fieldName 属性名
     * @return 类型
     */
    @SuppressWarnings("unchecked")
    private String getEsType(ESMapping mapping, String fieldName) {
        String key = mapping.get_index() + "-" + mapping.get_type();
        Map<String, String> fieldType = esFieldTypes.get(key);
        if (fieldType != null) {
            return fieldType.get(fieldName);
        } else {
            MappingMetaData mappingMetaData = esConnection.getMapping(mapping.get_index());

            if (mappingMetaData == null) {
                throw new IllegalArgumentException("Not found the mapping info of index: " + mapping.get_index());
            }

            fieldType = new LinkedHashMap<>();

            Map<String, Object> sourceMap = mappingMetaData.getSourceAsMap();
            Map<String, Object> esMapping = (Map<String, Object>) sourceMap.get("properties");
            for (Map.Entry<String, Object> entry : esMapping.entrySet()) {
                Map<String, Object> value = (Map<String, Object>) entry.getValue();
                if (value.containsKey("properties")) {
                    fieldType.put(entry.getKey(), "object");
                } else {
                    fieldType.put(entry.getKey(), (String) value.get("type"));
                }
            }
            esFieldTypes.put(key, fieldType);

            return fieldType.get(fieldName);
        }
    }

    private void putRelationDataFromRS(ESMapping mapping, SchemaItem schemaItem, ResultSet resultSet,
                                       Map<String, Object> esFieldData) {
        // 添加父子文档关联信息
        if (!mapping.getRelations().isEmpty()) {
            mapping.getRelations().forEach((relationField, relationMapping) -> {
                Map<String, Object> relations = new HashMap<>();
                relations.put("name", relationMapping.getName());
                if (StringUtils.isNotEmpty(relationMapping.getParent())) {
                    FieldItem parentFieldItem = schemaItem.getSelectFields().get(relationMapping.getParent());
                    Object parentVal;
                    try {
                        parentVal = getValFromRS(mapping,
                            resultSet,
                            parentFieldItem.getFieldName(),
                            parentFieldItem.getFieldName());
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                    if (parentVal != null) {
                        relations.put("parent", parentVal.toString());
                        esFieldData.put("$parent_routing", parentVal.toString());

                    }
                }
                esFieldData.put(relationField, relations);
            });
        }
    }

    private void putRelationData(ESMapping mapping, SchemaItem schemaItem, Map<String, Object> dmlData,
                                 Map<String, Object> esFieldData) {
        // 添加父子文档关联信息
        if (!mapping.getRelations().isEmpty()) {
            mapping.getRelations().forEach((relationField, relationMapping) -> {
                Map<String, Object> relations = new HashMap<>();
                relations.put("name", relationMapping.getName());
                if (StringUtils.isNotEmpty(relationMapping.getParent())) {
                    FieldItem parentFieldItem = schemaItem.getSelectFields().get(relationMapping.getParent());
                    String columnName = parentFieldItem.getColumnItems().iterator().next().getColumnName();
                    Object parentVal = getValFromData(mapping, dmlData, parentFieldItem.getFieldName(), columnName);
                    if (parentVal != null) {
                        relations.put("parent", parentVal.toString());
                        esFieldData.put("$parent_routing", parentVal.toString());

                    }
                }
                esFieldData.put(relationField, relations);
            });
        }
    }

    private String appendSqlConditionFiledBatch(ESMapping mapping, List<Map<String, Object>> dataList,String tableName) {
        Map<String,Map<String,String>> sqlConditionFields = mapping.getSqlConditionFields();
        Map<String,String> sqlConditionFiled = sqlConditionFields.get(tableName);

        StringBuffer sb = new StringBuffer();
        sb.append(sqlConditionFiled.get("column") + " in (" );
        String alias = sqlConditionFiled.get("alias");
        String esType = getEsType(mapping, alias);
        for (Map<String,Object> data : dataList) {
            if (esType.equalsIgnoreCase("keyword")){
                sb.append("'"+data.get(alias) + "' ,");
            }else {
                sb.append(data.get(alias) + " ,");
            }
        }
        int length = sb.length();
        sb.delete(length-1,length);
        sb.append(" )");
        return sb.toString();
    }
}
