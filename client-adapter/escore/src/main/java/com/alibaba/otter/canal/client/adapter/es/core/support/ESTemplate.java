package com.alibaba.otter.canal.client.adapter.es.core.support;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.core.config.SchemaItem;

public interface ESTemplate {

    /**
     * 插入数据
     *
     * @param mapping 配置对象
     * @param pkVal 主键值
     * @param esFieldData 数据Map
     */
    void insert(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData);

    /**
     * 根据主键更新数据
     *
     * @param mapping 配置对象
     * @param pkVal 主键值
     * @param esFieldData 数据Map
     */
    void update(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData);

    /**
     * update by query
     *
     * @param config 配置对象
     * @param paramsTmp sql查询条件
     * @param esFieldData 数据Map
     */
    void updateByQuery(ESSyncConfig config, Map<String, Object> paramsTmp, Map<String, Object> esFieldData, String tableName);

    void updateByQueryBatch(ESSyncConfig config, List<Map<String, Object>> allEsFieldData,String tableAlias);

    void secondaryUpdateByQuery(ESSyncConfig config, Map<String, Object> esFieldData, Map<String, Object> old, String tableAlias);

    void secondaryUpdateByQueryBatch(ESSyncConfig config, List<Map<String, Object>> esFieldDataList, List<Map<String, Object>> oldDataList, String tableAlias);

    void secondaryDeleteByQuery(ESSyncConfig config, Map<String, Object> old, List<SchemaItem.FieldItem> relationSelectFieldItems,String tableAlias);

    void secondaryDeleteByQueryBatch(ESSyncConfig config, List<Map<String, Object>> oldDataList,String tableAlias);
    /**
     * 通过主键删除数据
     *
     * @param mapping 配置对象
     * @param pkVal 主键值
     * @param esFieldData 数据Map
     */
    void delete(ESMapping mapping, Object pkVal, Map<String, Object> esFieldData);

    void deleteBatch(ESMapping mapping, List<Object> pkValList);

    /**
     * 提交批次
     */
    void commit();

    Object getValFromRS(ESMapping mapping, ResultSet resultSet, String fieldName,
                        String columnName) throws SQLException;

    Object getESDataFromRS(ESMapping mapping, ResultSet resultSet, Map<String, Object> esFieldData) throws SQLException;

    Object getIdValFromRS(ESMapping mapping, ResultSet resultSet) throws SQLException;

    Object getESDataFromRS(ESMapping mapping, ResultSet resultSet, Map<String, Object> dmlOld,
                           Map<String, Object> esFieldData) throws SQLException;

    Object getValFromData(ESMapping mapping, Map<String, Object> dmlData, String fieldName, String columnName);

    Object getESDataFromDmlData(ESMapping mapping, Map<String, Object> dmlData, Map<String, Object> esFieldData);

    Object getESDataFromDmlData(ESMapping mapping, Map<String, Object> dmlData, Map<String, Object> dmlOld,
                                Map<String, Object> esFieldData);
}
