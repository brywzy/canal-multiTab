package com.alibaba.otter.canal.client.adapter.support;

import java.io.Serializable;
import java.util.*;

/**
 * DML操作转换对象
 *
 * @author rewerma 2018-8-19 下午11:30:49
 * @version 1.0.0
 */
public class Dml implements Serializable {

    private static final long         serialVersionUID = 2611556444074013268L;

    private String                    destination;                            // 对应canal的实例或者MQ的topic
    private String                    groupId;                                // 对应mq的group id
    private String                    database;                               // 数据库或schema
    private String                    table;                                  // 表名
    private List<String>              pkNames;
    private Boolean                   isDdl;
    private String                    type;                                   // 类型: INSERT UPDATE DELETE
    // binlog executeTime
    private Long                      es;                                     // 执行耗时
    // dml build timeStamp
    private Long                      ts;                                     // 同步时间
    private String                    sql;                                    // 执行的sql, dml sql为空
    private List<Map<String, Object>> data;                                   // 数据列表
    private List<Map<String, Object>> old;                                    // 旧数据列表, 用于update, size和data的size一一对应

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getPkNames() {
        return pkNames;
    }

    public void setPkNames(List<String> pkNames) {
        this.pkNames = pkNames;
    }

    public Boolean getIsDdl() {
        return isDdl;
    }

    public void setIsDdl(Boolean isDdl) {
        this.isDdl = isDdl;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    public List<Map<String, Object>> getOld() {
        return old;
    }

    public void setOld(List<Map<String, Object>> old) {
        this.old = old;
    }

    public Long getEs() {
        return es;
    }

    public void setEs(Long es) {
        this.es = es;
    }

    public void clear() {
        database = null;
        table = null;
        type = null;
        ts = null;
        es = null;
        data = null;
        old = null;
        sql = null;
    }

    @Override
    public String toString() {
        return "Dml{" + "destination='" + destination + '\'' + ", database='" + database + '\'' + ", table='" + table
               + '\'' + ", type='" + type + '\'' + ", es=" + es + ", ts=" + ts + ", sql='" + sql + '\'' + ", data="
               + data + ", old=" + old + '}';
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof Dml) {
            Dml dml = (Dml)obj;
            if (type.equals(dml.getType()) && database.equals(dml.getDatabase())
                    && table.equals(dml.getTable()) && destination.equals(dml.getDestination())
                    && groupId!=null && groupId.equals(dml.getGroupId())){

                Set<String> keySet = data.get(0).keySet();
                Set<String> objKeySet = dml.getData().get(0).keySet();

                if (keySet.size() != objKeySet.size()) {
                    return false;
                } else {
                    for (String key : keySet){
                        if (!objKeySet.contains(key)) return false;
                    }
                }
                if (type.equalsIgnoreCase("update")){
                    Set<String> oldKeySet = old.get(0).keySet();
                    Set<String> objOldKeySet = dml.getOld().get(0).keySet();
                    if (oldKeySet.size() != objOldKeySet.size()) {
                        return false;
                    } else {
                        for (String key : oldKeySet){
                            if (!objOldKeySet.contains(key)) return false;
                        }
                    }
                }
            }
            return true;
        }
        return false;

    }

    @Override
    public int hashCode() {
        int result = 1;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (database != null ? database.hashCode() : 0);
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + (destination != null ? destination.hashCode() : 0);
        result = 31 * result + (groupId != null ? groupId.hashCode() : 0);
        Set<String> keys = this.getData().get(0).keySet();
        for (String key : keys) {
            result = 31 * result + (key != null ? key.hashCode() : 0);
        }
        if (type.equalsIgnoreCase("update")){
            Set<String> oldKeys = this.getData().get(0).keySet();
            for (String key : oldKeys) {
                result = 31 * result + (key != null ? key.hashCode() : 0);
            }
        }
        return result;
    }
}
