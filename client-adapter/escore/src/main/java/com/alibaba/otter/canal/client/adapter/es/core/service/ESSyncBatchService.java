package com.alibaba.otter.canal.client.adapter.es.core.service;

import com.alibaba.otter.canal.client.adapter.es.core.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author xiangyang
 * @Description TODO
 * @createTime 2020/12/24 16:46
 */
public class ESSyncBatchService extends ESSyncService {

    private static Logger logger = LoggerFactory.getLogger(ESSyncBatchService.class);

    private ESTemplate    esTemplate;

    public ESSyncBatchService(ESTemplate esTemplate) {
        super(esTemplate);
    }


}
