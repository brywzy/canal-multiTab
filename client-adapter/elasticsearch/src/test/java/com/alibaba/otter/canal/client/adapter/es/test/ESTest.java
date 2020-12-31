package com.alibaba.otter.canal.client.adapter.es.test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.alibaba.otter.canal.client.adapter.support.Dml;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
@Ignore
public class ESTest {

    private TransportClient transportClient;

    @Before
    public void init() throws UnknownHostException {
        Settings.Builder settingBuilder = Settings.builder();
        settingBuilder.put("cluster.name", TestConstant.clusterName);
        Settings settings = settingBuilder.build();
        transportClient = new PreBuiltTransportClient(settings);
        String[] hostArray = TestConstant.esHosts.split(",");
        for (String host : hostArray) {
            int i = host.indexOf(":");
            transportClient.addTransportAddress(new TransportAddress(InetAddress.getByName(host.substring(0, i)),
                Integer.parseInt(host.substring(i + 1))));
        }
    }

    @Test
    public void test01() {
        SearchResponse response = transportClient.prepareSearch("test")
            .setTypes("osm")
            .setQuery(QueryBuilders.termQuery("_id", "1"))
            .setSize(10000)
            .get();
        for (SearchHit hit : response.getHits()) {
            System.out.println(hit.getSourceAsMap().get("data").getClass());
        }
    }

    @Test
    public void test02() {
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        esFieldData.put("userId", 2L);
        esFieldData.put("eventId", 4L);
        esFieldData.put("eventName", "网络异常");
        esFieldData.put("description", "第四个事件信息");

        Map<String, Object> relations = new LinkedHashMap<>();
        esFieldData.put("user_event", relations);
        relations.put("name", "event");
        relations.put("parent", "2");

        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
        bulkRequestBuilder
            .add(transportClient.prepareIndex("test", "osm", "2_4").setRouting("2").setSource(esFieldData));
        commit(bulkRequestBuilder);
    }


    @Test
    public void testidx() {
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        esFieldData.put("login_name", "bbb@qq.com");
        esFieldData.put("name", "aaa");
        esFieldData.put("email", "aaabbb@qq.com");
        esFieldData.put("phone", "123456");
        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
        bulkRequestBuilder
                .add(transportClient.prepareIndex("test_canal", "_doc", "1").setSource(esFieldData));
        commit(bulkRequestBuilder);
    }

    @Test
    public void test03() {
        Map<String, Object> esFieldData = new LinkedHashMap<>();
        esFieldData.put("userId", 2L);
        esFieldData.put("eventName", "网络异常1");

        Map<String, Object> relations = new LinkedHashMap<>();
        esFieldData.put("user_event", relations);
        relations.put("name", "event");
        relations.put("parent", "2");

        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
        bulkRequestBuilder.add(transportClient.prepareUpdate("test", "osm", "2_4").setRouting("2").setDoc(esFieldData));
        commit(bulkRequestBuilder);
    }

    @Test
    public void test04() {
        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();
        bulkRequestBuilder.add(transportClient.prepareDelete("test", "osm", "2_4"));
        commit(bulkRequestBuilder);
    }

    private void commit(BulkRequestBuilder bulkRequestBuilder) {
        if (bulkRequestBuilder.numberOfActions() > 0) {
            BulkResponse response = bulkRequestBuilder.execute().actionGet();
            if (response.hasFailures()) {
                for (BulkItemResponse itemResponse : response.getItems()) {
                    if (!itemResponse.isFailed()) {
                        continue;
                    }

                    if (itemResponse.getFailure().getStatus() == RestStatus.NOT_FOUND) {
                        System.out.println(itemResponse.getFailureMessage());
                    } else {
                        System.out.println("ES bulk commit error" + itemResponse.getFailureMessage());
                    }
                }
            }
        }
    }

    @Test
    public void test3(){

        List<Dml> dmls = new ArrayList<>();
        Dml dm1 = new Dml();
        dm1.setDatabase("ds1");
        dm1.setTable("order_item");
        dmls.add(dm1);
        Dml dm2 = new Dml();
        dm2.setDatabase("ds1");
        dm2.setTable("order_item");
        dmls.add(dm2);
        Dml dm3 = new Dml();
        dm3.setDatabase("ds1");
        dm3.setTable("order_item2");
        dmls.add(dm3);

        List<String> collect = dmls.stream().map(dml -> {
            return dml.getDatabase() + "." + dml.getTable();
        }).distinct().collect(Collectors.toList());

        System.out.println(collect);


    }

    @Test
    public void test4(){
        StringBuffer sb = new StringBuffer();
        sb.append("a , b ,");
        System.out.println(sb.length());
        sb.delete(7,7);

        System.out.println(sb);
    }

    @After
    public void after() {
        transportClient.close();
    }
}
