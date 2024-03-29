package com.alibaba.otter.canal.client.adapter.es7x.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import com.alibaba.otter.canal.client.adapter.support.Dml;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ES7xTest {

    @SuppressWarnings("deprecation")
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
        bulkRequestBuilder.add(transportClient.prepareIndex("test", "osm", "2_4")
            .setRouting("2")
            .setSource(esFieldData));
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
    public void testbyid() throws IOException {
        SearchRequest order_item = new SearchRequest("order_item");
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.rangeQuery("ob_id").gte(2000124));
        order_item.source(builder);
        SearchResponse searchResponse = transportClient.search(order_item).actionGet();
        SearchHit[] hits = searchResponse.getHits().getHits();
        File file = new File("E:\\tmp/aa.txt");
        FileWriter fileWriter = new FileWriter(file.getAbsoluteFile());

        for (SearchHit searchHit : hits){
//            System.out.println(searchHit.getId());
            fileWriter.write(searchHit.getId()+"\r\n");
        }
        fileWriter.close();
    }

    @Test
    public void test05(){
        Map<String,Integer> map = new HashMap<>();
        System.out.println(map.get("bb"));
    }


    @Test
    public void test06(){
        Dml dml01 = initDataDml01();
        Dml dml02 = initDataDml01();

        Set<Dml> dml = new HashSet<>();
        dml.add(dml01);
        dml.add(dml02);

        System.out.println(dml);

        /*Set<String> a1 = new HashSet<>();
        a1.add("s1");
        Set<String> a2 = new HashSet<>();
        a2.add("s1");

        a1.addAll(a2);
        System.out.println(a1);*/
    }


    public Dml initDataDml01(){
        Dml dml01 = new Dml();
        dml01.setDestination("example");
        dml01.setTable("order_item");
        dml01.setDatabase("order-center");
        dml01.setGroupId("es7");
        dml01.setType("insert");
        HashMap<String, Object> map01 = new HashMap<>();
        map01.put("id","01010101");
        map01.put("name","zs01");
        map01.put("age","01");
        HashMap<String, Object> map01_2 = new HashMap<>();
        map01_2.put("id","01010101_2");
        map01_2.put("name","zs01_2");
        map01_2.put("age","01_2");
        List<Map<String,Object>> list01 = new ArrayList<>();
        list01.add(map01);
        list01.add(map01_2);
        dml01.setData(list01);
        return dml01;
    }

    public Dml initDataDml02(){
        Dml dml01 = new Dml();
        dml01.setDestination("example");
        dml01.setTable("order_item");
        dml01.setDatabase("order-center");
        dml01.setGroupId("es7");
        dml01.setType("insert");
        HashMap<String, Object> map01 = new HashMap<>();
        map01.put("id","01010101");
        map01.put("name","zs01");
        map01.put("age","01");
        HashMap<String, Object> map01_2 = new HashMap<>();
        map01_2.put("id","01010101_2");
        map01_2.put("name","zs01_2");
        map01_2.put("age","01_2");
        List<Map<String,Object>> list01 = new ArrayList<>();
        list01.add(map01);
        list01.add(map01_2);
        dml01.setData(list01);
        return dml01;
    }

    @After
    public void after() {
        transportClient.close();
    }



}
