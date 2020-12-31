
import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.otter.canal.client.adapter.es.core.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.Util;
import org.junit.Before;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author xiangyang
 * @Description TODO
 * @createTime 2020/12/28 16:26
 */
public class TestDML {

    @Test
    public void test01(){
        Dml dml01 = initDataDml01();
        Dml dml02 = initDataDml02();

        System.out.println(dml01.hashCode()+"\t "+dml02.hashCode());
        System.out.println(dml01.equals(dml02));

        Set<Dml> dml = new HashSet<>();
        dml.add(dml01);
        dml.add(dml02);


        System.out.println(dml.size());
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
        dml01.setType("detele");
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

        List<Map<String,Object>> oldList01 = new ArrayList<>();
        HashMap<String, Object> oldMap01 = new HashMap<>();
        oldMap01.put("id","01010101");
        oldMap01.put("age","01");
        oldMap01.put("sex","02");
        oldList01.add(oldMap01);
        dml01.setOld(oldList01);

        dml01.setData(list01);

        return dml01;
    }

    public Dml initDataDml02(){
        Dml dml01 = new Dml();
        dml01.setDestination("example");
        dml01.setTable("order_item");
        dml01.setDatabase("order-center");
        dml01.setGroupId("es7");
        dml01.setType("update");
        HashMap<String, Object> map01 = new HashMap<>();
        map01.put("id","020202");
        map01.put("name","zs02");
        map01.put("age","02");

        HashMap<String, Object> map01_2 = new HashMap<>();
        map01_2.put("id","02020202_2");
        map01_2.put("name","zs02_2");
        map01_2.put("age","02_2");
        List<Map<String,Object>> list01 = new ArrayList<>();
        list01.add(map01);
        list01.add(map01_2);
        dml01.setData(list01);

        List<Map<String,Object>> oldList01 = new ArrayList<>();
        HashMap<String, Object> oldMap01 = new HashMap<>();
        oldMap01.put("id","0202002");
        oldMap01.put("age","02");
        oldMap01.put("sex","02");
        oldList01.add(oldMap01);
        dml01.setOld(oldList01);

        return dml01;
    }

    private DruidDataSource dataSource;
    @Before
    public void init(){
        dataSource = new DruidDataSource();
        dataSource.setUrl("jdbc:mysql://localhost:3306/order_center?useUnicode=true");
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
    }
    @Test
    public void test02() throws SQLException, IOException, InterruptedException {

//        File file = new File("C:\\Users\\Administrator\\Desktop\\canal\\multi\\order_item.sql");
//        File file = new File("C:\\Users\\Administrator\\Desktop\\canal\\multi\\order_business.sql");
        File file = new File("C:\\Users\\Administrator\\Desktop\\canal\\multi\\user_little.sql");
//        File file = new File("C:\\Users\\Administrator\\Desktop\\canal\\multi\\invoice_order_item.sql");
//        File file = new File("C:\\Users\\Administrator\\Desktop\\canal\\multi\\order_common.sql");
//        File file = new File("C:\\Users\\Administrator\\Desktop\\canal\\multi\\order_broker.sql");

        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        System.out.println("开始写入数据："+new Date());

        DruidPooledConnection connection = dataSource.getConnection();
        connection.setAutoCommit(false);
        int i=0, n = new Random().nextInt(10)+50;
        String line = null;
        Statement preparedStatement = connection.createStatement();
        while ((line = bufferedReader.readLine())!=null){
            i++;
//            preparedStatement = connection.prepareStatement(line);
//            preparedStatement.executeUpdate();
            preparedStatement.addBatch(line);


            if (i%n==0){
                preparedStatement.executeBatch();
                connection.commit();
                n = new Random().nextInt(10)+50;
            }

//            preparedStatement.close();
//            TimeUnit.SECONDS.sleep(1);
        }
        preparedStatement.executeBatch();
        connection.commit();
        connection.close();

        System.out.println("写入数据完成："+new Date());

    }
}
