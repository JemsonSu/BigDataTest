package com.jemson.kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;

/**
 * kudu API测试
 */
public class TestKudu {
    


    public static void main(String[] args) throws Exception {
        String kuduMasters = "c21:7051";
        KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMasters);
        KuduClient client = kuduClientBuilder.build();



        insertRows(client);


        client.shutdown();


    }

    /**
     * show tables
     */
    public static void showTables(KuduClient client) throws Exception {
        ListTablesResponse tablesListResponse = client.getTablesList();
        List<String> tablesList = tablesListResponse.getTablesList();
        System.out.println("kudu所有表如下:");
        for(String table : tablesList) {
            System.out.println(table);
        }

    }


    /**
     * create table
     */
    public static void createTable(KuduClient client) throws Exception {
        String tableName = "kudu_users";

        //添加两个字段id,name; 其中id为组件
        List<ColumnSchema> columns = new ArrayList<ColumnSchema>();
        columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).build());
        Schema schema = new Schema(columns);

        //建表选项
        CreateTableOptions options = new CreateTableOptions();

        List<String> hashKeys = new ArrayList<String>();
        hashKeys.add("id"); //id做hash分桶
        int numBuckets = 4;
        options.addHashPartitions(hashKeys, numBuckets);
        //options.setNumReplicas(3);

        client.createTable(tableName, schema, options);

        System.out.println("Created table \"" + tableName + "\" succeed !");

    }


    /**
     * insert data
     */
    public static void insertRows(KuduClient client) throws KuduException {
        String tableName = "kudu_users";
        KuduTable table = client.openTable(tableName);
        KuduSession session = client.newSession();

        Insert insert = table.newInsert();
        PartialRow row = insert.getRow();
        row.addInt("id",5);
        row.addString("name","大长腿");
        OperationResponse apply = session.apply(insert);
        System.out.println(apply);

        session.close();

        System.out.println("插入成功！");

    }


}
