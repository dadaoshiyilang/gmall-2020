package com.atguigu;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import java.io.IOException;

public class BulkWriter {

    public static void main(String[] args) {

        //1.创建工厂对象
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.设置参数
        HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(clientConfig);

        //3.获取ES客户端对象
        JestClient jestClient = jestClientFactory.getObject();

        // 创建多个Index对象
        Stu a = new Stu("A", 18);
        Stu b = new Stu("B", 38);
        Stu c = new Stu("C", 38);

        Index index1 = new Index.Builder(a).id("1001").build();
        Index index2 = new Index.Builder(b).id("1002").build();
        Index index3 = new Index.Builder(c).id("1003").build();

        //5.创建Bulk对象
        Bulk bulk = new Bulk.Builder()
                .defaultIndex("stu")
                .defaultType("_doc")
                .addAction(index1)
                .addAction(index2)
                .addAction(index3)
                .build();

        //6.指定批量插入数据操作
        try {
            jestClient.execute(bulk);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //7.关闭连接
        jestClient.shutdownClient();
    }
}
