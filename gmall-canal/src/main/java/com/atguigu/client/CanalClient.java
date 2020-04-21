package com.atguigu.client;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstant;
import com.atguigu.utils.KafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {

    public static void main(String[] args) {

        // 获取Canal连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                "");
        // 长轮询获取数据
        while (true) {
            // 连接Canal
            canalConnector.connect();
            // 订阅的数据库里面的表
            canalConnector.subscribe("gmall.*");
            // 抓取数据
            Message message = canalConnector.get(100);

            // 判断数据是否为空
            if (message.getEntries().size() <= 0) {
                System.out.println("当前没有数据，休息一下！！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                // 解析message,获取Entry集合并遍历
                for (CanalEntry.Entry entry : message.getEntries()) {

                    // 判断当前Entry的类型，排除掉事务的开启，关闭等操作，只需要和数据相关的操作
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        try {
                            // 获取该sql所影响的数据内容
                            ByteString storeValue = entry.getStoreValue();
                            // 反序列化
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                            // 取出rowChange中的行集
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                            // 取出rowChange中的事件类型
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            // 取出该sql的表名
                            String tableName = entry.getHeader().getTableName();

                            // 根据表名以及事件类型处理数据集
                            handler(tableName, eventType, rowDatasList);






                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

    }

    // 根据表名以及事件类型处理数据集（发送至kafka）
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        // 取订单表中的下单数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(GmallConstant.GMALL_ORDER_INFO, rowDatasList);
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            sendToKafka(GmallConstant.GMALL_ORDER_DETAIL, rowDatasList);

        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) ||
                CanalEntry.EventType.UPDATE.equals(eventType))) {
            sendToKafka(GmallConstant.GMALL_USER_INFO, rowDatasList);
        }
    }

    private static void sendToKafka(String topic, List<CanalEntry.RowData> rowDatasList) {
        // 解析rowDatasList
        for (CanalEntry.RowData rowData : rowDatasList) {
            JSONObject jsonObject = new JSONObject();
            // 获取插入之后的数据，并遍历
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            // 打印数据，发送至kafka

            // 模拟网络堵塞原因
//            try {
//                Thread.sleep(new Random().nextInt(4)*1000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }

            System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>"+jsonObject.toString());
            KafkaSender.send(topic, jsonObject.toString());
        }
    }
}
