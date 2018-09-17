package com.guanhang.KafkaApi;

import kafka.admin.AdminClient;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.coordinator.GroupOverview;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.JaasUtils;
import scala.collection.JavaConversions;
import scala.collection.immutable.List;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class ZkUtilServerTest {

    public static void main(String[] args) {
        //创建topic
        //创建与ZK的连接
        ZkUtils zkUtils = ZkUtils.apply("localhost:2181", 3000, 30000, JaasUtils.isZkSecurityEnabled());
        //创建一个单分区、单副本、名为t1的topic，未指定topic级别的参数，所以传的空的properties
        //RackAwareMode.Enforced$.MODULE$等同于指定了RackAwareMode.Enforced，表示考虑机架位置
        AdminUtils.createTopic(zkUtils, "t1", 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);


        //删除topic
        AdminUtils.deleteTopic(zkUtils, "t1");

        //查询topic级别的属性
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "t1");
        Iterator<Map.Entry<Object, Object>> iterator = props.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Object, Object> next = iterator.next();
            Object key = next.getKey();
            Object value = next.getValue();
            System.out.println(key + "=" + value);
        }

        //变更topic级别的参数
        props.setProperty("min.cleanable.dirty.ratio", "0.3");

        AdminUtils.changeTopicConfig(zkUtils, "test", props);
        zkUtils.close();

        //查询当前集群下所有consumer group的信息
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        AdminClient adminClient = AdminClient.create(properties);
        Map<Node, List<GroupOverview>> nodeListMap = JavaConversions.mapAsJavaMap(adminClient.listAllGroups());
        for (Map.Entry<Node, List<GroupOverview>> entry : nodeListMap.entrySet()) {
            Iterator<GroupOverview> groupOverviewIterator = JavaConversions.asJavaIterator(entry.getValue().iterator());
            while (groupOverviewIterator.hasNext()) {
                GroupOverview next = groupOverviewIterator.next();
                System.out.println(next.groupId());
            }
        }

        //查看指定group的位移消息
        Properties props1 = new Properties();
        props.put("bootstrap.servers", "localhostA:9092");
        AdminClient client = AdminClient.create(props1);
        String groupId = "a1";
        Map<TopicPartition, Object> topicPartitionObjectMap = JavaConversions.mapAsJavaMap(adminClient.listGroupOffsets(groupId));
        Long offset = (Long) topicPartitionObjectMap.get(new TopicPartition("test", 0));
        System.out.println(offset);
        client.close();
    }
}
