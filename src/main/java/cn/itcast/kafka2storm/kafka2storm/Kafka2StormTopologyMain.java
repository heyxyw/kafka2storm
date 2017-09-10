package cn.itcast.kafka2storm.kafka2storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 * Created by maoxiangyi on 2016/4/16.
 */
public class Kafka2StormTopologyMain {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafkaSpout", new KafkaSpout(new SpoutConfig(new ZkHosts("zk01:2181,zk02:2181,zk03:2181"), "itheima", "/kafka2storm", "kafkaSpout")), 3);
        topologyBuilder.setBolt("printMessageBolt",new PrintMessageBolt(),1).shuffleGrouping("kafkaSpout");
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);
        System.out.println("start............");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Kafka2StormTopologyMain", conf, topologyBuilder.createTopology());

    }
}
