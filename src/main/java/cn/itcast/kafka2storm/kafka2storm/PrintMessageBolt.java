package cn.itcast.kafka2storm.kafka2storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by maoxiangyi on 2016/4/16.
 */
public class PrintMessageBolt extends BaseBasicBolt {
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        byte[] value = (byte[]) input.getValue(0);
        String string = new String(value);
        System.out.println(string);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
