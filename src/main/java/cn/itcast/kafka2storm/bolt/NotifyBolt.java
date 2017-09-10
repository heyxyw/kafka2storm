package cn.itcast.kafka2storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cn.itcast.kafka2storm.domain.Message;
import cn.itcast.kafka2storm.domain.Record;
import cn.itcast.kafka2storm.utils.MonitorHandler;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.log4j.Logger;

/**
 * Describe: 将触发信息保存到mysql数据库中
 * Author:   maoxiangyi
 * Domain:   www.itcast.cn
 * Data:     2015/11/11.
 */
//BaseRichBolt 需要手动调ack方法，BaseBasicBolt由storm框架自动调ack方法
public class NotifyBolt extends BaseBasicBolt {
    private static Logger logger = Logger.getLogger(NotifyBolt.class);

    public void execute(Tuple input, BasicOutputCollector collector) {
        //说明：PrepareRecordBolt收到的消息都是已经触发规则的消息
        Message message = (Message) input.getValueByField("message");
        String appId = input.getStringByField("appId");
        //调用notify 进行短信告警和邮件告警
        MonitorHandler.notify(appId, message);
        //补全字段，准备保存到数据库中
        Record record = new Record();
        try {
            BeanUtils.copyProperties(record, message);
            collector.emit(new Values(record));
        } catch (Exception e) {

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("record"));
    }

}
