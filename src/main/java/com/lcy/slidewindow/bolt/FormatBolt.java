package com.lcy.slidewindow.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
/**
 * 统计30秒内相同ip 访问的次数
 * @author dayue
 *
 */
public class FormatBolt implements IRichBolt{
	OutputCollector _collector;
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
			_collector = collector;
		
	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String access = input.getString(0);
		String[] splitAccess = access.split("\t");
		if(splitAccess ==null || splitAccess.length !=3){
			_collector.ack(input);
		}
		
		_collector.emit(new Values(splitAccess[0],splitAccess[1],splitAccess[2]));
		_collector.ack(input);
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ip","date","url"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
