package lcy.strom.start;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBolt implements IBasicBolt {

	
	public void prepare(Map stormConf, TopologyContext context) {
		
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String sentence = input.getString(0);
		for(String word : sentence.split(" ")){
			collector.emit(new Values(word));
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));	
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void cleanup() {
		
	}

}
