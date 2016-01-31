package lcy.strom.start;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt implements IBasicBolt  {
	
	private Map<String , Integer> counts = new HashMap<String , Integer>();

	

	public void prepare(Map stormConf, TopologyContext context) {
		
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String word = input.getString(0);
		int count = 0;
		if(counts.containsKey(word)){
			count = counts.get(word); 
		}
		count++;
		counts.put(word, count);
		collector.emit(new Values(word,count));
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word","count"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	public void cleanup() {
		
	}

}
