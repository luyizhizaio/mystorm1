package lcy.strom.start;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private static final String[] msgs =new String[] {
		"i have a dream ",
		"you can be what you want be"
	};

	private static final Random random = new Random();
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector= collector;	
		
	}

	public void nextTuple() {
		String sentence = msgs[random.nextInt(2)];
		collector.emit(new Values(sentence));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));	
	}

}
