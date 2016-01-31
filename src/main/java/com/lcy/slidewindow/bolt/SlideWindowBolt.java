package com.lcy.slidewindow.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.lcy.slidewindow.bean.IpBean;

public class SlideWindowBolt implements IRichBolt{
	
	private int windowSecond;//窗口大小
	
	OutputCollector _collector;
	
	private Map<String,IpBean> map = new ConcurrentHashMap<String, IpBean>();

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}
	
	public SlideWindowBolt(int windowSecond){
		this.windowSecond = windowSecond;
	}

	public void execute(Tuple input) {
		
		if (isTickTuple(input)) {
			for (Map.Entry<String,IpBean> entry : map.entrySet()) {
				System.out.println("slide execute emit ip :" +entry.getKey());
				_collector.emit(new Values(entry.getKey(),entry.getValue()));
			}
			map.clear();
		} else {
			String ip = input.getString(0);
			System.out.println("ip"+ ip);
			IpBean ipBean = map.get(ip);
			if(ipBean == null){
				IpBean Bean = new IpBean();
				Bean.setIp(ip);
				Bean.setNum(1);
				map.put(ip, Bean);	
			}else{
				ipBean.setNum(ipBean.getNum() + 1);
				map.put(ip,ipBean);	
			}
		}
		_collector.ack(input);
		
	}
	
	
	public static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) // SYSTEM_COMPONENT_ID == "__system" 
				&& tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID); // SYSTEM_TICK_STREAM_ID == "__tick";
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ip","window"));
	}

	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, windowSecond); //配置定时发送
		return conf;
	}

}
