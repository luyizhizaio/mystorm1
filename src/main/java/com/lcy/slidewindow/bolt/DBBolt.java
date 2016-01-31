package com.lcy.slidewindow.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.lcy.slidewindow.bean.IpBean;
import com.lcy.slidewindow.utils.AccessIpNumTable;
import com.lcy.slidewindow.utils.HBaseUtils;
/**
 * 数据保存到hbase
 * rowkey ip
 * famliy detail
 * column num 
 * @author dayue
 *
 */
public class DBBolt implements IRichBolt{
	
	private AccessIpNumTable accessIpNumTable;
	private String tableName; 
	OutputCollector _collector;
	private HBaseUtils HUtils;
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

		HUtils = new HBaseUtils("yarn1,yarn2,yarn3","/hbase");
		accessIpNumTable = new AccessIpNumTable(HUtils,tableName);
		_collector = collector;
	}
	
	public DBBolt(String tableName){
		this.tableName = tableName;
	}
	

	public void execute(Tuple input) {
		System.out.println("dbbolt start");
		Object value = input.getValue(1);
		IpBean bean = (IpBean)value;
		System.out.println("dbbolt ipbean" + bean.toString());
		accessIpNumTable.insert(HUtils, bean);
		System.out.println("db insert success");
		_collector.emit(new Values(input));
	
		_collector.ack(input);
		
	}

	public void cleanup() {
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("value"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
