package com.lcy.slidewindow.spout;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SourceSpout extends BaseRichSpout {
	private SpoutOutputCollector _collector;
	
	private String[] data ={"222.36.188.206	20130530235959	uc_server/avatar.php?uid=57279&size=middle HTTP/1.1",
			"2.36.188.206	20150530235349	uc_server/reg.php? HTTP/1.1",
			"2.36.188.201	20150530235229	uc_server/abc.php?uid=57279&size=middle HTTP/1.1",
			"10.36.18.2	20130530235959	uc_server/dddd.php?uid=57279&size=middle HTTP/1.1"}; 

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
	}

	
	public void nextTuple() {
		System.out.println("nextTuple execute.....");
		
		Random random = new Random();
		int nextInt = random.nextInt(3);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		_collector.emit(new Values(data[nextInt]));
		
		/*String uri = "hdfs://yarn1:9000/hmbbs_cleaned/2013_05_30/part-r-00000";
		InputStream in =null;
		try {
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(uri),conf);
			in = fs.open(new Path(uri));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			//222.36.188.206  20130530235959  uc_server/avatar.php?uid=57279&size=middle HTTP/1.1
			String line = null;
			while(null != (line =br.readLine())){
				_collector.emit(new Values(line));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			IOUtils.closeStream(in);
		}*/
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("access"));
	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

}
