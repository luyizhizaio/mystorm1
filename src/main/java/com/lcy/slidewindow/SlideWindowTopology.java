package com.lcy.slidewindow;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.lcy.slidewindow.bolt.DBBolt;
import com.lcy.slidewindow.bolt.FormatBolt;
import com.lcy.slidewindow.bolt.SlideWindowBolt;
import com.lcy.slidewindow.spout.SourceSpout;


public class SlideWindowTopology {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		// TODO Auto-generated method stub
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("sourceSpout", new SourceSpout());
		builder.setBolt("formatBolt", new FormatBolt()).shuffleGrouping("sourceSpout");
		builder.setBolt("slideWindowBolt", new SlideWindowBolt(30)).fieldsGrouping("formatBolt", new Fields("ip"));
		builder.setBolt("dbBolt", new DBBolt("access_ip_num")).fieldsGrouping("slideWindowBolt", new Fields("ip"));

		Config config = new Config();
		//config.setNumWorkers(4);
		config.setMaxSpoutPending(1000);
		
		StormSubmitter.submitTopology(args[0], config, builder.createTopology());
		
	}

}
