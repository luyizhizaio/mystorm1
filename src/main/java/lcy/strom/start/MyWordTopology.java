package lcy.strom.start;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class MyWordTopology {
	
	 public static void main(String args[]) throws Exception {
	        
	        TopologyBuilder builder = new TopologyBuilder();
	        
	        builder.setSpout("WordSpout", new WordSpout());
	        
	        
	        builder.setBolt("SplitSentenceBolt", new SplitSentenceBolt()).shuffleGrouping("WordSpout");
	        builder.setBolt("WordCountBolt", new WordCountBolt()).shuffleGrouping("SplitSentenceBolt");
	        //config,就是一个map
	        Config conf = new Config();
	        conf.setDebug(true); 
	        conf.setNumWorkers(3);
	        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
	        /* 
	         * 定义一个 LocalCluster对象来定义一个进程内的集群。提交topology 给这个虚拟的集群和提交 topology 给分布式集群是一样的。 
	         * 通过调用 submitTopology 方法来提交topology， 它接受三个参数：要运行的topology 的名字，一个配置对象以及要运行的topology 本身。 
	         * topology 的名字是用来唯一区别一个 topology的，这样你然后可以用这个名字来杀死这个 topology 的。前面已经说过了， 你必须显式的杀掉一个topology， 否则它会一直运行。 
	         */ 
	        if(args == null){
        	    LocalCluster cluster = new LocalCluster(); 
		        cluster.submitTopology("wordCounterTopology", conf, builder.createTopology()); 
		        Thread.sleep(1000); 
		        cluster.killTopology("wordCounterTopology"); //杀死 topolgy
		        cluster.shutdown(); 
	        }else{
	        	 
		        //提交到集群的方法
		        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());	
	        }
	     
	    }

}
