 package storm.starter;

import storm.starter.bolt.AggregationBolt;
import storm.starter.bolt.FilterBolt;
import storm.starter.bolt.TestBolt;
import storm.starter.bolt.TransformBolt;
import storm.starter.spout.RandomLogSpout;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class ProcessingTopology {
	public static void main(String[] args) throws Exception {
		int paralellism = 2;

		TopologyBuilder builder = new TopologyBuilder();

		SpoutDeclarer spout = builder.setSpout("spout_head", new RandomLogSpout(), paralellism);
		spout.setCPULoad(30.0);
	
		BoltDeclarer bolt1 = builder.setBolt("bolt_transform", new TransformBolt(), paralellism+2);
		bolt1.shuffleGrouping("spout_head");
		bolt1.setCPULoad(30.0);
		
		
		BoltDeclarer bolt2 = builder.setBolt("bolt_filter", new FilterBolt(), paralellism+2);
		bolt2.shuffleGrouping("bolt_transform");
		bolt2.setCPULoad(30.0);
		
		BoltDeclarer bolt3 = builder.setBolt("bolt_join1", new TestBolt(), paralellism+2);
		bolt3.shuffleGrouping("bolt_filter");
		bolt3.setCPULoad(10.0);
		
		BoltDeclarer bolt4 =builder.setBolt("bolt_output_1", new TestBolt(),paralellism+2);
		bolt4.shuffleGrouping("bolt_join1");
		bolt4.setCPULoad(10.0);
		
		BoltDeclarer bolt5 =builder.setBolt("bolt_join2", new TransformBolt(), paralellism+2);
		bolt5.shuffleGrouping("bolt_transform");
		bolt5.setCPULoad(15.0);
		
		BoltDeclarer bolt6 =builder.setBolt("bolt_normalize", new TestBolt(),paralellism+2);
		bolt6.shuffleGrouping("bolt_join2");
		bolt6.setCPULoad(10.0);
		
		BoltDeclarer bolt7 =builder.setBolt("bolt_output_2", new TestBolt(),paralellism+2);
		bolt7.shuffleGrouping("bolt_normalize");
		bolt7.setCPULoad(10.0);

		Config conf = new Config();
		conf.setDebug(true);

		conf.setNumAckers(0);

		conf.setNumWorkers(12);

		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}

}