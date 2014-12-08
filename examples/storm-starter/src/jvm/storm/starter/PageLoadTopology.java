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

public class PageLoadTopology {
	public static void main(String[] args) throws Exception {
		//int numBolt = 3;
		int paralellism = 3;

		TopologyBuilder builder = new TopologyBuilder();

		SpoutDeclarer spout = builder.setSpout("spout_head", new RandomLogSpout(), paralellism);
		spout.setCPULoad(50.0);

		BoltDeclarer bolt1 = builder.setBolt("bolt_transform", new TransformBolt(), paralellism);
		bolt1.setCPULoad(15.0);
		bolt1.shuffleGrouping("spout_head");
		
		BoltDeclarer bolt2 = builder.setBolt("bolt_filter", new FilterBolt(), paralellism);
		bolt2.setCPULoad(15.0);
		bolt2.shuffleGrouping("bolt_transform");
		
		BoltDeclarer bolt3 = builder.setBolt("bolt_join", new TestBolt(), paralellism);
		bolt3.setCPULoad(15.0);
		bolt3.shuffleGrouping("bolt_filter");
		
		BoltDeclarer bolt4 = builder.setBolt("bolt_filter_2", new FilterBolt(), paralellism);
		bolt4.setCPULoad(15.0);
		bolt4.shuffleGrouping("bolt_join");
		
		BoltDeclarer bolt5 = builder.setBolt("bolt_aggregate", new AggregationBolt(), paralellism);
		bolt5.setCPULoad(15.0);
		bolt5.shuffleGrouping("bolt_filter_2");
		
		BoltDeclarer bolt6 = builder.setBolt("bolt_output_sink", new TestBolt(),paralellism);
		bolt6.setCPULoad(15.0);
		bolt6.shuffleGrouping("bolt_aggregate");

		Config conf = new Config();
		conf.setDebug(true);

		conf.setNumAckers(0);

		conf.setNumWorkers(12);

		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}

}