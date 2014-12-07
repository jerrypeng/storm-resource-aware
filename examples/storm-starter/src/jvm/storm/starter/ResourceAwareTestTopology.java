package storm.starter;

import storm.starter.bolt.TestBolt;
import storm.starter.spout.TestSpout;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class ResourceAwareTestTopology {
	public static void main(String[] args) throws Exception {
		int numBolt = 3;
		int paralellism = 5;

		TopologyBuilder builder = new TopologyBuilder();

		SpoutDeclarer spout =  builder.setSpout("spout_head", new TestSpout(), paralellism);
		spout.setCPULoad(50.0);

		for (int i = 0; i < numBolt; i++) {
			if (i == 0) {
				BoltDeclarer bolt = builder.setBolt("bolt_linear_" + i, new TestBolt(), paralellism);
				bolt.setCPULoad(15.0);
				bolt.shuffleGrouping("spout_head");
			} else {
				if (i == (numBolt - 1)) {
					BoltDeclarer bolt = builder.setBolt("bolt_output_" + i, new TestBolt(),
							paralellism);
					bolt.setCPULoad(15.0);
					bolt.shuffleGrouping(
							"bolt_linear_" + (i - 1));
				} else {
					BoltDeclarer bolt = builder.setBolt("bolt_linear_" + i, new TestBolt(),
							paralellism);
					bolt.setCPULoad(15.0);
					bolt.shuffleGrouping(
							"bolt_linear_" + (i - 1));
				}
			}
		}

		Config conf = new Config();
		conf.setDebug(true);

		conf.setNumAckers(0);

		conf.setNumWorkers(12);

		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}

}