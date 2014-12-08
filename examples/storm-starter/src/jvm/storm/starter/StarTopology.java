package storm.starter;

import storm.starter.bolt.TestBolt;
import storm.starter.spout.TestSpout;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class StarTopology {
	public static void main(String[] args) throws Exception {
		int numSpout = 2;
		int numBolt = 2;
		int paralellism = 3;

		TopologyBuilder builder = new TopologyBuilder();

		BoltDeclarer center = builder.setBolt("center", new TestBolt(),
				paralellism*2);
		center.setCPULoad(15.0);

		for (int i = 0; i < numSpout; i++) {
			SpoutDeclarer spout = builder.setSpout("spout_" + i, new TestSpout(), paralellism);
			center.shuffleGrouping("spout_" + i);
			spout.setCPULoad(50.0);
		}

		for (int i = 0; i < numBolt; i++) {
			BoltDeclarer bolt = builder.setBolt("bolt_output_" + i, new TestBolt(), paralellism)
					.shuffleGrouping("center");
			bolt.setCPULoad(15.0);
		}
		
		
		Config conf = new Config();
		conf.setDebug(true);
		
		conf.setNumAckers(0);

		conf.setNumWorkers(12);
		
	

		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}
}