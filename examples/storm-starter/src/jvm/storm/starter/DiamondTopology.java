package storm.starter;

import storm.starter.bolt.TestBolt;
import storm.starter.spout.TestSpout;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class DiamondTopology {
	public static void main(String[] args) throws Exception {
		int paralellism = 4;

        TopologyBuilder builder = new TopologyBuilder();

        SpoutDeclarer spout = builder.setSpout("spout_head", new TestSpout(), paralellism);
        spout.setCPULoad(50.0);

       BoltDeclarer bolt1 = builder.setBolt("bolt_1", new TestBolt(), paralellism);
       BoltDeclarer bolt2 = builder.setBolt("bolt_2", new TestBolt(), paralellism);
       BoltDeclarer bolt3 = builder.setBolt("bolt_3", new TestBolt(), paralellism);
       BoltDeclarer bolt4 = builder.setBolt("bolt_4", new TestBolt(), paralellism);
       
       bolt1.shuffleGrouping("spout_head");
       bolt1.setCPULoad(15.0);
       
       bolt2.shuffleGrouping("spout_head");
       bolt2.setCPULoad(15.0);
       
       bolt3.shuffleGrouping("spout_head");
       bolt3.setCPULoad(15.0);
       
       bolt4.shuffleGrouping("spout_head");
       bolt4.setCPULoad(15.0);

        BoltDeclarer output = builder.setBolt("bolt_output_3", new TestBolt(), paralellism).setNumTasks(6);
        output.shuffleGrouping("bolt_1");
        output.shuffleGrouping("bolt_2");
        output.shuffleGrouping("bolt_3");
        output.shuffleGrouping("bolt_4");
        output.setCPULoad(15.0);

        Config conf = new Config();
        conf.setDebug(true);

         conf.setNumAckers(0);

        conf.setNumWorkers(12);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
                        builder.createTopology());

	}

}