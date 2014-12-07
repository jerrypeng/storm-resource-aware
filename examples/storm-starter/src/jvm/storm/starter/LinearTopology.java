package storm.starter;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.base.BaseRichSpout;


/**
* This is a basic example of a Storm topology.
*/
public class LinearTopology {

  //cpu workload 
  public static boolean isPrime(int n) 
  {
    boolean ret=true;
    for(int i=2;i<n;i++) {
        if(n%i==0)
            ret=false;
    }
    return ret;
  }
  public static int PrimeSearch(int time_loop,int index)
  {
    int max=2;
    //find the largest prime number within [1,input]
    // percentage index up to 70% percentage = 10*index
    int[] cpu_para = new int[] {65,65,65,120,190,260,440,660};
    for(int j=0; j<time_loop ; j++)
    {
      for(int i =300; i<2000; i++)
      {
          if(i%cpu_para[index]==0)
          {
            try {
                Thread.sleep(5);
            } 
            catch (InterruptedException ie) {
                   //Handle exception
            }
          }

          if(isPrime(i))
              max=i;
      }
    }
    return max;
  }
  ///bolt 5
  public static class LinearBolt5 extends BaseRichBolt {
    OutputCollector _collector;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }
    @Override
    public void execute(Tuple tuple) {
        PrimeSearch(1,2);
        _collector.emit(tuple, new Values(tuple.getString(0) + "!"));
        //_collector.ack(tuple);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
  }
  ///bolt 6
  public static class LinearBolt6 extends BaseRichBolt {
    OutputCollector _collector;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }
    @Override
    public void execute(Tuple tuple) {
        PrimeSearch(1,2);
        _collector.emit(tuple, new Values(tuple.getString(0) + "!"));
        //_collector.ack(tuple);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
  }
  ///bolt 7
  public static class LinearBolt7 extends BaseRichBolt {
    OutputCollector _collector;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }
    @Override
    public void execute(Tuple tuple) {
        PrimeSearch(1,2);
        _collector.emit(tuple, new Values(tuple.getString(0) + "!"));
        //_collector.ack(tuple);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
  }

  ///spout
  public static class LinearSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
    }
    @Override
    public void nextTuple() {
        PrimeSearch(1,4);
        _collector.emit(new Values("Jerry"));
    }
    @Override
    public void ack(Object id) {
    }
    @Override
    public void fail(Object id) {
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
  }
  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    SpoutDeclarer spout = builder.setSpout("word", new LinearSpout(), 8);
    BoltDeclarer bolt_1 = builder.setBolt("exclaim1", new LinearBolt5(), 4);
    BoltDeclarer bolt_2 = builder.setBolt("exclaim2", new LinearBolt6(), 4);
    BoltDeclarer bolt_3 = builder.setBolt("exclaim_output_3", new LinearBolt7(), 4);
    
    spout.setCPULoad(40.0);
    
    bolt_1.shuffleGrouping("word");
    bolt_1.setCPULoad(20.0);
    
    bolt_2.shuffleGrouping("exclaim1");
    bolt_2.setCPULoad(20.0);
    
    bolt_3.shuffleGrouping("exclaim2");
    bolt_3.setCPULoad(20.0);
    Config conf = new Config();
    conf.setNumAckers(0);

    conf.setDebug(true);
    if (args != null && args.length > 0) {
        conf.setNumWorkers(6);
        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
  }
}
