/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

import storm.starter.bolt.TestBolt;
import storm.starter.spout.TestSpout;

/**
 * This is a basic example of a Storm topology.
 */
public class ResourceAwareTestTopology {


  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    SpoutDeclarer spout = builder.setSpout("word", new TestSpout(), 10);
    BoltDeclarer bolt1 = builder.setBolt("exclaim1", new TestBolt(), 3);
    BoltDeclarer bolt2 = builder.setBolt("exclaim2", new TestBolt(), 2);
    
    spout.setCPULoad(20.0);
    spout.setMemoryLoad(1024.0);
    
    bolt1.shuffleGrouping("word");
    bolt1.setCPULoad(10.0);
    bolt1.setMemoryLoad(1024.0, 512.0);
    
    bolt2.shuffleGrouping("exclaim1");
    bolt2.setCPULoad(15.0);
    bolt2.setMemoryLoad(1024.0, 768.0);
    
    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {

    }
  }
}
