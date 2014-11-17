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
package backtype.storm.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.scheduler.resource.Globals;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologyDetails {
    String topologyId;
    Map topologyConf;
    StormTopology topology;
    Map<ExecutorDetails, String> executorToComponent;
    int numWorkers;
    //<ExecutorDetails - Task, Map<String - Type of resource, Map<String - type of that resource, Double - amount>>>
    Map<ExecutorDetails, Map<String, Map<String, Double>>> resourceList;
    private static final Logger LOG = LoggerFactory.getLogger(TopologyDetails.class);

    //Constructor function to support mock up unit tests
    //Not actually going to be used in actual execution
    public TopologyDetails(String topologyId, Map topologyConf, StormTopology topology, int numWorkers, Map<ExecutorDetails, String> executorToComponents,Map<ExecutorDetails, Map<String, Map<String, Double>>> resourceList) {
      
        this.topologyId = topologyId;
        this.topologyConf = topologyConf;
        this.topology = topology;
        this.numWorkers = numWorkers;

        this.executorToComponent = new HashMap<ExecutorDetails, String>(0);
        if (executorToComponents != null) {
            this.executorToComponent.putAll(executorToComponents);
        }
        this.resourceList = new HashMap <ExecutorDetails, Map<String, Map<String, Double>>> ();
        if (resourceList != null) {
            for (Map.Entry<ExecutorDetails, Map<String, Map<String, Double>>> entry : 
                  resourceList.entrySet()) {
                this.resourceList.put(entry.getKey(), new HashMap<String, Map<String, Double>>());
                for (Map.Entry<String, Map<String, Double>> element : entry.getValue().entrySet()) {
                    this.resourceList.get(entry.getKey())
                          .put(element.getKey(), new HashMap<String, Double>());
                    for (Map.Entry<String, Double> line : element.getValue().entrySet()) {
                        this.resourceList.get(entry.getKey())
                              .get(element.getKey()).put(line.getKey(), line.getValue());
                    }
                }
            }
            for (Map.Entry<ExecutorDetails, Map<String, Map<String, Double>>> entry : 
                  this.resourceList.entrySet()) { 
                this.checkIntialization(entry.getValue(), entry.getKey(), "");
            }
        } else {
            if (executorToComponent.size()>0) {
                for (Map.Entry<ExecutorDetails, String> entry : executorToComponent.entrySet()) {
                    this.resourceList.put(entry.getKey(), new HashMap<String, Map<String, Double>>());
                    this.checkIntialization(this.resourceList.get(entry.getKey()), entry.getKey(), "");
                }
            } else {
              this.resourceList = null;
            } 
        }
        LOG.info("TopoID: {} ResourceList: {}", topologyId, this.resourceList);
    }

    public TopologyDetails (String topologyId, Map topologyConf, StormTopology topology, int numWorkers) {
      this.topologyId = topologyId;
      this.topologyConf = topologyConf;
      this.topology = topology;
      this.numWorkers = numWorkers;
    }

    public TopologyDetails (String topologyId, Map topologyConf, StormTopology topology, int numWorkers, Map<ExecutorDetails, String> executorToComponents) {
        this(topologyId, topologyConf, topology, numWorkers);
        this.executorToComponent = new HashMap<ExecutorDetails, String>(0);
        if (executorToComponents != null) {
            this.executorToComponent.putAll(executorToComponents);
        }
    }

    public String getId() {
        return topologyId;
    }
    
    public String getName() {
        return (String)this.topologyConf.get(Config.TOPOLOGY_NAME);
    }
    
    public Map getConf() {
        return topologyConf;
    }
    
    public int getNumWorkers() {
        return numWorkers;
    }
    
    public StormTopology getTopology() {
        return topology;
    }

    public Map<ExecutorDetails, String> getExecutorToComponent() {
        return this.executorToComponent;
    }

    public Map<ExecutorDetails, String> selectExecutorToComponent(Collection<ExecutorDetails> executors) {
        Map<ExecutorDetails, String> ret = new HashMap<ExecutorDetails, String>(executors.size());
        for (ExecutorDetails executor : executors) {
            String compId = this.executorToComponent.get(executor);
            if (compId != null) {
                ret.put(executor, compId);
            }
        }
        
        return ret;
    }
    
    public Collection<ExecutorDetails> getExecutors() {
        return this.executorToComponent.keySet();
    }

    public Map<ExecutorDetails, Map<String, Map<String, Double>>> getResourceList() {
        if (this.resourceList != null) {
            return this.resourceList;
        }
        if (this.getTopology() == null) {
            return null;
        }
        if (this.getTopology().get_bolts() == null) {
            return null;
        }
        this.resourceList = new HashMap<ExecutorDetails, Map<String, Map<String, Double>>>();

        // Extract bolt memory info
        for (Map.Entry<String, Bolt> bolt : this.getTopology().get_bolts()
                .entrySet()) {
            for (Map.Entry<ExecutorDetails, String> element : this
                    .getExecutorToComponent().entrySet()) {
                if (bolt.getKey().compareTo(element.getValue()) == 0) {
                    Map<String, Map<String, Double>> topology_resources = this.parseResources(bolt
                            .getValue().get_common().get_json_conf());
                    this.checkIntialization(topology_resources, element.getKey(), bolt.getValue().toString());
                    this.resourceList.put(element.getKey(), topology_resources);
                }
            }
        }

        // Extract spout memory info
        for (Map.Entry<String, SpoutSpec> spout : this.getTopology().get_spouts()
                .entrySet()) {
            for (Map.Entry<ExecutorDetails, String> element : this
                    .getExecutorToComponent().entrySet()) {
                if (spout.getKey().compareTo(element.getValue()) == 0) {
                    Map<String, Map<String, Double>> topology_resources = this.parseResources(spout
                            .getValue().get_common().get_json_conf());
                    this.checkIntialization(topology_resources, element.getKey(), spout.getValue().toString());
                    this.resourceList.put(element.getKey(), topology_resources);
                }
            }
        }
        return this.resourceList;
    }

    private Map<String, Map<String, Double>> parseResources(String input) {
        Map<String, Map<String, Double>> topology_resources= 
            new HashMap<String, Map<String, Double>>();
          JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(input);
            JSONObject jsonObject = (JSONObject) obj;
            Map<String, Double> topoMem = 
                (Map)jsonObject.get(Config.TOPOLOGY_RESOURCES_MEMORY_MB); 
            Double topoCPU = 
                (Double)jsonObject.get(Config.TOPOLOGY_RESOURCES_CPU);
            if (topoMem != null) {
                topology_resources.put(Globals.TYPE_MEMORY, new HashMap<String, Double>());
                topology_resources.get(Globals.TYPE_MEMORY).putAll(topoMem);
            } 
            if (topoCPU != null) {
              topology_resources.put(Globals.TYPE_CPU, new HashMap<String, Double>());
              topology_resources.get(Globals.TYPE_CPU).put(Globals.TYPE_CPU_TOTAL, topoCPU);
            }
        } catch (ParseException e) {
            LOG.error(e.toString());
            return null;
        }
        return topology_resources;
    }

    private void checkIntialization(Map<String, Map<String, Double>> topology_resources, 
          ExecutorDetails exec, String Com){
        this.checkInitMem(topology_resources, exec, Com);
        this.checkInitCPU(topology_resources, exec, Com);
    }

    private void checkInitMem(Map<String, Map<String, Double>> topology_resources, 
          ExecutorDetails exec, String Com) {
      
        String msg = "";
        if (topology_resources.size() <= 0 || topology_resources.containsKey(Globals.TYPE_MEMORY)==false) {
            topology_resources.put(Globals.TYPE_MEMORY, new HashMap<String, Double>());
        }
        if (topology_resources.get(Globals.TYPE_MEMORY).containsKey(Globals.TYPE_MEMORY_ONHEAP)==false) {
            topology_resources.get(Globals.TYPE_MEMORY)
                .put(Globals.TYPE_MEMORY_ONHEAP, Globals.DEFAULT_ONHEAP_MEMORY_REQUIREMENT);
            msg+= 
                "Resource : "+Globals.TYPE_MEMORY + 
                " Type: "+Globals.TYPE_MEMORY_ONHEAP + 
                " set to default "+ Globals.DEFAULT_ONHEAP_MEMORY_REQUIREMENT.toString() + 
                "\n";
        }
        if (topology_resources.get(Globals.TYPE_MEMORY).containsKey(Globals.TYPE_MEMORY_OFFHEAP)==false) {
            topology_resources.get(Globals.TYPE_MEMORY)
                .put(Globals.TYPE_MEMORY_OFFHEAP, Globals.DEFAULT_OFFHEAP_MEMORY_REQUIREMENT);
            msg+= 
                "Resource : "+Globals.TYPE_MEMORY + 
                " Type: "+Globals.TYPE_MEMORY_OFFHEAP + 
                " set to default "+ Globals.DEFAULT_OFFHEAP_MEMORY_REQUIREMENT.toString() + 
                "\n";
        }
        if (msg!="") {
            LOG.debug(
                  "Unable to extract resource requirement of Executor " +
                   exec + " for Component " + Com + "\n" + msg);
        }
    }

    private void checkInitCPU(Map<String, Map<String, Double>> topology_resources, 
          ExecutorDetails exec, String Com) {
 
        String msg = "";
        if (topology_resources.size() <= 0 || topology_resources.containsKey(Globals.TYPE_CPU)==false) {
            topology_resources.put(Globals.TYPE_CPU, new HashMap<String, Double>());
        }
        if (topology_resources.get(Globals.TYPE_CPU).containsKey(Globals.TYPE_CPU_TOTAL)==false) {
            topology_resources.get(Globals.TYPE_CPU)
                .put(Globals.TYPE_CPU_TOTAL, Globals.DEFAULT_CPU_REQUIREMENT);
            msg+= 
                "Resource : "+Globals.TYPE_CPU + 
                " Type: "+Globals.TYPE_CPU_TOTAL + 
                " set to default "+ Globals.DEFAULT_CPU_REQUIREMENT.toString() + 
                "\n";
        }
        if (msg!="") {
            LOG.debug(
                  "Unable to extract resource requirement of Executor " +
                   exec + " for Component " + Com + "\n" + msg);
        }
    }
}
