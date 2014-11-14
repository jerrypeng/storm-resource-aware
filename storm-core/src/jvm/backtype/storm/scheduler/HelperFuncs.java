package backtype.storm.scheduler;
import java.util.Map;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.Iterator;

import backtype.storm.Config;
import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.SchedulerAssignment;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.WorkerSlot;

import java.util.Collection;
import java.util.ArrayList;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.SupervisorDetails;

public class HelperFuncs {
  private static final Logger LOG = LoggerFactory.getLogger(HelperFuncs.class);

  public static void printMap(Map mp) {
    if (mp == null) {
      LOG.info("Map is empty!");
      return;
    }
    if (mp.isEmpty() == true) {
      return;
    }
    Iterator it = mp.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry pairs = (Map.Entry) it.next();
      LOG.info(pairs.getKey() + " = " + pairs.getValue());
    }
  }

  public static void printResources(Map<String, SupervisorDetails> supervisors) {
    for (Map.Entry<String, SupervisorDetails> entry : supervisors.entrySet()) {
      LOG.info("SUPERVISOR: " + entry.getKey() + " TOTAL MEMORY: "
          + entry.getValue().getTotalMemory());
    }
  }

  public static void printNodeResources(Map<String, Node> nodeIdToNode) {
    for (Map.Entry<String, Node> entry : nodeIdToNode.entrySet()) {
      if(entry.getValue().sup!=null) {
        LOG.info("Node: " + entry.getValue().sup.getHost() + " TOTAL MEMORY: "
            + entry.getValue().getTotalMemoryResources() + " AVAILABLE MEMORY: "
            + entry.getValue().getAvailableMemoryResources()
            +" TOTAL CPU: "+entry.getValue().getTotalCpuResources()
            +" AVAILABLE CPU: "+entry.getValue().getAvailableCpuResources()); 
      }
    }
  }

  public static Collection<ExecutorDetails> getExecutors(WorkerSlot ws,
      Cluster cluster) {
    Collection<ExecutorDetails> retList = new ArrayList<ExecutorDetails>();
    for (Entry<String, SchedulerAssignment> entry : cluster.getAssignments()
        .entrySet()) {
      Map<ExecutorDetails, WorkerSlot> executorToSlot = entry.getValue()
          .getExecutorToSlot();
      for (Map.Entry<ExecutorDetails, WorkerSlot> execToSlot : executorToSlot
          .entrySet()) {
        WorkerSlot slot = execToSlot.getValue();
        if (ws.getPort() == slot.getPort()
            && ws.getNodeId() == slot.getNodeId()) {
          ExecutorDetails exec = execToSlot.getKey();
          retList.add(exec);
        }
      }
    }
    return retList;
  }
}
