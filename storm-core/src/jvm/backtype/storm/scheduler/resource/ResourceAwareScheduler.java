package backtype.storm.scheduler.resource;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.Topologies;

public class ResourceAwareScheduler implements IScheduler {
	private static final Logger LOG = LoggerFactory
			.getLogger(ResourceAwareScheduler.class);
	@SuppressWarnings("rawtypes")
	private Map _conf;

	@Override
	public void prepare(Map conf) {
		_conf = conf;
	}

	@Override
	public void schedule(Topologies topologies, Cluster cluster) {
		LOG.info("\n\n\nRerunning ResourceAwareScheduler...");

		GlobalResources globalResources = new GlobalResources(topologies);
		GlobalState globalState = GlobalState.getInstance("ResourceAwareScheduer");
		globalState.updateInfo(cluster, topologies, globalResources);

		Map<String, Node> nodeIdToNode = Node.getAllNodesFrom(cluster,
				globalResources);
		LOG.info("GlobalResources: \n{}\n", globalResources);
		HelperFuncs.printNodeResources(nodeIdToNode);
	}

}
