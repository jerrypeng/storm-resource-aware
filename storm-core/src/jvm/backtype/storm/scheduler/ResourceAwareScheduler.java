package backtype.storm.scheduler;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

		Map<String, Node> nodeIdToNode = Node.getAllNodesFrom(cluster,
				globalResources);
		LOG.info("GlobalResources: \n{}\n", globalResources);
		HelperFuncs.printNodeResources(nodeIdToNode);
	}

}
