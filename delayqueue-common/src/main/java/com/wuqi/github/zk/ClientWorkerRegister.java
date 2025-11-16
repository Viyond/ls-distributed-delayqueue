package com.wuqi.github.zk;

import com.wuqi.github.utils.IpUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientWorkerRegister {
    private static final Logger LOG = LoggerFactory.getLogger(ClientWorkerRegister.class);

    private static final String WORKER_ROOT_NODE = "/delayqueue/worker/worker_";
    private static final String TOPIC_CONSUMER_WORKER_PREFIX = "%s_worker_%s";

    private final String zkHosts;

    private final ZkConnector zkConnectorHolder;

    public ClientWorkerRegister(String zkHosts) throws Exception{
        this.zkHosts = zkHosts;
        zkConnectorHolder = new ZkConnector(zkHosts);
    }

    public void registerWorkerIndex(String topic, int workerIndex) throws Exception{
        String workerAddress = String.format("%s:$s", IpUtils.getLocalIp(), workerIndex);
        String uniqueWorker = String.format(TOPIC_CONSUMER_WORKER_PREFIX, topic, workerAddress);
        String workerNode = WORKER_ROOT_NODE + uniqueWorker;
        String path = zkConnectorHolder.getZooKeeper().create(workerNode,
                null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, null, null);
        //
    }

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    break;
                case OK:
                    break;
                case NODEEXISTS:
                    break;
                default:
                    LOG.error("Something went wrong: "+ KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    }
}
