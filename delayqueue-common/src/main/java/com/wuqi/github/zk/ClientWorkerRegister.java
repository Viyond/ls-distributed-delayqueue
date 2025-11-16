package com.wuqi.github.zk;

import com.wuqi.github.utils.IpUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.wuqi.github.zk.ZkPathConstants.*;

public class ClientWorkerRegister {
    private static final Logger LOG = LoggerFactory.getLogger(ClientWorkerRegister.class);

    private final ZkConnector zkConnectorHolder;

    private ConcurrentMap<String, List<String>> globalTopicWorkerMap = new ConcurrentHashMap<>();

    public ClientWorkerRegister(String zkHosts) throws Exception{
        zkConnectorHolder = new ZkConnector(zkHosts);
    }

    public void registerWorkerIndex(String topic, int workerIndex) throws Exception{
        String workerAddress = String.format("%s:$s", IpUtils.getLocalIp(), workerIndex);
        String topicPath = String.format(TOPIC_WORKER_PREFIX, topic);
        String workerPath = String.format(CLIENT_WORKER_FULLPATH, topic, workerAddress);
        ClientWorker worker = new ClientWorker(topic, workerPath, zkConnectorHolder);
        worker.zkRegister();

        zkConnectorHolder.getZooKeeper().getChildren(topicPath, true, childrenCallback, null);
    }

    public List<String> getTopicClientWorkers(String topic){
        List<String> res = globalTopicWorkerMap.get(topic);
        return res != null? res : Collections.emptyList();
    }

    private void refreshTopicClientWorkers(String topicPath){
        zkConnectorHolder.getZooKeeper().getChildren(topicPath, true, childrenCallback, null);
    }

    AsyncCallback.ChildrenCallback childrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    refreshTopicClientWorkers(path);
                    break;
                case OK:
                    String topic = parseTopicNameFromTopicPath(path);
                    globalTopicWorkerMap.put(topic, children);
                    break;
                default:
                    LOG.error("Something wrong happened: "+KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

}
