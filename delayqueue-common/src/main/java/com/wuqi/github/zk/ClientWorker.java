package com.wuqi.github.zk;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientWorker {
    private static final Logger LOG = LoggerFactory.getLogger(ClientWorker.class);

    private final String topic;

    private final String workerPath;

    private final ZkConnector zkConnector;

    public ClientWorker(String topic, String workerPath, ZkConnector zkConnector){
        this.topic = topic;
        this.workerPath = workerPath;
        this.zkConnector = zkConnector;
    }

    public void zkRegister(){
        zkConnector.getZooKeeper().create(workerPath,
                null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, createWorkerCallback, null);
    }

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    zkRegister();
                    break;
                case OK:
                    LOG.info("Registered successfully: " + workerPath);
                    break;
                case NODEEXISTS:
                    LOG.warn("Already registered, " + workerPath);
                    break;
                default:
                    LOG.error("Something went wrong: "+ KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
}
