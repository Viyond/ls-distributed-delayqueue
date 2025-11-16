package com.wuqi.github.zk;


import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZkConnector implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(ZkConnector.class);

    // ZooKeeper Object
    private ZooKeeper zooKeeper;

    private final String connectHosts;

    // To block any operation until ZooKeeper is connected. It's initialized
    // with count 1, that is, ZooKeeper connect state.
    CountDownLatch connectedSignal;

    protected static final List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    public ZkConnector(String hosts) throws IOException, InterruptedException {
        connectHosts = hosts;
        connect();
    }

    private void connect() throws IOException, InterruptedException{
        zooKeeper = new ZooKeeper(
                connectHosts, // ZooKeeper service hosts
                5000,  // Session timeout in milliseconds
                this); // watches -- this class implements Watcher; events are handled by the process method
        connectedSignal = new CountDownLatch(1);
        connectedSignal.await();
    }

    /**
     * Closes connection with ZooKeeper
     */
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    /**
     * @return the zooKeeper
     */
    public ZooKeeper getZooKeeper() {
        // Verify ZooKeeper's validity
        if (null == zooKeeper || !zooKeeper.getState().equals(ZooKeeper.States.CONNECTED)) {
            throw new IllegalStateException ("ZooKeeper is not connected.");
        }
        return zooKeeper;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        // check if event is for connection done; if so, unblock main thread blocked in connect
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
        }else if (watchedEvent.getState() == Event.KeeperState.Disconnected ||
                watchedEvent.getState() == Event.KeeperState.Expired){
            tryReconnect();
        }
    }

    private void tryReconnect(){
        Thread reConnectWorker = new Thread(() -> {
            while (true){
                try{
                    Thread.sleep(3000L);
                    zooKeeper.close();
                    connect();
                    break;
                }catch (Exception e){
                    LOG.error("zk-reconnect-worker reconnect error, zkHosts:{}", connectHosts, e);
                }
            }
        });
        reConnectWorker.setName("zk-reconnect-worker");
        reConnectWorker.setDaemon(true);
        reConnectWorker.start();
    }
}
