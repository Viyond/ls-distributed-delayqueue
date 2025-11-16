package com.wuqi.github.zk;

import org.apache.commons.lang3.StringUtils;

public interface ZkPathConstants {
    /**
     * 根节点
     */
    String WORKER_ROOT_NODE = "/delayqueue/worker";

    /**
     * topic根节点
     */
    String TOPIC_WORKER_PREFIX = WORKER_ROOT_NODE + "/%s";

    /**
     * 某个具体的worker节点
     */
    String CLIENT_WORKER_FULLPATH = TOPIC_WORKER_PREFIX + "/worker_%s";


    static String parseTopicNameFromTopicPath(String path){
        if (StringUtils.isBlank(path)){
            return null;
        }
        int index = path.indexOf(WORKER_ROOT_NODE);
        if (index == -1){
            return null;
        }
        return path.substring(index + WORKER_ROOT_NODE.length() + 1);
    }

}
