package com.wuqi.github.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

public class IpUtils {

    private static String LOCAL_IP = null;

    public static String getLocalIp() throws Exception{
        if (LOCAL_IP != null){
            return LOCAL_IP;
        }
        List<String> ips = new ArrayList<>(); // 存储IP地址的列表
        Enumeration<NetworkInterface> networks = NetworkInterface.getNetworkInterfaces();
        for (NetworkInterface network : Collections.list(networks)) {
            if (network.isLoopback() || !network.isUp()) {
                continue; // 忽略loopback接口或未激活的接口
            }
            Enumeration<InetAddress> addresses = network.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();
                if (!address.isLoopbackAddress() && !address.isLinkLocalAddress()) { // 排除loopback和link-local地址
                    ips.add(address.getHostAddress()); // 添加到列表中
                }
            }
        }

        LOCAL_IP = ips.get(0);
        return LOCAL_IP;
    }

    public static void main(String[] args) {
        try{
            System.out.println("local:" + getLocalIp());
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
