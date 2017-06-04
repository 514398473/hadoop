package com.xz.bigdata.zookeeper.dist;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分布式客户端
 * 
 * @author Administrator
 *
 */
public class DistributedClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(DistributedClient.class);

	private ZooKeeper zooKeeper;

	private static final String CONNECTSTRING = "study1:2181,study2:2181,study3:2181";

	private static final String GROUPNAME = "/server";

	private static final int SESSIONTIMEOUT = 2000;

	private volatile List<String> serverList;

	public static void main(String[] args) {
		DistributedClient distributedClient = new DistributedClient();
		distributedClient.getConnect();
		distributedClient.getServerList();
		distributedClient.handleBussiness();
	}

	public void handleBussiness() {
		LOGGER.info("client start working");
		try {
			Thread.sleep(Long.MAX_VALUE);
		} catch (InterruptedException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	public void getServerList() {
		try {
			List<String> childrens = zooKeeper.getChildren(GROUPNAME, true);
			List<String> servers = new ArrayList<String>();
			for (String children : childrens) {
				byte[] data = zooKeeper.getData(GROUPNAME + "/" + children, false, new Stat());
				servers.add(new String(data));
			}
			serverList = servers;
			LOGGER.info(serverList.toString());
		} catch (KeeperException | InterruptedException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	public void getConnect() {
		try {
			zooKeeper = new ZooKeeper(CONNECTSTRING, SESSIONTIMEOUT, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					getServerList();
				}
			});
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}
}
