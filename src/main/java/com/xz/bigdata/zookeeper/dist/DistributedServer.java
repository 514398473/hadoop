package com.xz.bigdata.zookeeper.dist;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 分布式服务端
 * 
 * @author Administrator
 *
 */
public class DistributedServer {

	private static final Logger LOGGER = LoggerFactory.getLogger(DistributedServer.class);

	private ZooKeeper zooKeeper;

	private static final String CONNECTSTRING = "study1:2181,study2:2181,study3:2181";

	private static final String GROUPNAME = "/server";

	private static final int SESSIONTIMEOUT = 2000;

	public static void main(String[] args) {
		DistributedServer distributedServer = new DistributedServer();
		distributedServer.getConnect();
		distributedServer.registerServer(args[0]);
		distributedServer.handleBusiness(args[0]);
	}

	public void handleBusiness(String hostName) {
		LOGGER.info(hostName + "start working");
		try {
			Thread.sleep(Long.MAX_VALUE);
		} catch (InterruptedException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	public void registerServer(String hostName) {
		try {
			String create = zooKeeper.create(GROUPNAME + "/server", hostName.getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.EPHEMERAL_SEQUENTIAL);
			LOGGER.info(hostName + "is online.." + create);
		} catch (KeeperException | InterruptedException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	public void getConnect() {

		try {
			zooKeeper = new ZooKeeper(CONNECTSTRING, SESSIONTIMEOUT, new Watcher() {
				@Override
				public void process(WatchedEvent event) {
					LOGGER.info(event.getType() + "-----" + event.getPath());
					try {
						zooKeeper.getChildren("/", true);
					} catch (KeeperException | InterruptedException e) {
						LOGGER.error(e.getMessage(), e);
					}
				}
			});
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}
}
