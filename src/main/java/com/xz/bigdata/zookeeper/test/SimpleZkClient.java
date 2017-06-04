package com.xz.bigdata.zookeeper.test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 测试zk客户端的增删改查
 * 
 * @author Administrator
 *
 */
public class SimpleZkClient {

	private static final Logger LOGGER = LoggerFactory.getLogger(SimpleZkClient.class);

	private ZooKeeper zooKeeper;

	private static final String CONNECTSTRING = "study1:2181,study2:2181,study3:2181";

	private static final int SESSIONTIMEOUT = 2000;

	@Before
	public void init() {
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

	@Test
	public void createZnode() {
		try {
			String create = zooKeeper.create("/server", "hello".getBytes(), Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
			LOGGER.info("新建了节点:" + create);
		} catch (KeeperException | InterruptedException e) {
			LOGGER.error("创建节点失败!", e);
		}
	}

	@Test
	public void getChildrens() {
		try {
			List<String> childrens = zooKeeper.getChildren("/", true);
			for (String children : childrens) {
				LOGGER.info(children);
			}
		} catch (KeeperException | InterruptedException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	@Test
	public void testExist() {
		try {
			Stat stat = zooKeeper.exists("/eclipse", false);
			if (null != stat) {
				LOGGER.info("节点存在!" + stat.toString());
			} else {
				LOGGER.info("节点不存在!");
			}
		} catch (KeeperException | InterruptedException e) {
			LOGGER.error(e.getMessage(), e);
		}

	}

	@Test
	public void getData() {
		try {
			byte[] data = zooKeeper.getData("/eclipse", false, new Stat());
			LOGGER.info(new String(data, "utf-8"));
		} catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	@Test
	public void deleteZnode() {
		try {
			zooKeeper.delete("/eclipse", -1);
		} catch (InterruptedException | KeeperException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	@Test
	public void setData() {
		try {
			Stat data = zooKeeper.setData("/eclipse", "niubi".getBytes(), -1);
			LOGGER.info("修改数据成功:" + data.toString());
		} catch (KeeperException | InterruptedException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

}
