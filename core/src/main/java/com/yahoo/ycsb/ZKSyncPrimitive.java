package com.yahoo.ycsb;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZKSyncPrimitive implements Watcher {

	static ZooKeeper zk = null;
	static Integer mutex;
	String ZKAddress;

	public ZKSyncPrimitive(String address) {
		this.ZKAddress = address;
		if (zk == null) {
			try {
				System.out.println("Starting ZK:");
				zk = new ZooKeeper(address, 3000, this);
				mutex = new Integer(-1);
				System.out.println("Finished starting ZK: " + zk);
			} catch (IOException e) {
				System.out.println(e.toString());
				zk = null;
			}
		}
	}

	synchronized public void process(WatchedEvent event) {
		synchronized (mutex) {
			mutex.notify();
		}
	}

	class ZKBarrier extends ZKSyncPrimitive {
		private int groupSize;
		private String barrierRoot;
		private String myNodeName;
		private String actualPath;

		ZKBarrier(String address, String root, int size) {
			super(address);
			this.barrierRoot = root;
			this.groupSize = size;

			// Create barrier node
			if (zk != null) {
				try {
					Stat s = zk.exists(root, false);
					if (s == null) {
						zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
								CreateMode.PERSISTENT);
					}
				} catch (KeeperException e) {
					System.out
							.println("Keeper exception when instantiating queue: "
									+ e.toString());
				} catch (InterruptedException e) {
					System.out.println("Interrupted exception");
				}
			}

			// My node name
			try {
				this.myNodeName = new String(InetAddress.getLocalHost()
						.getCanonicalHostName().toString());
			} catch (UnknownHostException e) {
				System.out.println(e.toString());
			}
		}

		boolean enter() throws KeeperException, InterruptedException {
			this.actualPath = zk.create(barrierRoot + "/" + myNodeName, new byte[0],
					Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
			while (true) {
				synchronized (mutex) {
					List<String> list = zk.getChildren(barrierRoot, true);

					if (list.size() < groupSize) {
						mutex.wait();
					} else {
						return true;
					}
				}
			}
		}

		boolean leave() throws KeeperException, InterruptedException {
			zk.delete(this.actualPath, 0);
			while (true) {
				synchronized (mutex) {
					List<String> list = zk.getChildren(barrierRoot, true);
					if (list.size() > 0) {
						mutex.wait();
					} else {
						return true;
					}
				}
			}
		}
	}
}
