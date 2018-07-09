package test;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 测试zookeeper各个api
 */
public class ZookeeperTest {
    /**
     * 超时时间3秒
     */
    private static final int SESSION_TIMEOUT = 3000;
    /**
     * zookeeper分布式节点
     */
    private static final String ZK_HOSTS = "hadoop004,hadoop001,hadoop002";
    /**
     * 测试的znode路径
     */
    private static final String rootPath = "/testzk";

    public static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperTest.class);

    protected CountDownLatch countDownLatch = new CountDownLatch(1);
    /**
     * 监控zookeeper节点的状态，增删改的时候通知
     */
    private Watcher watcher =  new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getState() == Event.KeeperState.SyncConnected) {
                LOGGER.info("process : " + event.getState());
                countDownLatch.countDown();
            }
        }
    };

    private ZooKeeper zooKeeper;

    @Before
    /**
     * 链接ZK
     */
    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(ZK_HOSTS, SESSION_TIMEOUT, watcher);
        // 防止zookeeper未连接成功就执行下一步操作，所以这边阻塞，等待链接成功
        countDownLatch.await();
    }

    @After
    /**
     * 断开ZK
     */
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Test
    /**
     * 测试创建节点
     */
    public void create() throws KeeperException, InterruptedException {
        String result = null;

        try{
            Stat res = zooKeeper.exists(rootPath, null);
            // 节点不存在在
            if (res == null) {
                /**
                 * 创建节点 /testzk
                 * 节点数据 zktest01
                 * Ids.OPEN_ACL_UNSAFE  将所有ADMIN之外的权限授予每个人
                 * PERSISTENT  创建一个持久化的节点，及节点链接断开，节点依旧存在
                 */
                result = zooKeeper.create(rootPath, "zktest01".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        LOGGER.info("create result : {}", result);
    }

    @Test
    /**
     * 测试获取节点数据
     */
    public void getNodeData(){
        String result = null;
        try {
            byte[] res = zooKeeper.getData(rootPath, null, null);
            result = new String(res);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
        LOGGER.info("create result : {}", result);
    }

    @Test
    /**
     * 检测节点变化
     */
    public void watchNodeChange() {
        String result = null;
        try {
            byte[] res = zooKeeper.getData(rootPath, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDataChanged) {
                        LOGGER.info("node data has changed: event type is {}", event.getType());
                    }
                }
            }, null);
            result = new String(res);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }

        LOGGER.info("getdata result : {}", result);

        try {
            // 这里来改变节点数据
            zooKeeper.setData(rootPath, "testchangedata".getBytes(), -1);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
    }

    @Test
    /**
     * 测试删除节点
     */
    public void deleteNode() {
        try {
            zooKeeper.delete(rootPath, -1);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            Assert.fail();
        }
    }
}
