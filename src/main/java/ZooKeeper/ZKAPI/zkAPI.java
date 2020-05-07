package ZooKeeper.ZKAPI;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @ClassName zkAPI
 * @MethodDesc: TODO zkAPI功能介绍
 * @Author Movle
 * @Date 5/7/20 1:21 下午
 * @Version 1.0
 * @Email movle_xjk@foxmail.com
 **/


public class zkAPI {

    private static String connectString = "192.168.31.132:2181";

    private static int sessionTimeOut = 2000;

    private ZooKeeper zkClient = null;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeOut, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType()+"--"+watchedEvent.getPath());


                try {
                    zkClient.getChildren("/",true);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Test
    public void create() throws KeeperException, InterruptedException {
        String nodeCreated = zkClient.create("/eclipse","hello zk".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);

    }

    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/",true);

        for(String child:children){
            System.out.println(child);
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void exist() throws KeeperException, InterruptedException {

        Stat stat = zkClient.exists("/eclipse",false);
        System.out.println(stat==null?"not exist":"exist");
    }


}
