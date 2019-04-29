package com.lv.learn.zk.demo;

import org.apache.zookeeper.*;

public class ZkDemo {

    public static void main(String[] args) throws Exception {

        ZooKeeper zk = new ZooKeeper("mas130:2181", 5000, new Watcher() {

            // 监控所有被触发的事件
            @Override
            public void process(WatchedEvent event) {
                System.out.println("====== 监控所有被触发的事件 ===== EVENT:" + event.getType());
            }
        });

        System.out.println("*******************************************************");
        // 查看根节点的子节点
        System.out.println("查看根节点的子节点:ls / => " + zk.getChildren("/", true));

        System.out.println("*******************************************************");
        // 创建一个目录节点
        if (zk.exists("/node", true) == null) {
            zk.create("/node", "test-zk".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("创建一个目录节点:create /node test-zk");
        }

        System.out.println("*******************************************************");
        byte[] data = zk.getData("/node", false, null);
        System.out.println("查看/node节点数据:get /node => " + new String(data));

        System.out.println("查看根节点:ls / => " + zk.getChildren("/", true));

        // 创建一个子目录节点
        if (zk.exists("/node/sub1", true) == null) {
            zk.create("/node/sub1", "sub1".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("创建一个子目录节点:create /node/sub1 sub1");
            // 查看node节点
            System.out.println("查看node节点:ls /node => "
                    + zk.getChildren("/node", true));
        }
        System.out.println("*******************************************************");

        // 修改节点数据
        if (zk.exists("/node", true) != null) {
            zk.setData("/node", "changed".getBytes(), -1);
            System.out.println("查看/node节点数据:get /node => " + new String(zk.getData("/node", false, null)));
        }
        System.out.println("*******************************************************");


        // 删除节点
        if (zk.exists("/node/sub1", true) != null) {
            zk.delete("/node/sub1", -1);
            zk.delete("/node", -1);

            // 查看根节点
            System.out.println("删除节点:ls / => " + zk.getChildren("/", true));
        }

        // 关闭连接
        zk.close();
    }
}
