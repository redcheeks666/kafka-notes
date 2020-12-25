# 三台阿里云服务器搭建zookeeper,Kafka集群

## 准备

1. **三台云服务器,zookeeper Linux安装包，kafka Linux安装包**
2. **安装jdk，配置java环境**
3. **配置三台服务器的SSH免密登录**
4. **关闭防火墙**
5. **配置主机名映射 /etc/hosts**
6. **配置阿里云安全组ip，添加ip信任端口, ip:0.0.0.0/0 端口1/65535**

## 安装配置zookeeper

> ```shell
> mkdir /home/zookeeper
> ```

> ```shell
> tar  -zxvf  zookeeper-3.4.6.jar -C  /home/zookeeper #解压
> ```

> ```shell
> cd	/home/zookeeper/zookeeper-3.4.6/
> ```

> ```shell
> cp	/conf/zoo_sample.cfg 	zoo.cfg
> ```

> ```shell
> mkdir	/home/zookeeper/zkdata	#并且在该文件下新建文件名为myid的文件，填入此节点编号，如0/1/2
> ```

> ```shell
> mkdir	/home/zookeeper/zklog
> ```

> ```shell
> vi	zoo.cfg
> ```

```java
# The number of milliseconds of each tick
tickTime=2000
# The number of ticks that the initial 
# synchronization phase can take
initLimit=10
# The number of ticks that can pass between 
# sending a request and getting an acknowledgement
syncLimit=5
# the directory where the snapshot is stored.
# do not use /tmp for storage, /tmp here is just 
# example sakes.
dataDir=/home/zookeeper/zkdata
dataLogDir=/home/zookeeper/zklog
# the port at which the clients will connect
clientPort=2181
# the maximum number of client connections.
# increase this if you need to handle more clients
#maxClientCnxns=60
#
# Be sure to read the maintenance section of the 
# administrator guide before turning on autopurge.
#
# http://zookeeper.apache.org/doc/current/zookeeperAdmin.html#sc_maintenance
#
# The number of snapshots to retain in dataDir
#autopurge.snapRetainCount=3
# Purge task interval in hours
# Set to "0" to disable auto purge feature
#autopurge.purgeInterval=1
server.0=zk01:2888:3888	
server.1=zk02:2888:3888
server.2=zk03:2888:3888

```

```shell
server.num=ip:port01:port02 

#num:与ip对应的myid编号保持一致

#ip:节点ip，(在阿里的云服务器上，通过自己的公网ip，是不能监听自己本机的端口的。将本机的ip改为0.0.0.0，通过这个ip来监听本机的这两个端口。)
#例如 本机节点 IP为zk01 配置为
#server.0=0.0.0.0:2888:3888	
#server.1=zk02:2888:3888
#server.2=zk03:2888:3888
#port01:节点通信端口

#port02:Leader选举端口

```

```shell
三台节点[root@zkX zookeeper-3.4.6]# bin/zkServer.sh start   (zk启动)
	   [root@zkX zookeeper-3.4.6]# bin/zkServer.sh status (zk节点状态)
	   [root@zkX zookeeper-3.4.6]# bin/zkServer.sh restart  (zk节点重启)

```

## 安装配置Kafka

> ```shell
> mkdir /home/kafka
>
> ```

> ```shell
> tar  -zxvf  kafka_2.12-2.3.1.tgz -C  /home/zookeeper #解压 
>
> ```

> ```shell
> cd  /home/kafka/kafka_2.12-2.3.1
>
> ```

> ```shell
> vi	/config/server.properties
>
> ```

```shell
#zookeeper的集群地址
zookeeper.connect=zk01:2181,zk02:2181,zk03:2181
#broker.id 配置brokerid 必须保证每台节点唯一
broker.id=0/1/2

```

> ```shell
> bin/kafka-server-start.sh config/server.properties #启动kafka
>
> ```



