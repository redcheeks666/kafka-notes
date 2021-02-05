

# kafka源码环境搭建 And Run

操作系统:Windows系统

一台已经启动好的zookeeper服务

**jdk环境**

配置环境变量

```shell
C:\Users\EDZ>java -version
java version "1.8.0_131"
Java(TM) SE Runtime Environment (build 1.8.0_131-b11)
Java HotSpot(TM) 64-Bit Server VM (build 25.131-b11, mixed mode)
```

**scala环境**

配置环境变量

```shell
C:\Users\EDZ>scala -version
Scala code runner version 2.11.12 -- Copyright 2002-2017, LAMP/EPFL
```

**Gradle(项目构建工具)**

> 下载地址 https://gradle.org/releases/
>
> 配置环境变量

```shell
C:\Users\EDZ>gradle -v

------------------------------------------------------------
Gradle 4.9
------------------------------------------------------------

Build time:   2018-07-16 08:14:03 UTC
Revision:     efcf8c1cf533b03c70f394f270f46a174c738efc

Kotlin DSL:   0.18.4
Kotlin:       1.2.41
Groovy:       2.4.12
Ant:          Apache Ant(TM) version 1.9.11 compiled on March 23 2018
JVM:          1.8.0_131 (Oracle Corporation 25.131-b11)
OS:           Windows 7 6.1 amd64
```

**kafka源码包**

> 下载地址 http://kafka.apache.org/downloads

kafka-2.3.1-src.tgz

解压

进入根目录 打开build.gradle文件

修改两个仓库地址属性 (url替换为阿里镜像为了下载速度）

```groovy
allprojects {

  repositories {
	maven {
      url "http://maven.aliyun.com/nexus/content/groups/public/"
    }
	maven {
      url "https://plugins.gradle.org/m2/"
    }
	mavenCentral()
    google()
    jcenter()
  }
   ...
}
 buildscript {
  repositories {
	maven {
      url "http://maven.aliyun.com/nexus/content/groups/public/"
    }
	maven {
      url "https://plugins.gradle.org/m2/"
    }
	mavenCentral()
    google()
    jcenter()
  
   ...
 }
```

修改gradle.properties文件

```properties
version=2.3.1
#修改此处，此处是scala版本 与上面安装的scala版本一致
scalaVersion=2.11.12
task=build
org.gradle.jvmargs=-Xmx1024m -Xss2m
```

**gradle构建kafka**

kafka-2.3.1-src目录 cmd 

gradle idea 走你

```shell
gradle idea
```

出了个BUG

```shell
> Could not resolve all artifacts for configuration ':classpath'.
   > Could not find com.diffplug.spotless:spotless-plugin-gradle:3.23.0.
```

好像是在阿里云的仓库没找到对应的插件

然后在build.gradle文件中 两个仓库地址下面添加了

```groovy
maven { url "https://plugins.gradle.org/m2/"}
```

再次构建

```shell
BUILD SUCCESSFUL in 6m 36s
31 actionable tasks: 31 executed
```

**导入IDEA并运行**

使用IDEA 打开build.gradle文件，选择Open As Project, 选中"Create Directories for empty contents roots Automatically"

**idea安装scala插件**

idea-Plugins-scala 下载安装重启

**添加log4j.properties文件**

将config目录下的log4j.properties文件拷贝到core/src/main/scala目录下

**配置server.properties文件**

```properties
zookeeper.connect=112.126.***.***:2181
log.dirs=D:\\Download\\log
```

**配置启动Configurations**

![idea源码启动配置](C:\Users\EDZ\Desktop\新建文件夹\picture\idea源码启动配置.png)



启动~

报错 ! 好像少包~

```shell
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
```

解决方案：添加依赖

找到项目中的\kafka-2.3.1-src\gradle\dependencies.gradle

在下面位置添加

```groovy
versions += [
        ...
        log4j_bugfix: "1.7.30"
]
libs += [
        ... 
        log4jBugFix:"org.slf4j:slf4j-log4j12:$versions.log4j_bugfix"
]
```

打开\kafka-2.3.1-src\build.gradle

```groovy
project(':core') {
  println "Building project 'core' with Scala version ${versions.scala}"

  apply plugin: 'scala'
  apply plugin: "org.scoverage"
  archivesBaseName = "kafka_${versions.baseScala}"

  dependencies {
    compile project(':clients')
    compile libs.jacksonDatabind
    compile libs.jacksonModuleScala
    compile libs.jacksonDataformatCsv
    compile libs.jacksonJDK8Datatypes
    compile libs.joptSimple
    compile libs.metrics
    compile libs.scalaLibrary
    // only needed transitively, but set it explicitly to ensure it has the same version as scala-library
    compile libs.scalaReflect
    compile libs.scalaLogging
    compile libs.slf4jApi
    compile(libs.zkclient) {
      exclude module: 'zookeeper'
    }
    compile(libs.zookeeper) {
      exclude module: 'slf4j-log4j12'
      exclude module: 'log4j'
      exclude module: 'netty'
    }
    //当前位置新增如下配置
	compile(libs.log4jBugFix)
      
      ...
}
```

再启动~

再次报错~

```shell
No appenders could be found for logger (kafka.utils.Log4jControllerRegistration$)
```

在\kafka-2.3.1-src\core\src\main\目录下新建resources文件夹，把log4j.properties挪入。刷新Gradle ,再次启动！

```shell
[2020-08-10 15:29:54,487] INFO [TransactionCoordinator id=6] Starting up. (kafka.coordinator.transaction.TransactionCoordinator)
[2020-08-10 15:29:54,492] INFO [Transaction Marker Channel Manager 6]: Starting (kafka.coordinator.transaction.TransactionMarkerChannelManager)
[2020-08-10 15:29:54,492] INFO [TransactionCoordinator id=6] Startup complete. (kafka.coordinator.transaction.TransactionCoordinator)
[2020-08-10 15:29:54,560] INFO [/config/changes-event-process-thread]: Starting (kafka.common.ZkNodeChangeNotificationListener$ChangeEventProcessThread)
[2020-08-10 15:29:54,676] INFO [SocketServer brokerId=6] Started data-plane processors for 1 acceptors (kafka.network.SocketServer)
[2020-08-10 15:29:54,683] INFO Kafka version: 2.3.1 (org.apache.kafka.common.utils.AppInfoParser)
[2020-08-10 15:29:54,683] INFO Kafka commitId: unknown (org.apache.kafka.common.utils.AppInfoParser)
[2020-08-10 15:29:54,684] INFO Kafka startTimeMs: 1597044594679 (org.apache.kafka.common.utils.AppInfoParser)
[2020-08-10 15:29:54,686] INFO [KafkaServer id=6] started (kafka.server.KafkaServer)
```

成功！
