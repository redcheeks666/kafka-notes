

# kafkaԴ�뻷��� And Run

����ϵͳ:Windowsϵͳ

һ̨�Ѿ������õ�zookeeper����

**jdk����**

���û�������

```shell
C:\Users\EDZ>java -version
java version "1.8.0_131"
Java(TM) SE Runtime Environment (build 1.8.0_131-b11)
Java HotSpot(TM) 64-Bit Server VM (build 25.131-b11, mixed mode)
```

**scala����**

���û�������

```shell
C:\Users\EDZ>scala -version
Scala code runner version 2.11.12 -- Copyright 2002-2017, LAMP/EPFL
```

**Gradle(��Ŀ��������)**

> ���ص�ַ https://gradle.org/releases/
>
> ���û�������

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

**kafkaԴ���**

> ���ص�ַ http://kafka.apache.org/downloads

kafka-2.3.1-src.tgz

��ѹ

�����Ŀ¼ ��build.gradle�ļ�

�޸������ֿ��ַ���� (url�滻Ϊ���ﾵ��Ϊ�������ٶȣ�

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

�޸�gradle.properties�ļ�

```properties
version=2.3.1
#�޸Ĵ˴����˴���scala�汾 �����氲װ��scala�汾һ��
scalaVersion=2.11.12
task=build
org.gradle.jvmargs=-Xmx1024m -Xss2m
```

**gradle����kafka**

kafka-2.3.1-srcĿ¼ cmd 

gradle idea ����

```shell
gradle idea
```

���˸�BUG

```shell
> Could not resolve all artifacts for configuration ':classpath'.
   > Could not find com.diffplug.spotless:spotless-plugin-gradle:3.23.0.
```

�������ڰ����ƵĲֿ�û�ҵ���Ӧ�Ĳ��

Ȼ����build.gradle�ļ��� �����ֿ��ַ���������

```groovy
maven { url "https://plugins.gradle.org/m2/"}
```

�ٴι���

```shell
BUILD SUCCESSFUL in 6m 36s
31 actionable tasks: 31 executed
```

**����IDEA������**

ʹ��IDEA ��build.gradle�ļ���ѡ��Open As Project, ѡ��"Create Directories for empty contents roots Automatically"

**idea��װscala���**

idea-Plugins-scala ���ذ�װ����

**���log4j.properties�ļ�**

��configĿ¼�µ�log4j.properties�ļ�������core/src/main/scalaĿ¼��

**����server.properties�ļ�**

```properties
zookeeper.connect=112.126.***.***:2181
log.dirs=D:\\Download\\log
```

**��������Configurations**

![ideaԴ����������](C:\Users\EDZ\Desktop\�½��ļ���\picture\ideaԴ����������.png)



����~

���� ! �����ٰ�~

```shell
SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
SLF4J: Defaulting to no-operation (NOP) logger implementation
SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
```

����������������

�ҵ���Ŀ�е�\kafka-2.3.1-src\gradle\dependencies.gradle

������λ�����

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

��\kafka-2.3.1-src\build.gradle

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
    //��ǰλ��������������
	compile(libs.log4jBugFix)
      
      ...
}
```

������~

�ٴα���~

```shell
No appenders could be found for logger (kafka.utils.Log4jControllerRegistration$)
```

��\kafka-2.3.1-src\core\src\main\Ŀ¼���½�resources�ļ��У���log4j.propertiesŲ�롣ˢ��Gradle ,�ٴ�������

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

�ɹ���
