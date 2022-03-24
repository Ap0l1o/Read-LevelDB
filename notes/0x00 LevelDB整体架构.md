# LevelDB 整体架构
## 0x00 开始
犹豫就会败北，一直犹豫，一直失败。
## 0x01 整体架构

levelDB的整体结构图如下所示。

![overview.drawio](https://ap0l1o.oss-cn-qingdao.aliyuncs.com/img/overview_levelDB.drawio.png)

levelDB的静态结构由以下六个部分组成：

- Log文件（磁盘）：存储在磁盘中，当写入一个KV数据的时候，会先将其写入到Log文件中，写入成功后再将其写入到MemTable中。这包含了一次磁盘顺序I/O和一次内存I/O，因为磁盘I/O是顺序的（也即append追加），而且仅需要一次，这也是LevelDB写入速度快的原因。

- MemTable（内存）：存在于内存中的数据，使用Skiplist实现，数据的最新版本存在这里。
- Immutable MemTable（内存）：存在于内存中的数据，MemTable中的数据达到其容量上限后会转为Immutable MemTable，与MemTable的区别在于Imuutable MemTable是只读的。
- SSTable（磁盘）：存储在磁盘中的数据，由内存中的Imuutable MemTable不断写到磁盘中然后进行Compaction而成。Compaction操作进行合并，并剔除无效数据。
- Manifest文件（磁盘）：Manifest文件中记录SST文件在不同Level的分布，单个SST文件的最大最小key，以及其他一些LevelDB需要的元信息。
- Current文件（磁盘）：LevelDB启动时的首要任务就是要找到当前的Manifest文件，而Manifest文件可能有多个。Current文件简单记录了当前版本的Manifest文件名。

## 0x01 Log文件

因为在将KV数据写入到MemTable之前会先将其写入到磁盘Log文件，而磁盘中的文件是持久化的（不会掉电丢失），所以Log文件的主要作用就是故障恢复，避免在系统发生故障而宕机时丢失数据。

### a. log文件的结构

LevelDB将一个Log文件切分为以32K为单位的Block，Log文件中的数据以Block为单位进行组织。所以，从物理存储布局上来看，一个Log文件是由多个连续的Block组成的。

LevelDB将Block作为基本的读取单位，但是出于一致性考虑并未将Block作为基本的写入单位，而是将record作为写入的基本单位，每次写Log操作都会写入一条record数据。record的结构如下图所示。

 ![record.drawio](https://ap0l1o.oss-cn-qingdao.aliyuncs.com/img/record.drawio.png)

- `checksum`：记录`type`和`data`字段的CRC检验和。
- `length`：记录存储的`data`字段的长度。
- `type`：记录每条record的逻辑结构和log文件的物理分块结构之间的关系，具体来看，有以下四种类型：
  - `FULL`：当前record完整的存储在一个物理Block里；
  - `FIRST`：record无法完整的存放在一个Block中，此record是原record的第一片；
  - `MIDDLE`：record无法完整的存放在一个Block中，此record是原record的中间分片，还不是结束分片；
  - `LAST`：record无法完整的存放在一个Block中，此record是原record的结束分片；
- `data`：记录存储的KV数据。

如下图所示，Record A 和 Record C都可以放在一个Block中，而Record B需要放在连续的三个Block中。

![block.drawio](https://ap0l1o.oss-cn-qingdao.aliyuncs.com/img/block.drawio.png)

## 0x02 Memtable & Immutable MemTable文件

所有的KV数据都是存储在MemTable、Immutable MemTable 以及 SSTable中的，其中SSTable存储在磁盘上，MemTable和Immutable MemTable都存储在内存中，并且具有相同的结构，区别在于：

- MemTable允许写入和读取，当一个MemTable中写入的数据量达到预定义的上限时会自动转为Immutable MemTable；
- Immutable MemTable是只读的，不允许写操作，Immutable MemTable等待Flush操作将其顺序写到磁盘上，此时会自动生成新的MemTable；

LevelDB的MemTable提供了KV数据的读写及删除接口，但是事实上MemTable并不提供真正的删除操作，删除某个KV数据是通过插入一条标记删除的记录完成的，通过此记录来对其进行标记，真正的删除操作是在Compaction的过程中完成的，在Compaction过程中会丢弃标记的KV数据。

在LevelDB中，MemTable中的KV数据是根据key的字典序存储的，是有序的。在插入新的KV数据时LevelDB必须将其插入到合适的位置上以维持其有序性。事实上，LevelDB的MemTable是一个借口累，真正的操作是通过背后的SkipList（跳表）实现的，所以MemTable的核心数据结构是一个跳表。

>相关源码位置：
>
>db/MemTable.h
>db/MemTable.cc
>db/dbformat.h
>db/dbformat.cc
>db/skiplist.h

## 0x03 Manifest文件

Manifest文件记录了SSTable的管理信息，例如：某个SSTable属于哪个Level，其文件名，最小key和最大key。示意图如下所示：

![Manifest.drawio](https://ap0l1o.oss-cn-qingdao.aliyuncs.com/img/Manifest.drawio.png)

## 0x04 Current文件

Current文件的内容只有记载的当前Manifest的文件名。

在LevelDB运行过程中，SSTable会不断发生变化，例如在Flush和Compaction操作时会产生新的SSTable，在Compaction时还会丢弃旧的SSTable，而Manifest也会做相应的调整，往往会产生新的Manifest文件，而Current专门用来指出当前有效的Manifest文件。

## 0xff 参考资料

- https://zhuanlan.zhihu.com/p/80684560
- https://www.ravenxrz.ink/archives/1a545f48.html
- https://zhuanlan.zhihu.com/p/67833030
- https://bean-li.github.io/leveldb-log/



