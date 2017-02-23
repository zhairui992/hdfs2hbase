### hdfs2hbase
#### 说明
用于hdfs中文本数据向hbase中批量导入，使用mr任务生成hfile，再用hbase loadbulk 接口导入hbase。
例子适用于 单列族 第一次导入。
效率还是可以。

#### 使用
git clone项目
注意修改 HFileGenerator HbaseMapper类中的写死的配置（输入输出路径、表名、列族名、列名等）
mvn clean
mvn install
target 下生成 load-hbase-1.0.jar
bin/hadoop jar load-hbase-1.0.jar com.zr.hbase.mr.HFileGenerator ../hbase/conf/hbase-site.xml#hbase配置文件

#### 例子
hadoop dfs -mkdir /user/zr/file/
hadoop dfs -put test.tsv /user/zr/file/
bin/hadoop jar load-hbase-1.0.jar com.zr.hbase.mr.HFileGenerator ../hbase/conf/hbase-site.xml

到hbase查询
hbase(main):004:0> scan "hbase_test"
ROW                                      COLUMN+CELL                                                                                                          


 1                                       column=dim:dim1, timestamp=1487833017843, value=a                                                                    
 1                                       column=dim:dim2, timestamp=1487833017843, value=b                                                                    
 2                                       column=dim:dim1, timestamp=1487833018112, value=c                                                                    
 2                                       column=dim:dim2, timestamp=1487833018112, value=d                                                                    
2 row(s) in 15.9110 seconds
