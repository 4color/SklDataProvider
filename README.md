# SklDataProvider
C# 开发的一款数据工厂
支持的数据包含：Oracle,Access,Mysql,SqlServer,Sqliste。
.Net Framework版本 2.0


# 调用方式

``` java
SklDataSource xdataSource = DataSourceFactory.CreateInstance(连接字段串, 数据库类型); 
```


```
graph LR
A[DataSourceFactory] -->B{SklDataSource}
B -->C[OraOracle]
B -->D[SqlServer]
B -->E[Sqliste]
B -->F[Mysql]
B -->G[Access]
```
我的博客

http://4color.cn/


