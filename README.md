# kudu-sds
This project make apache kudu as structured data store, use it for nosql ORM operate, also offer sql operate base on apache impala or prestodb

# nosql.kudu.core
针对另２个项目封装的基础操作，主要对apache kudu的操作进行封装，使用工厂模式和连接池管理session和kudu client，达到高效操作，此部分封装是把apache kudu当作nosql数据库，并提供类似ORM的操作；如果需要进行sql操作，在引入jdbc连接池的情况下，使用apache impala或prestodb作为查询引擎(如果底层是apache kudu，sql也能满足增删改查，但会比apache kudu的接口慢)，提供olap能力或sql能力．

# nosql.kudu.sdk
以SDK的形式存在，提供给操作数据的应用，高效进行数据的增删改查

# nosql.kudu.gateway
以web服务的形式存在，提供restfull接口给操作数据的应用，通过http接口高效进行数据的增删改查，这里主要使用场景是不希望引入sdk或多语言场景.

# example
