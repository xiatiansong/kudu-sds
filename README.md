# kudu-sds
This project make apache kudu as structured data store, use it for nosql ORM operate, also offer sql operate base on apache impala or prestodb

# nosql.kudu.core
针对另２个项目封装的基础操作，主要对apache kudu的操作进行封装，使用工厂模式和连接池管理session和kudu client，达到高效操作，此部分封装是把apache kudu当作nosql数据库，并提供类似ORM的操作；如果需要进行sql操作，在引入jdbc连接池的情况下，使用apache impala或prestodb作为查询引擎(如果底层是apache kudu，sql也能满足增删改查，但会比apache kudu的接口慢)，提供olap能力或sql能力．

# nosql.kudu.sdk
以SDK的形式存在，提供给操作数据的应用，高效进行数据的增删改查

# nosql.kudu.gateway
以web服务的形式存在，提供restfull接口给操作数据的应用，通过http接口高效进行数据的增删改查，这里主要使用场景是不希望引入sdk或多语言场景.

# example
```
// get operate
KuduDataTemplate kuduDataTemplate = new KuduDataTemplate();
String table = "device_attribute_cur";

//单条查询
for (int i = 0; i < 100; i++) {
  Map<String, Object> map = new HashMap<String, Object>();
  map.put("device_id", "" + (i + 524642));
  map.put("object_name", "Device");
  map.put("attribute_name", "device_fw_ver");
  //map.put("id", i + 2000);
  long start = System.currentTimeMillis();
  Map<String, Object> result = kuduDataTemplate.getRow("test", table, map);
  long stop = System.currentTimeMillis();
  System.out.println("单条查询耗时：" + (stop - start));
  System.out.println(result);
}

kuduDataTemplate.close();
    
//batch insert
KuduDataTemplate kdt = new KuduDataTemplate();
List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>(200);
for (int i = 0; i < 200; i++) {
  Map<String, Object> map = new HashMap<String, Object>();
  map.put("deviceid", "123");
  map.put("objectname", "Humiture");
  map.put("attributename", "temperature");
  map.put("presentvalue", "3563");
  map.put("updatedate", "2017-11-02");
  map.put("updatetime", "18:42:05.0");
  map.put("userid", "16671559");
  rows.add(map);
}
for (int i = 0; i < 1000; i++) {
  long start = System.currentTimeMillis();
  kdt.insertRowList("test", "devicecurrentattribute", rows);
  long stop = System.currentTimeMillis();
  System.out.println(stop - start);
}

//scan操作
KuduDataTemplate kuduDataTemplate = new KuduDataTemplate();
String db = "test";
try {
  for(int i = 0;i < 10; i++){
    Map<String,Object> data = new HashMap<String, Object>();
    data.put("position_id", "po." + i);
    //data.put("user_id", "ui." + i);
    kuduDataTemplate.insertRow(db, "td_position_h_test", data);
  }
  List<Map<String, Object>> result = kuduDataTemplate.scanByPredicates(db, "td_position_h_test", new HashMap<>(), 100);

  System.out.println("-------------------------");
  for(Map<String, Object> map : result){
    System.out.println(map);
  }
  kuduDataTemplate.close();
} catch (Exception e) {
  e.printStackTrace();
}

//query by sql
KuduDataTemplate kuduDataTemplate = new KuduDataTemplate();
String sql = "select * from default.test where id = 1";
try {
  List<Map<String, Object>> result = kuduDataTemplate.queryBySql(sql);
  System.out.println(result);
  kuduDataTemplate.close();
} catch (Exception e) {
  e.printStackTrace();
}
```

