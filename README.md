# ntc-jrocksdb
ntc-jrocksdb is a module swapper java RocksDB

## Usage
```java
NRSerializer nrs = new NRSerializer();
String dbPath = "./db";
RDBSingleConnection conn = RDBSingleConnection.getInstance(dbPath);


String key = "nghiatc";
String value = "handsome";
// Put Data
conn.put(key, value);
String rs1 = conn.get(key);
Assert.assertEquals(value, rs1);
// Get Data
String rs2 = nrs.deserializeString(conn.getByte(nrs.serializeString(key)));
Assert.assertEquals(value, rs2);
// Delete Data
conn.delete(key);
String rs3 = conn.get(key);
Assert.assertEquals(null, rs3);


conn.close();
```

## License
This code is under the [Apache License v2](https://www.apache.org/licenses/LICENSE-2.0).  
