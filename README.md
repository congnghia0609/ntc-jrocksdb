# ntc-jrocksdb
ntc-jrocksdb is a module swapper java RocksDB

## Maven
```Xml
<dependency>
    <groupId>com.streetcodevn</groupId>
    <artifactId>ntc-jrocksdb</artifactId>
    <version>1.0.0</version>
</dependency>
```

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
