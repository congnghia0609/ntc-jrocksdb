/*
 * Copyright 2016 nghiatc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ntc.rocksdb;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.rocksdb.*;
import static org.rocksdb.util.ByteUtil.bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author nghiatc
 * @since Jan 21, 2016
 */
public class RDBSingleConnection {

    private final Logger logger = LoggerFactory.getLogger(RDBSingleConnection.class);

    private static Map<String, RDBSingleConnection> mapInstanceRDBSingleConn = new ConcurrentHashMap<String, RDBSingleConnection>();
    private static Map<String, String> mapInstanceRDBDir = new ConcurrentHashMap<>();
    private static Lock lockInstance = new ReentrantLock();
    
    private String dbDirectory;
    private RocksDB db;
    private Options options;
    private NRSerializer nrs;

    public String getDbDirectory() {
        return dbDirectory;
    }

    public RocksDB getDb() {
        return db;
    }

    public Options getOptions() {
        return options;
    }

    public NRSerializer getNRSerializer() {
        return nrs;
    }

    private RDBSingleConnection() {
    }

    private void init(String pathDB) throws RocksDBException {
        if (pathDB == null || pathDB.isEmpty()) {
            logger.error("=====>>> Path to DB not empty...");
            throw new ExceptionInInitializerError("Path to DB not empty.");
        }
        File dbDir = new File(pathDB);
        if (mapInstanceRDBDir.containsKey(dbDir.getAbsolutePath())) {
            throw new ExceptionInInitializerError("Path directory database was used by another service: " + pathDB);
        }
        mapInstanceRDBDir.put(dbDir.getAbsolutePath(), pathDB);
        dbDirectory = pathDB;
        if (!dbDir.exists()) {
            if (!dbDir.mkdirs()) {
                throw new ExceptionInInitializerError("Path directory database can not created for: " + pathDB);
            }
        }
        nrs = new NRSerializer();
        db = RocksDB.open(options, dbDirectory);
    }
    
    private RDBSingleConnection(String pathDB) throws RocksDBException {
        options = new Options().setCreateIfMissing(true);
        init(pathDB);
    }
    
    private RDBSingleConnection(String pathDB, Options opts) throws RocksDBException {
        if (opts == null) {
            opts = new Options().setCreateIfMissing(true);
        }
        options = opts;
        init(pathDB);
    }

    public static RDBSingleConnection getInstance(String pathDB) throws RocksDBException {
        if (pathDB == null || pathDB.isEmpty()) {
            return null;
        }
        RDBSingleConnection _instance = mapInstanceRDBSingleConn.containsKey(pathDB) ? mapInstanceRDBSingleConn.get(pathDB) : null;
        if (_instance == null) {
            lockInstance.lock();
            try {
                _instance = mapInstanceRDBSingleConn.containsKey(pathDB) ? mapInstanceRDBSingleConn.get(pathDB) : null;
                if (_instance == null) {
                    _instance = new RDBSingleConnection(pathDB);
                    mapInstanceRDBSingleConn.put(pathDB, _instance);
                }
            } finally {
                lockInstance.unlock();
            }
        }
        return _instance;
    }
    
    public static RDBSingleConnection getInstance(String pathDB, Options opts) throws RocksDBException {
        if (pathDB == null || pathDB.isEmpty()) {
            return null;
        }
        RDBSingleConnection _instance = mapInstanceRDBSingleConn.containsKey(pathDB) ? mapInstanceRDBSingleConn.get(pathDB) : null;
        if (_instance == null) {
            lockInstance.lock();
            try {
                _instance = mapInstanceRDBSingleConn.containsKey(pathDB) ? mapInstanceRDBSingleConn.get(pathDB) : null;
                if (_instance == null) {
                    _instance = new RDBSingleConnection(pathDB, opts);
                    mapInstanceRDBSingleConn.put(pathDB, _instance);
                }
            } finally {
                lockInstance.unlock();
            }
        }
        return _instance;
    }

    public void close() {
        try {
            if (db != null) {
                // be sure to release the c++ pointer
                db.close();
            }
            if (options != null) {
                // be sure to dispose c++ pointers
                options.dispose();
            }
        } catch (Exception e) {
            logger.error("close: ", e);
        }
    }

    public void put(String key, String value) {
        try {
            if (key != null && !key.isEmpty() && value != null && !value.isEmpty()) {
                db.put(bytes(key), bytes(value));
            }
        } catch (Exception ex) {
            logger.error("put: ", ex);
        }
    }

    public void putBatch(Map<String, String> mapData) throws IOException {
        if (mapData != null && !mapData.isEmpty()) {
            WriteBatch batch = new WriteBatch();
            try {
                for (String key : mapData.keySet()) {
                    String value = mapData.get(key);
                    if (key != null && !key.isEmpty() && value != null && !value.isEmpty()) {
                        batch.put(bytes(key), bytes(value));
                    }
                }
                db.write(new WriteOptions(), batch);
            } catch (Exception ex) {
                logger.error("putBatch: ", ex);
            } finally {
                // Make sure you close the batch to avoid resource leaks.
                batch.close();
            }
        }
    }

    public void putByte(byte[] key, byte[] value) {
        try {
            if (key != null && key.length > 0 && value != null && value.length > 0) {
                db.put(key, value);
            }
        } catch (Exception ex) {
            logger.error("putByte: ", ex);
        }
    }

    public void putBatchByte(Map<byte[], byte[]> mapData) throws IOException {
        if (mapData != null && !mapData.isEmpty()) {
            WriteBatch batch = new WriteBatch();
            try {
                for (byte[] key : mapData.keySet()) {
                    byte[] value = mapData.get(key);
                    if (key != null && key.length > 0 && value != null && value.length > 0) {
                        batch.put(key, value);
                    }
                }
                db.write(new WriteOptions(), batch);
            } catch (Exception ex) {
                logger.error("putBatchByte: ", ex);
            } finally {
                // Make sure you close the batch to avoid resource leaks.
                batch.close();
            }
        }
    }

    public String get(String key) {
        try {
            if (key != null && !key.isEmpty()) {
                byte[] bv = db.get(bytes(key));
                return bv != null ? nrs.deserializeString(bv) : null;
            }
        } catch (Exception ex) {
            logger.error("get: ", ex);
        }
        return null;
    }

    public Map<String, String> getList(List<String> listKey) throws RocksDBException {
        Map<String, String> rs = new LinkedHashMap<>();
        if (listKey != null && !listKey.isEmpty()) {
            for (String key : listKey) {
                if (key != null && !key.isEmpty()) {
                    byte[] bv = db.get(bytes(key));
                    String value = bv != null ? nrs.deserializeString(bv) : null;
                    rs.put(key, value);
                }
            }
        }
        return rs;
    }

    public byte[] getByte(byte[] key) {
        try {
            if (key != null && key.length > 0) {
                return db.get(key);
            }
        } catch (Exception ex) {
            logger.error("getByte: ", ex);
        }
        return null;
    }

    public Map<byte[], byte[]> getListByte(List<byte[]> listKey) throws RocksDBException {
        Map<byte[], byte[]> rs = new LinkedHashMap<>();
        if (listKey != null && !listKey.isEmpty()) {
            for (byte[] key : listKey) {
                if (key != null && key.length > 0) {
                    byte[] value = db.get(key);
                    rs.put(key, value);
                }
            }
        }
        return rs;
    }

    public void delete(String key) {
        try {
            if (key != null && !key.isEmpty()) {
                db.delete(bytes(key));
            }
        } catch (Exception ex) {
            logger.error("delete: ", ex);
        }
    }

    public void deleteList(List<String> listKey) throws RocksDBException {
        if (listKey != null && !listKey.isEmpty()) {
            for (String key : listKey) {
                if (key != null && !key.isEmpty()) {
                    db.delete(bytes(key));
                }
            }
        }
    }

    public void deleteBatch(List<String> listKey) throws IOException {
        if (listKey != null && !listKey.isEmpty()) {
            WriteBatch batch = new WriteBatch();
            try {
                for (String key : listKey) {
                    if (key != null && !key.isEmpty()) {
                        batch.delete(bytes(key));
                    }
                }
                db.write(new WriteOptions(), batch);
            } catch (Exception ex) {
                logger.error("deleteBatch: ", ex);
            } finally {
                // Make sure you close the batch to avoid resource leaks.
                batch.close();
            }
        }
    }

    public void deleteByte(byte[] key) {
        try {
            if (key != null && key.length > 0) {
                db.delete(key);
            }
        } catch (Exception ex) {
            logger.error("deleteByte: ", ex);
        }
    }

    public void deleteListByte(List<byte[]> listKey) throws RocksDBException {
        if (listKey != null && !listKey.isEmpty()) {
            for (byte[] key : listKey) {
                if (key != null && key.length > 0) {
                    db.delete(key);
                }
            }
        }
    }

    public void deleteBatchByte(List<byte[]> listKey) throws IOException {
        if (listKey != null && !listKey.isEmpty()) {
            WriteBatch batch = new WriteBatch();
            try {
                for (byte[] key : listKey) {
                    if (key != null && key.length > 0) {
                        batch.delete(key);
                    }
                }
                db.write(new WriteOptions(), batch);
            } catch (Exception ex) {
                logger.error("deleteBatchByte: ", ex);
            } finally {
                // Make sure you close the batch to avoid resource leaks.
                batch.close();
            }
        }
    }
    
    synchronized public int incInt(String key, int value) throws RocksDBException {
        int rs = 0;
        if (key != null && !key.isEmpty()) {
            byte[] bk = nrs.serializeString(key);
            byte[] bv = db.get(bk);
            rs = bv != null ? nrs.deserializeInt(bv) + value : value;
            db.put(bk, nrs.serializeInt(rs));
        }
        return rs;
    }
    
    synchronized public long incLong(String key, long value) throws RocksDBException {
        long rs = 0L;
        if (key != null && !key.isEmpty()) {
            byte[] bk = nrs.serializeString(key);
            byte[] bv = db.get(bk);
            rs = bv != null ? nrs.deserializeLong(bv) + value : value;
            db.put(bk, nrs.serializeLong(rs));
        }
        return rs;
    }
}
