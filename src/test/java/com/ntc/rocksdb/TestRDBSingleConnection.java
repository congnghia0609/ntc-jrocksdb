/*
 * Copyright 2020 nghiatc.
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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.*;
import org.rocksdb.RocksDBException;

/**
 *
 * @author nghiatc
 * @since Jun 5, 2020
 */
public class TestRDBSingleConnection {
    private static NRSerializer nrs;
    private static String dbPath = "./db";
    private static RDBSingleConnection conn;
    
    @BeforeClass
    public static void init() {
        try {
            nrs = new NRSerializer();
            conn = RDBSingleConnection.getInstance(dbPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @AfterClass
    public static void clean() {
        try {
            conn.close();
            // Delete folder db. https://www.baeldung.com/java-delete-directory
            Files.walk(new File(dbPath).toPath()).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
 
    @Before
    public void beforeEachTest() {
        //System.out.println("This is executed before each Test");
    }
 
    @After
    public void afterEachTest() {
        //System.out.println("This is executed after each Test");
    }
 
    @Test
    public void testPutGetDelete() {
        String key = "nghiatc";
        String value = "handsome";
        conn.put(key, value);
        String rs1 = conn.get(key);
        Assert.assertEquals("testPutGetDelete1", value, rs1);
        
        String rs2 = nrs.deserializeString(conn.getByte(nrs.serializeString(key)));
        //System.out.println("rs2: " + rs2);
        Assert.assertEquals("testPutGetDelete2", value, rs2);
        
        conn.delete(key);
        String rs3 = conn.get(key);
        //System.out.println("rs3: " + rs3);
        Assert.assertEquals("testPutGetDelete3", null, rs3);
    }
    
    @Test
    public void testPutGetDeleteByte() {
        String key = "key";
        int value = 0;
        conn.putByte(nrs.serializeString(key), nrs.serializeInt(value));
        int rs1 = nrs.deserializeInt(conn.getByte(nrs.serializeString(key)));
        Assert.assertEquals("testPutGetDeleteByte1", value, rs1);
        
        ++value;
        conn.putByte(nrs.serializeString(key), nrs.serializeInt(value));
        int rs2 = nrs.deserializeInt(conn.getByte(nrs.serializeString(key)));
        Assert.assertEquals("testPutGetDeleteByte2", value, rs2);
        
        conn.deleteByte(nrs.serializeString(key));
        byte[] rs3 = conn.getByte(nrs.serializeString(key));
        //System.out.println("rs3: " + rs3);
        Assert.assertEquals("testPutGetDeleteByte3", null, rs3);
    }
    
    @Test
    public void testPutGetDeleteBatch() {
        try {
            String key = "key";
            String value = "value";
            Map<String, String> mapData = new LinkedHashMap<>(); //HashMap<>();
            for (int i=0; i<30; i++) {
                mapData.put(key+i, value+i);
            }
            //System.out.println("mapData: " + mapData);
            conn.putBatch(mapData);
            List<String> listKey = new ArrayList<>(mapData.keySet());
            Collections.reverse(listKey);
            Map<String, String> mapRs1 = conn.getList(listKey);
            //System.out.println("mapRs1: " + mapRs1);
            
            Assert.assertEquals("testPutGetBatch size", mapData.size(), mapRs1.size());
            
            boolean isSame = true;
            for (String k : mapData.keySet()) {
                if (!mapRs1.containsKey(k) || !mapData.get(k).equals(mapRs1.get(k))) {
                    isSame = false;
                    break;
                }
            }
            Assert.assertEquals("testPutGetBatch check", true, isSame);
            
            conn.deleteBatch(new ArrayList<>(mapData.keySet()));
            Map<String, String> mapRs2 = conn.getList(new ArrayList<>(mapData.keySet()));
            //System.out.println("mapRs2: " + mapRs2);
            Assert.assertEquals("testDeleteBatch size", mapData.size(), mapRs2.size());
            
            boolean isNull = true;
            for (String k : mapData.keySet()) {
                if (!mapRs2.containsKey(k) || mapRs2.get(k) != null) {
                    isNull = false;
                    break;
                }
            }
            Assert.assertEquals("testDeleteBatch check", true, isNull);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testMultiConnDB() {
        try {
            int k = 5;
            int n = 1000;
            int c = initCounter();
            //System.out.println("initCounter: " + c);
            
            List<Thread> listTh = new ArrayList<>();
            for (int i=0; i<k; i++) {
                Thread th = new Thread(new CounterRunable(i, n));
                listTh.add(th);
            }
            for (Thread t : listTh) {
                t.start();
            }
            for (Thread t : listTh) {
                t.join();
            }
            
            //System.out.println("=========== End All Thread ===========");
            int end = getCounter();
            //long end = getCounter2();
            //System.out.println("Final Counter: " + end);
            Assert.assertEquals("testMultiConnDB", k*n, end);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static final String keyCounter = "counter";
    
    private class CounterRunable implements Runnable {
        private RDBSingleConnection con;
        private int index;
        private int num;
        
        public CounterRunable(int i, int n) throws IOException, RocksDBException {
            index = i;
            num = n;
            con = RDBSingleConnection.getInstance(dbPath);
        }

        @Override
        public void run() {
            for (int i=0; i<num; i++) {
                try {
                    //System.out.println("Counter " + index + " incCounter: " + incCounter());
                    incCounter2();
                    //System.out.println("Counter " + index + " incCounter2: " + incCounter2());
                    //incCounter3();
                } catch (RocksDBException ex) {
                    ex.printStackTrace();
                }
            }
        }
        
        private int incCounter() {
            byte[] bk = nrs.serializeString(keyCounter);
            int value = nrs.deserializeInt(con.getByte(bk));
            ++value;
            con.putByte(bk, nrs.serializeInt(value));
            int rs = nrs.deserializeInt(con.getByte(bk));
            return rs;
        }
        
        private int incCounter2() throws RocksDBException {
            return con.incInt(keyCounter, 1);
        }
        
        private long incCounter3() throws RocksDBException {
            return con.incLong(keyCounter, 1);
        }
    }
    
    private int initCounter() {
        int value = 0;
        byte[] bk = nrs.serializeString(keyCounter);
        conn.putByte(bk, nrs.serializeInt(value));
        int rs1 = nrs.deserializeInt(conn.getByte(bk));
        return rs1;
    }
    
    private int getCounter() {
        byte[] bk = nrs.serializeString(keyCounter);
        int rs1 = nrs.deserializeInt(conn.getByte(bk));
        return rs1;
    }
    
    private long getCounter2() {
        byte[] bk = nrs.serializeString(keyCounter);
        long rs1 = nrs.deserializeLong(conn.getByte(bk));
        return rs1;
    }
}
