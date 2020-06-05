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

import org.junit.*;

/**
 *
 * @author nghiatc
 * @since Jun 5, 2020
 */
public class TestNRSerializer {
    private static NRSerializer nrs;
    
    @BeforeClass
    public static void init() {
        nrs = new NRSerializer();
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
    public void testSDBool() {
        boolean b1 = true;
        byte[] bb = nrs.serializeBool(b1);
        boolean b2 = nrs.deserializeBool(bb);
        Assert.assertEquals("testSDBool", b1, b2);
    }
    
    @Test
    public void testSDInt() {
        int i1 = 10;
        byte[] ii = nrs.serializeInt(i1);
        int i2 = nrs.deserializeInt(ii);
        Assert.assertEquals("testSDInt", i1, i2);
    }
    
    @Test
    public void testSDLong() {
        long l1 = 11;
        byte[] ll = nrs.serializeLong(l1);
        long l2 = nrs.deserializeLong(ll);
        Assert.assertEquals("testSDLong", l1, l2);
    }
    
    @Test
    public void testSDFloat() {
        float f1 = 12.0000001F;
        byte[] ff = nrs.serializeFloat(f1);
        float f2 = nrs.deserializeFloat(ff);
        Assert.assertEquals("testSDFloat", f1, f2, 0.00000001F);
    }
    
    @Test
    public void testSDDouble() {
        double d1 = 13.0000001D;
        byte[] dd = nrs.serializeDouble(d1);
        double d2 = nrs.deserializeDouble(dd);
        Assert.assertEquals("testSDDouble", d1, d2, 0.00000001D);
    }
    
    @Test
    public void testSDString() {
        String s1 = "nghiatc";
        byte[] ss = nrs.serializeString(s1);
        String s2 = nrs.deserializeString(ss);
        Assert.assertEquals("testSDString", s1, s2);
    }
}
