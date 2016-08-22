/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.pdx;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.internal.PdxInstanceFactoryImpl;
import com.gemstone.gemfire.pdx.internal.PdxInstanceImpl;
import com.gemstone.gemfire.pdx.internal.PdxString;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class PdxStringJUnitTest {

  private GemFireCacheImpl c;

  @Before
  public void setUp() {
    // make it a loner
    this.c = (GemFireCacheImpl) new CacheFactory()
        .set(MCAST_PORT, "0")
        .setPdxReadSerialized(true)
        .create();
  }

  @After
  public void tearDown() {
    this.c.close();
  }
  
  @Test
  public void testEquals() throws Exception{
    PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeString("secId", "abc");
    PdxInstanceImpl pi = (PdxInstanceImpl) pf.create();  
    PdxString pdx1 = (PdxString) pi.getRawField("secId");
    assertEquals(false, pdx1.equals(null));
    assertEquals(false, pdx1.equals(new Date(37)));
    
    PdxString pdx2 = new PdxString("abc");
    assertEquals(pdx1,pdx2);
    
    pdx2 = new PdxString("ABC");
    assertEquals(false, pdx1.equals(pdx2));
  
  }
  @Test
  public void testHashCodeEquals() throws Exception{
    PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeString("secId", "abc");
    PdxInstanceImpl pi = (PdxInstanceImpl) pf.create();  
    PdxString pdx1 = (PdxString) pi.getRawField("secId");
    Map<PdxString,String> map = new HashMap<PdxString,String>();
    map.put(pdx1,"abc");
    
    PdxString pdx2 = new PdxString("abc");
    assertEquals(map.get(pdx2),"abc");
    
    map = new Object2ObjectOpenHashMap();
    map.put(pdx1,"abc");
    assertEquals(map.get(pdx2),"abc");

  }
  
  @Test
  public void testCompareTo() throws Exception{
    PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeString("secId", "abc");
    PdxInstanceImpl pi = (PdxInstanceImpl) pf.create();  
    PdxString pdx1 = (PdxString) pi.getRawField("secId");

    PdxString pdx2 = new PdxString("abc");
    assertEquals(pdx1.compareTo(pdx2),0);
    
    pdx2 = new PdxString("ABC");
    assertEquals(pdx1.compareTo(pdx2),32); // a - A = 32
    
    String str1 = new String("A" + "\u00e9" + "\u00f1" );
    String str2 = new String("A" + "\u00ea" + "\u00f1" );
    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeString("secId", str1);
    pi = (PdxInstanceImpl) pf.create();  
    pdx1 = (PdxString) pi.getRawField("secId");
    pdx2 = new PdxString(str2);
    assertEquals(-1, pdx1.compareTo(pdx2)); // str1 < str2

    //test compareTo for a huge string and small string
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 200000; i++) {
      sb.append("a");
    }
    str1 = sb.toString();
    str2 = "aaa";
    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeString("secId", str1);
    pi = (PdxInstanceImpl) pf.create();  
    pdx1 = (PdxString) pi.getRawField("secId");
    pdx2 = new PdxString(str2);
    assertTrue(pdx1.compareTo(pdx2) > 0); // str1 > str2 so positive result
    sb = null;
    str1 = null;

    //huge utf8 string and compareto
    sb = new StringBuilder();
    for (int i = 0; i < 65535; i++) {
      sb.append( "\u00e9"  );
    }
    str1 = sb.toString();
    str2 = "abc";
    pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeString("secId", str1);
    pi = (PdxInstanceImpl) pf.create();  
    pdx1 =  new PdxString(str1);
    pdx2 = new PdxString(str2);
    
    assertTrue(pdx1.compareTo(pdx2) > 0);  // str1 > str2 so positive result
    sb = null;
    str1 = null;
  }
  
  @Test
  public void testToString() throws Exception{
    String s = "abc";
    PdxString pdx = new PdxString(s);
    assertEquals(s, pdx.toString());
    
    PdxInstanceFactory pf = PdxInstanceFactoryImpl.newCreator("Portfolio", false);
    pf.writeString("secId", "abc");
    PdxInstanceImpl pi = (PdxInstanceImpl) pf.create();  
    pdx = (PdxString) pi.getRawField("secId");
    assertEquals(s, pdx.toString());
    
  }
 
  
  
}
