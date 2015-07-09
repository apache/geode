/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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

import junit.framework.TestCase;

@Category(IntegrationTest.class)
public class PdxStringJUnitTest {
  private GemFireCacheImpl c;

  @Before
  public void setUp() {
    // make it a loner
    this.c = (GemFireCacheImpl) new CacheFactory()
        .set("mcast-port", "0")
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
