/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.TestCase;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category({IntegrationTest.class, HoplogTest.class})
public class GFKeyJUnitTest extends TestCase {
  public void testSerde() throws Exception {
    String str = "str";
    GFKey key = new GFKey();
    key.setKey(str);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    key.write(dos);
    
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    key.readFields(dis);
    
    assertEquals(str, key.getKey());
  }
  
  public void testCompare() {
    GFKey keya = new GFKey();
    keya.setKey("a");
    
    GFKey keyb = new GFKey();
    keyb.setKey("b");
    
    assertEquals(-1, keya.compareTo(keyb));
    assertEquals(1, keyb.compareTo(keya));
  }
}
