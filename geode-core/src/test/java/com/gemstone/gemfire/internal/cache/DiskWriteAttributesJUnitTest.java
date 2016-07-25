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
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.DiskWriteAttributes;
import com.gemstone.gemfire.cache.DiskWriteAttributesFactory;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Tests if DiskWriteAttributeFactory returns the correct DWA object with the
 * desired values
 */
@Category(UnitTest.class)
public class DiskWriteAttributesJUnitTest {

  /**
   * Test method for
   * 'com.gemstone.gemfire.cache.DiskWriteAttributes.getDefaultInstance()'
   */
  @Test
  public void testGetDefaultInstance() {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    DiskWriteAttributes dwa = dwaf.create();
    assertTrue(!dwa.isSynchronous());
    assertTrue(dwa.isRollOplogs());
  }

  /**
   * Test method for
   * 'com.gemstone.gemfire.cache.DiskWriteAttributes.getDefaultSync()'
   */
  @Test
  public void testGetDefaultSync() {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    dwaf.setSynchronous(true);
    DiskWriteAttributes dwa = dwaf.create();
    assertTrue(dwa.isSynchronous());
    assertTrue(dwa.isRollOplogs());
  }

  /**
   * Test method for
   * 'com.gemstone.gemfire.cache.DiskWriteAttributes.getDefaultAsync()'
   */
  @Test
  public void testGetDefaultAsync() {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    DiskWriteAttributes dwa = dwaf.create();
    assertTrue(!dwa.isSynchronous());
    assertTrue(dwa.isRollOplogs());
  }

  /**
   * Test method for
   * 'com.gemstone.gemfire.cache.DiskWriteAttributes.getDefaultRollingSync()'
   */
  @Test
  public void testGetDefaultRollingSync() {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    dwaf.setSynchronous(true);
    DiskWriteAttributes dwa = dwaf.create();

    assertTrue(dwa.isSynchronous());
    assertTrue(dwa.isRollOplogs());
  }

  /**
   * Test method for
   * 'com.gemstone.gemfire.cache.DiskWriteAttributes.getDefaultRollingAsync()'
   */
  @Test
  public void testGetDefaultRollingAsync() {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    DiskWriteAttributes dwa = dwaf.create();
    assertTrue(!dwa.isSynchronous());
    assertTrue(dwa.isRollOplogs());
  }

  /**
   * Test method for
   * 'com.gemstone.gemfire.cache.DiskWriteAttributes.getDefaultNonRollingSync()'
   */
  @Test
  public void testGetDefaultNonRollingSync() {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    dwaf.setRollOplogs(false);
    dwaf.setSynchronous(true);
    DiskWriteAttributes dwa = dwaf.create();
    assertTrue(dwa.isSynchronous());
    assertTrue(!dwa.isRollOplogs());
  }

  /**
   * Test method for
   * 'com.gemstone.gemfire.cache.DiskWriteAttributes.getDefaultNonRollingAsync()'
   */
  @Test
  public void testGetDefaultNonRollingAsync() {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    dwaf.setRollOplogs(false);
    DiskWriteAttributes dwa = dwaf.create();
    assertTrue(!dwa.isSynchronous());
    assertTrue(!dwa.isRollOplogs());
  }
  
  /**
   * Tests the behaviour of DiskWriteAttributesFactory & DiskWritesAttrbutes with
   * various combinations of  time interval & buffer size.
   */
  @Test
  public void testDiskWriteAttributesCreation() {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    dwaf.setSynchronous(true);
    DiskWriteAttributes dwa = dwaf.create();
    assertEquals(dwa.getBytesThreshold(),0);
    assertEquals(dwa.getTimeInterval(),0);
    
    dwaf.setSynchronous(false);
    dwa = dwaf.create();
    assertEquals(dwa.getBytesThreshold(),0);
    assertEquals(dwa.getTimeInterval(),DiskWriteAttributesImpl.DEFAULT_TIME_INTERVAL);
    
    dwaf.setBytesThreshold(0);
    dwa = dwaf.create();
    assertEquals(dwa.getBytesThreshold(),0);
    assertEquals(dwa.getTimeInterval(),DiskWriteAttributesImpl.DEFAULT_TIME_INTERVAL);
    
    dwaf.setBytesThreshold(1);
    dwa = dwaf.create();
    assertEquals(dwa.getBytesThreshold(),1);
    assertEquals(dwa.getTimeInterval(),0);
    
    dwaf.setBytesThreshold(0);
    dwaf.setTimeInterval(0);
    dwa = dwaf.create();
    assertEquals(dwa.getBytesThreshold(),0);
    assertEquals(dwa.getTimeInterval(),0);
    
    DiskWriteAttributesFactory dwaf1 = new DiskWriteAttributesFactory();    
    DiskWriteAttributes dwa1 = dwaf1.create();
    assertEquals(dwa1.getBytesThreshold(),0);
    assertEquals(dwa1.getTimeInterval(),DiskWriteAttributesImpl.DEFAULT_TIME_INTERVAL);
    
    DiskWriteAttributesFactory dwaf2 = new DiskWriteAttributesFactory(dwa1);
    DiskWriteAttributes dwa2 = dwaf2.create();
    assertEquals(dwa2.getBytesThreshold(),0);
    assertEquals(dwa2.getTimeInterval(),DiskWriteAttributesImpl.DEFAULT_TIME_INTERVAL);
    
    dwaf1 = new DiskWriteAttributesFactory();
    dwaf1.setBytesThreshold(100);
    dwaf2 = new DiskWriteAttributesFactory(dwaf1.create());
    dwa2 = dwaf2.create();
    assertEquals(dwa2.getBytesThreshold(),100);
    assertEquals(dwa2.getTimeInterval(),0);
    
    dwaf1 = new DiskWriteAttributesFactory();
    dwaf1.setBytesThreshold(0);
    dwaf1.setTimeInterval(0);
    dwaf2 = new DiskWriteAttributesFactory(dwaf1.create());
    dwa2 = dwaf2.create();
    assertEquals(dwa2.getBytesThreshold(),0);
    assertEquals(dwa2.getTimeInterval(),0);
    
    
    dwaf1 = new DiskWriteAttributesFactory();
    dwa1 = dwaf1.create();
    dwaf2 = new DiskWriteAttributesFactory(dwa1);
    dwa2 = dwaf2.create();
    assertEquals(dwa2.getBytesThreshold(),0);
    assertEquals(dwa2.getTimeInterval(),DiskWriteAttributesImpl.DEFAULT_TIME_INTERVAL);
    assertEquals(dwa1.getBytesThreshold(),0);
    assertEquals(dwa1.getTimeInterval(),DiskWriteAttributesImpl.DEFAULT_TIME_INTERVAL);
    //Important :Notice the behaviour difference in the time nterval setting
    dwaf1.setBytesThreshold(1);
    dwaf2.setBytesThreshold(1);
    dwa1 = dwaf1.create();
    dwa2 = dwaf2.create();
    assertEquals(dwa2.getBytesThreshold(),1);
    assertEquals(dwa2.getTimeInterval(),DiskWriteAttributesImpl.DEFAULT_TIME_INTERVAL);
    assertEquals(dwa1.getBytesThreshold(),1);
    assertEquals(dwa1.getTimeInterval(),0);     
  }  
}