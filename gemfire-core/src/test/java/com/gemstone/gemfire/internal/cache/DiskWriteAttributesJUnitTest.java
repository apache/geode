/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.DiskWriteAttributes;
import com.gemstone.gemfire.cache.DiskWriteAttributesFactory;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

/**
 * Tests if DiskWriteAttributeFactory returns the correct DWA object with the
 * desired values
 * 
 * @author Mitul Bid
 *  
 */
@Category(UnitTest.class)
public class DiskWriteAttributesJUnitTest extends TestCase
{

  public DiskWriteAttributesJUnitTest(String arg0) {
    super(arg0);
  }

  protected void setUp() throws Exception
  {
    super.setUp();
  }

  protected void tearDown() throws Exception
  {
    super.tearDown();
  }

  /*
   * Test method for
   * 'com.gemstone.gemfire.cache.DiskWriteAttributes.getDefaultInstance()'
   */
  public void testGetDefaultInstance()
  {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    DiskWriteAttributes dwa = dwaf.create();
    Assert.assertTrue(!dwa.isSynchronous());
    Assert.assertTrue(dwa.isRollOplogs());
  }

  /*
   * Test method for
   * 'com.gemstone.gemfire.cache.DiskWriteAttributes.getDefaultSync()'
   */
  public void testGetDefaultSync()
  {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    dwaf.setSynchronous(true);
    DiskWriteAttributes dwa = dwaf.create();
    Assert.assertTrue(dwa.isSynchronous());
    Assert.assertTrue(dwa.isRollOplogs());
  }

  /*
   * Test method for
   * 'com.gemstone.gemfire.cache.DiskWriteAttributes.getDefaultAsync()'
   */
  public void testGetDefaultAsync()
  {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    DiskWriteAttributes dwa = dwaf.create();
    Assert.assertTrue(!dwa.isSynchronous());
    Assert.assertTrue(dwa.isRollOplogs());
  }

  /*
   * Test method for
   * 'com.gemstone.gemfire.cache.DiskWriteAttributes.getDefaultRollingSync()'
   */
  public void testGetDefaultRollingSync()
  {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    dwaf.setSynchronous(true);
    DiskWriteAttributes dwa = dwaf.create();

    Assert.assertTrue(dwa.isSynchronous());
    Assert.assertTrue(dwa.isRollOplogs());
  }

  /*
   * Test method for
   * 'com.gemstone.gemfire.cache.DiskWriteAttributes.getDefaultRollingAsync()'
   */
  public void testGetDefaultRollingAsync()
  {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    DiskWriteAttributes dwa = dwaf.create();
    Assert.assertTrue(!dwa.isSynchronous());
    Assert.assertTrue(dwa.isRollOplogs());

  }

  /*
   * Test method for
   * 'com.gemstone.gemfire.cache.DiskWriteAttributes.getDefaultNonRollingSync()'
   */
  public void testGetDefaultNonRollingSync()
  {

    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    dwaf.setRollOplogs(false);
    dwaf.setSynchronous(true);
    DiskWriteAttributes dwa = dwaf.create();
    Assert.assertTrue(dwa.isSynchronous());
    Assert.assertTrue(!dwa.isRollOplogs());

  }

  /*
   * Test method for
   * 'com.gemstone.gemfire.cache.DiskWriteAttributes.getDefaultNonRollingAsync()'
   */
  public void testGetDefaultNonRollingAsync()
  {
    DiskWriteAttributesFactory dwaf = new DiskWriteAttributesFactory();
    dwaf.setRollOplogs(false);
    DiskWriteAttributes dwa = dwaf.create();
    Assert.assertTrue(!dwa.isSynchronous());
    Assert.assertTrue(!dwa.isRollOplogs());
  }
  
  /**
   * Tests the behaviour of DiskWriteAttributesFactory & DiskWritesAttrbutes with
   * various combinations of  time interval & buffer size.
   * @author Asif   
   */
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