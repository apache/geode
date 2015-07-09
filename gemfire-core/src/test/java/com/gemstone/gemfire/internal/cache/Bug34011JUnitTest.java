/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * To verify the bug no. 34011 no longer exists:
 * Disk region perf test for Persist only with Async writes. 
 * 
 * The test verifies that puts per second perfomance if bytes threshold being exceeded before time interval is not more than a factor
 * of 2 (the reverse is also tested, time interval causing flush's perfomance should not be better than byte-threshold exceeding by
 * a factor of 2)
 * 
 * Note : This test can fail due to external factors such as filesystem becoming slow or CPU being overloaded
 * during one run and fast during the second run.
 *  
 * @author Vikram Jadhav 
 */
@Category(IntegrationTest.class)
public class Bug34011JUnitTest extends DiskRegionTestingBase
{
  String stats1 = null;
  String stats2 = null;
  DiskRegionProperties diskProps1 = new DiskRegionProperties();
  DiskRegionProperties diskProps2 = new DiskRegionProperties();
  Region region1= null;
  Region region2= null;
  public float opPerSec1= 0l;
  public float opPerSec2= 0l;
  
  @Before
  public void setUp() throws Exception
  {
    super.setUp();
    diskProps1.setDiskDirs(dirs);
    diskProps2.setDiskDirs(dirs);
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
  }

  @After
  public void tearDown() throws Exception
  {
    super.tearDown();
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
  }
 
  private static int ENTRY_SIZE = 2;

  private static int OP_COUNT = 100000; // 100000;

  
  /**
   * First, the time interval is set to a low value such that time-interval always elapses before bytes threshold is reached.
   * Then the bytes-threshold is set in such a way that byte threshold occurs before time-interval. The perfomance
   * of the first one should not be more than a factor of two as compared to the perfomance of the second scenario. The
   * reverse also hold true
   *
   */
 
  @Test
  public void testpersistASync()
  {

    // test-persistASync-ByteThreshold
    try {
     
      diskProps1.setTimeInterval(10);
      diskProps1.setBytesThreshold(Integer.MAX_VALUE); // now a queue size
      diskProps1.setRegionName("region1");
      region1 = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps1);
      
    }
    catch (Exception e) {
      if(logWriter.fineEnabled()){
        e.printStackTrace();
      }
      fail("failed : test-persistASync-ByteThreshold.Exception="+e);
    }
    //Perf test for 1kb writes
    populateData1();
    if(logWriter.infoEnabled()){
    logWriter.info("testpersistASyncByteThreshold:: Stats for 1 kb writes :"
        + stats1);
    }
   //  close region1
    region1.close();
 
  
    try {      
      diskProps2.setTimeInterval(150000000l);
      diskProps2.setBytesThreshold(32); // now a queue size
      diskProps2.setRegionName("region2");
      region2 = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache, diskProps2);
    }
    catch (Exception e) {
      if(logWriter.fineEnabled()) e.printStackTrace();
      fail("Failed : test-persistASync-TimeInterval. Exception = "+e);
    }
    //Perf test for 1kb writes
    populateData2();
    if(logWriter.infoEnabled()) logWriter.info("testpersistASyncTimeInterval:: Stats for 1 kb writes :"
        + stats2);
     //close region2
     region2.close();
    
     
   
     
   
    
    
    //validate that the pus/sec in both cases do not differ by twice 
     if(logWriter.infoEnabled()) logWriter.info("opPerSec1= "+opPerSec1+"_________opPerSec2= "+opPerSec2);
    assertTrue(opPerSec1/opPerSec2 < 3.0 );
    assertTrue(opPerSec2/opPerSec1 < 3.0) ;
        
  } //end of testpersistASyncTimeInterva
  
  public void populateData1 ()
  {
    //Put for validation.
    putForValidation(region1);
   
    final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte)77);
    //warm up the system
    for (int i = 0; i < OP_COUNT; i++) {
      region1.put("" + i, value);
     }
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < OP_COUNT; i++) {
      region1.put("" + i, value);
     }
    long endTime = System.currentTimeMillis();
    if(logWriter.fineEnabled()) logWriter.fine(" done with putting");
    float et = endTime - startTime;
    float etSecs = et / 1000f;
    opPerSec1 = etSecs == 0 ? 0 : (OP_COUNT / (et / 1000f));
    float bytesPerSec = etSecs == 0 ? 0
        : ((OP_COUNT * ENTRY_SIZE) / (et / 1000f));
    stats1 = "et=" + et + "ms writes/sec=" + opPerSec1 + " bytes/sec="
        + bytesPerSec;
    logWriter.info(stats1);
   //  validate put operation
    validatePut(region1);
    
  }
  
  public void populateData2()
  {
    //  Put for validation.
    putForValidation(region2);
    final byte[] value = new byte[ENTRY_SIZE];
    Arrays.fill(value, (byte)77);
    //warm up the system
    for (int i = 0; i < OP_COUNT; i++) {
      region2.put("" + i, value);
     }
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < OP_COUNT; i++) {
      region2.put("" + i, value);
     }
    long endTime = System.currentTimeMillis();
    if(logWriter.fineEnabled())  logWriter.fine(" done with putting");
    float et = endTime - startTime;
    float etSecs = et / 1000f;
    opPerSec2 = etSecs == 0 ? 0 : (OP_COUNT / (et / 1000f));
    float bytesPerSec = etSecs == 0 ? 0
        : ((OP_COUNT * ENTRY_SIZE) / (et / 1000f));
    stats2 = "et=" + et + "ms writes/sec=" + opPerSec2 + " bytes/sec="
        + bytesPerSec;
    logWriter.info(stats2);
   //  validate put operation
    validatePut(region2);
  }
  

  
}//end of the test

