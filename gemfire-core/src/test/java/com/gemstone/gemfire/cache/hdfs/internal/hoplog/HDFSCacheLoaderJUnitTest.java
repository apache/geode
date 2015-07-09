/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.util.List;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesMutator;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueStats;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributesFactory;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;

/**
 * Tests that entries loaded from a cache loader are inserted in the HDFS queue 
 * 
 * @author hemantb
 */
@Category({IntegrationTest.class, HoplogTest.class})
public class HDFSCacheLoaderJUnitTest extends BaseHoplogTestCase {

  private static int totalEventsReceived = 0;
  protected void configureHdfsStoreFactory() throws Exception {
    hsf = this.cache.createHDFSStoreFactory();
    hsf.setHomeDir(testDataDir.toString());
    hsf.setHDFSEventQueueAttributes(new HDFSEventQueueAttributesFactory().setBatchTimeInterval(100000000).setBatchSizeMB(10000).create());
  }

  /**
   * Tests that entries loaded from a cache loader are inserted in the HDFS queue 
   * but are not inserted in async queues. 
   * @throws Exception
   */
  public void testCacheLoaderForAsyncQAndHDFS() throws Exception {
    
    final AsyncEventQueueStats hdfsQueuestatistics = ((AsyncEventQueueImpl)cache.
        getAsyncEventQueues().toArray()[0]).getStatistics();
    
    AttributesMutator am = this.region.getAttributesMutator();
    am.setCacheLoader(new CacheLoader() {
      private int i = 0;
      public Object load(LoaderHelper helper)
      throws CacheLoaderException {
        return new Integer(i++);
      }
      
      public void close() { }
    });
    
    
    
    String asyncQueueName = "myQueue";
    new AsyncEventQueueFactoryImpl(cache).setBatchTimeInterval(1).
    create(asyncQueueName, new AsyncEventListener() {
      
      @Override
      public void close() {
        // TODO Auto-generated method stub
        
      }

      @Override
      public boolean processEvents(List events) {
        totalEventsReceived += events.size();
        return true;
      }
    });
    am.addAsyncEventQueueId(asyncQueueName);
    
    region.put(1, new Integer(100));
    region.destroy(1);
    region.get(1);
    region.destroy(1);
    
    assertTrue("HDFS queue should have received four events. But it received " + 
        hdfsQueuestatistics.getEventQueueSize(), 4 == hdfsQueuestatistics.getEventQueueSize());
    assertTrue("HDFS queue should have received four events. But it received " + 
        hdfsQueuestatistics.getEventsReceived(), 4 == hdfsQueuestatistics.getEventsReceived());
    
    region.get(1);
    Thread.sleep(2000);
    
    assertTrue("Async queue should have received only 5 events. But it received " + 
        totalEventsReceived, totalEventsReceived == 5);
    assertTrue("HDFS queue should have received 5 events. But it received " + 
        hdfsQueuestatistics.getEventQueueSize(), 5 == hdfsQueuestatistics.getEventQueueSize());
    assertTrue("HDFS queue should have received 5 events. But it received " + 
        hdfsQueuestatistics.getEventsReceived(), 5 == hdfsQueuestatistics.getEventsReceived());
    
    
  }
  
}
