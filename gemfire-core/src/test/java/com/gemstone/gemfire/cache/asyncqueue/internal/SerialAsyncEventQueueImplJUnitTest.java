package com.gemstone.gemfire.cache.asyncqueue.internal;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderAttributes;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class SerialAsyncEventQueueImplJUnitTest {

  private Cache cache;
  @Before
  public void setUp() {
    CacheFactory cf = new CacheFactory().set("mcast-port", "0");
    cache = cf.create();
  }
  
  @After
  public void tearDown() {
    cache.close();
  }
  @Test
  public void testStopClearsStats() {
    GatewaySenderAttributes attrs = new GatewaySenderAttributes();
    attrs.id = AsyncEventQueueImpl.ASYNC_EVENT_QUEUE_PREFIX + "id";
    SerialAsyncEventQueueImpl queue = new SerialAsyncEventQueueImpl(cache, attrs);
    queue.getStatistics().incQueueSize(5);
    queue.getStatistics().incTempQueueSize(10);
    
    assertEquals(5, queue.getStatistics().getEventQueueSize());
    assertEquals(10, queue.getStatistics().getTempEventQueueSize());
   
    queue.stop();
    
    assertEquals(0, queue.getStatistics().getEventQueueSize());
    assertEquals(0, queue.getStatistics().getTempEventQueueSize());
  }

}
