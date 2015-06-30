/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.ha;

import java.io.IOException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.internal.logging.LogService;
/**
 * Test class for Blocking HA region queue functionalities
 *  
 * 
 * @author Suyog Bhokare
 * 
 */

//TODO:Asif: Modify the test to allow working with the new class containing 
//ReadWrite lock functionality
public class TestBlockingHARegionQueue extends HARegionQueue.TestOnlyHARegionQueue
{
  private static final Logger logger = LogService.getLogger();

  /**
   * Object on which to synchronize/wait
   */
  private Object forWaiting = new Object();

  boolean takeFirst = false;

  boolean takeWhenPeekInProgress = false;

  public TestBlockingHARegionQueue(String regionName, Cache cache)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    super(regionName, cache);
  }

  /**
   * Does a put and a notifyAll() multiple threads can possibly be waiting on
   * this queue to put
   * @throws CacheException
   * @throws InterruptedException
   * 
   * @throws InterruptedException
   */

  public void put(Object object) throws CacheException, InterruptedException
  {
    super.put(object);

    if (takeFirst) {
      this.take();
      this.takeFirst = false;
    }

    synchronized (forWaiting) {
      forWaiting.notifyAll();
    }
  }

  /**
   * blocking peek. This method will not return till it has acquired a
   * legitimate object from teh queue.
   * @throws InterruptedException 
   */

  public Object peek() throws  InterruptedException
  {
    Object object = null;
    while (true) {

      if (takeWhenPeekInProgress) {
        try{
        this.take();
        }catch (CacheException ce) {
          throw new RuntimeException(ce){};
        }
        this.takeWhenPeekInProgress = false;
      }
      object = super.peek();
      if (object == null) {
        synchronized (forWaiting) {
          object = super.peek();

          if (object == null) {
            boolean interrupted = Thread.interrupted();
            try {
              forWaiting.wait();
            }
            catch (InterruptedException e) {
              interrupted = true;
              /** ignore* */
              if (logger.isDebugEnabled()) {
                logger.debug("Interrupted exception while wait for peek", e);
              }
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          }
          else {
            break;
          }
        }
      }
      else {
        break;
      }
    }
    return object;
  }
}
