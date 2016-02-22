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
