/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.redis.internal;

import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * 
 * Test cases for the Redis lock service
 *
 */
@Category(UnitTest.class)
public class RedisLockServiceTest {
  private static boolean testLockBool = false;
  private static boolean testUnlock = false;

  /**
   * Test lock method
   * 
   * @throws Exception when an unknown error occurs
   */
  @Test
  public void testLock() throws Exception {
    RedisLockService lockService = new RedisLockService(1000);

    // test null handling
    Assert.assertFalse(lockService.lock(null));

    Object key1 = "010";

    // test locks across threads
    Thread t1 = new Thread(() -> {

      try {
        lockService.lock(key1);

        while (true) {
          if (RedisLockServiceTest.testLockBool) {
            lockService.unlock(key1);
            break;
          }

          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
          }
        }
      } catch (Exception e) {
      }
    });

    // start thread with locking
    t1.start();
    Thread.sleep(5);


    // test current thread cannot lock y key
    Assert.assertFalse(lockService.lock(key1));

    // set flag for thread to unlock key
    RedisLockServiceTest.testLockBool = true;
    Thread.sleep(1000);

    // assert true you can now lock the service
    Assert.assertTrue(lockService.lock(key1));


    Object key2 = new Integer(123);
    Assert.assertTrue(lockService.lock(key2));

    // check weak reference support
    key2 = null;
    System.gc();

    // lock should be removed when not references to key
    Assert.assertNull(lockService.getLock(Integer.valueOf(123)));

    // check that thread 1 has stopped
    t1.join();

  }

  /**
   * Test unlock method
   */
  @Test
  public void testUnlock() throws Exception {
    RedisLockService lockService1 = new RedisLockService();
    RedisLockService lockService2 = new RedisLockService();

    // check null handling
    lockService1.unlock(null);

    Object key = new Integer(2);
    // test locks across threads
    Thread t1 = new Thread(() -> {

      try {
        lockService1.lock(Integer.valueOf(2));

        while (true) {
          if (RedisLockServiceTest.testUnlock) {
            lockService1.unlock(Integer.valueOf(2));
            break;
          }

          try {
            Thread.sleep(5);
          } catch (InterruptedException e) {
          }
        }
      } catch (Exception e) {
      }
    });

    // start thread with locking
    t1.start();
    Thread.sleep(5);

    Assert.assertFalse(lockService1.lock(key));

    // test locks across services different
    Assert.assertTrue(lockService2.lock(key));
    lockService2.unlock(key);

    Assert.assertFalse(lockService1.lock(key));

    // set flag for thread to unlock
    RedisLockServiceTest.testUnlock = true;
    Thread.sleep(20);

    Assert.assertTrue(lockService1.lock(key));
    t1.join();
  }

  @Test
  public void testGetLock() {

    Object obj = Integer.valueOf(2);
    RedisLockService lockService = new RedisLockService();

    Assert.assertNull(lockService.getLock(null));
    Assert.assertNull(lockService.getLock(obj));
    lockService.lock(obj);
    Assert.assertNotNull(lockService.getLock(obj));
    lockService.unlock(Integer.valueOf(2));

    // check lock removed
    Assert.assertNull(lockService.getLock(obj));
  }

}
