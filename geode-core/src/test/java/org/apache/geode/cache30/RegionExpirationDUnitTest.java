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
package org.apache.geode.cache30;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;

/**
 * Test Region expiration - both time-to-live and idle timeout.
 *
 * Note:  See LocalRegionTest and MultiVMRegionTestCase for more expiration
 * tests.
 *
 *
 * @since GemFire 3.0
 */
@Category(DistributedTest.class)
public class RegionExpirationDUnitTest extends JUnit4CacheTestCase {

  public RegionExpirationDUnitTest() {
    super();
  }

//  /**
//   * Returns the attributes of a region to be tested.
//   */
//  private RegionAttributes getRegionAttributes() {
//    AttributesFactory factory = new AttributesFactory();
//    factory.setScope(Scope.LOCAL);
//    factory.setStatisticsEnabled(true);
//    return factory.create();
//  }

  /**
   * Test internal methods that encode & decode time
   */
  /* The encode and decode time methods are now private in MetaMap
  @Test
  public void testTimeEncoding() throws CacheException {
    Region r = createRegion(getUniqueName(), getRegionAttributes());
    long start = ((InternalDistributedSystem)getCache().getDistributedSystem()).getStartTime();
    long timeMs  = System.currentTimeMillis();
    assertTrue(timeMs >= start);
    int encoded = ((LocalRegion)r).encodeTime(timeMs);
    long decoded = ((LocalRegion)r).decodeTime(encoded);
    getLogWriter().info(
      "testTimeEncoding: timeMs: " + timeMs +
      ", ds start: " + start +
      ", encoded: " + encoded +
      ", decoded: " + decoded);
    assertTrue(Math.abs(timeMs - decoded) <= 100);
  }
   */

  @Test
  public void testRegionTTLLocalDestroy()
  throws CacheException, InterruptedException
  {
    _testRegionTTL(getUniqueName(), ExpirationAction.LOCAL_DESTROY);
  }

  @Test
  public void testRegionTTLDestroy()
  throws CacheException, InterruptedException
  {
    _testRegionTTL(getUniqueName(), ExpirationAction.DESTROY);
  }

  @Test
  public void testRegionTTLLocalInvalidate()
  throws CacheException, InterruptedException
  {
    _testRegionTTL(getUniqueName(), ExpirationAction.LOCAL_INVALIDATE);
  }

  @Test
  public void testRegionTTLInvalidate()
  throws CacheException, InterruptedException
  {
    _testRegionTTL(getUniqueName(), ExpirationAction.INVALIDATE);
  }

  @Test
  public void testRegionTTLAfterMutating()
  throws InterruptedException, CacheException
  {
    String regionName = getUniqueName();
    int firstTimeout = 2;
    int secondTimeout = 6;
    createWithExpiration(
      regionName,
      new ExpirationAttributes(firstTimeout, ExpirationAction.DESTROY),
      null);
    long startTime = System.currentTimeMillis();
    final Region region = getOrCreateRootRegion().getSubregion(regionName);
    region.getAttributesMutator().setRegionTimeToLive(
      new ExpirationAttributes(secondTimeout, ExpirationAction.DESTROY));
    Thread.sleep(firstTimeout * 1000 + 100);
    if (region.isDestroyed()) {
      assertTrue(System.currentTimeMillis() >= startTime + secondTimeout * 1000);
    }
    
    Thread.sleep((secondTimeout - firstTimeout) * 1000 + 100);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return region.isDestroyed();
      }
      public String description() {
        return "region never destroyed";
      }
    };
    Wait.waitForCriterion(wc, 30 * 1000, 1000, true);
  }

  @Test
  public void testWhenBothTtlAndIdleAreSet() 
  throws InterruptedException, CacheException
  {
    String regionName = getUniqueName();
    int ttlTimeout = 4;
    int idleTimeout = 3;
    createWithExpiration(
      regionName,
      new ExpirationAttributes(ttlTimeout, ExpirationAction.DESTROY),
      new ExpirationAttributes(idleTimeout, ExpirationAction.INVALIDATE));
    Region region = getOrCreateRootRegion().getSubregion(regionName);
    region.create("key", "val");
    Thread.sleep(idleTimeout * 1000 + 200);
    assertFalse(region.isDestroyed());

    // Touch the lastAccessedTime
    assertNull(region.get("key"));
    
    // Next action that occurs should be ttl - the destroy
    Thread.sleep(((ttlTimeout - idleTimeout) * 1000) + 5000);
    assertTrue(region.isDestroyed());
  }


  /////////////// Utility methods ///////////////////////////

  private void _testRegionTTL(final String regionName, final ExpirationAction action)
  throws InterruptedException
  {
    final int timeoutSecs = 10;
    final Object key = "key";
    final Object originalValue = "original value";
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    LogWriterUtils.getLogWriter().info("vm0 is " + vm0.getPid() + ", vm1 is " + vm1);

    LogWriterUtils.getLogWriter().info("2: " + regionName + " action is " + action);

    final long tilt = System.currentTimeMillis() + timeoutSecs * 1000;

    // In vm0, create the region with ttl, and put value
    vm0.invoke(new CacheSerializableRunnable("Create Region & Key") {
        public void run2() throws CacheException {
          createWithExpiration(
            regionName,
            new ExpirationAttributes(timeoutSecs, action),
            null);
          Region region = getOrCreateRootRegion().getSubregion(regionName);
          region.put(key, originalValue);
          assertEquals(originalValue, region.get(key));
          assertTrue(region.containsValueForKey(key));
        }
      });
    // In vm1, create the region - no ttl
    vm1.invoke(new CacheSerializableRunnable("Create Region & Key") {
        public void run2() throws CacheException {
          createWithExpiration(
            regionName,
            null,
            null);
        }
      });

    // In vm1, do get() to cause netsearch
    vm1.invoke(new CacheSerializableRunnable("Get") {
        public void run2() throws CacheException {
          Region region = getOrCreateRootRegion().getSubregion(regionName);
          Object newVal = region.get(key);

          if (!originalValue.equals(newVal)) {
            assertTrue("expected original value but got " + newVal,
                System.currentTimeMillis() + 1000 >= tilt);
          }
          if (!region.containsValueForKey(key)) {
            assertTrue("Region doesn't hold key",
                System.currentTimeMillis() + 1000 >= tilt);
          }
        }
      });

    // Wait for expiration to occur
    Thread.sleep(timeoutSecs * 1000 + 2000);

    // In vm0, region should be absent (for destroy, localDestroy), or
    // entry invalid
    vm0.invoke(new CacheSerializableRunnable("Get") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(regionName);
          LogWriterUtils.getLogWriter().info("3: " + regionName + ", " + region + ", action is " + action);
          if (action.isInvalidate() || action.isLocalInvalidate()) {
            assertTrue(!region.containsValueForKey(key));
          } else {
            assertTrue(region == null);
          }
        }
      });
    // In vm1, region should be absent (for destroy), or entry invalid (invalidate).
    // For LOCAL_DESTROY or LOCAL_INVALIDATE, the value should be intact
    vm1.invoke(new CacheSerializableRunnable("Get") {
        public void run2() throws CacheException {
          Region region = getRootRegion().getSubregion(regionName);
          if (action.isInvalidate()) {
            assertTrue(region.containsKey(key));
            assertTrue(!region.containsValueForKey(key));
          } else if (action.isDestroy()) {
            assertTrue(region == null);
          } else {
            assertTrue(region.containsValueForKey(key));
            assertEquals(originalValue, region.get(key));
          }
        }
      });
  }

  protected void createWithExpiration(String regionName, ExpirationAttributes ttl,
                                    ExpirationAttributes idle)
  throws CacheException
  {
    AttributesFactory factory = new AttributesFactory();
    factory.setStatisticsEnabled(true);
    if (ttl != null) factory.setRegionTimeToLive(ttl);
    if (idle != null) factory.setRegionIdleTimeout(idle);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setEarlyAck(false);
    RegionAttributes attrs = factory.create();
    LogWriterUtils.getLogWriter().info("4: " + regionName + " ttl action is " + ttl);
    getOrCreateRootRegion().createSubregion(regionName, attrs);
  }

  protected Region getOrCreateRootRegion()
  throws CacheException
  {
    Region root = getRootRegion();
    if (root == null) {
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setEarlyAck(false);
      factory.setStatisticsEnabled(true);
      root = createRootRegion(factory.create());
    }
    return root;
  }

}
