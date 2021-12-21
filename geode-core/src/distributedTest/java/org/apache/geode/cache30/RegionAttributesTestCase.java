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
package org.apache.geode.cache30;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;

/**
 * An abstract class whose test methods test the functionality of a {@link RegionAttributes},
 * {@link AttributesFactory}, and {@link AttributesMutator}.
 *
 *
 * @since GemFire 3.0
 */
public abstract class RegionAttributesTestCase extends RegionTestCase {

  public RegionAttributesTestCase() {
    super();
  }

  protected static class TestExpiry implements CustomExpiry, Declarable {
    final Exception created = new Exception();

    public String toString() {
      final StringBuffer sb = new StringBuffer();
      sb.append("CustomExpiry from: <");
      OutputStream os = new OutputStream() {

        @Override
        public void write(int b) throws IOException {
          sb.append((char) b);
        }
      };
      PrintStream ps = new PrintStream(os);
      created.printStackTrace(ps);
      sb.append(">");
      return sb.toString();
    }

    @Override
    public ExpirationAttributes getExpiry(Entry entry) {
      return null;
    }

    @Override
    public void init(Properties props) {}

    @Override
    public void close() {}
  }
  // ///////////////////// Test Methods ///////////////////////

  /**
   * Tests that an {@link AttributesMutator} actually changes the region's attributes. Also tests
   * the return values of the mutator methods.
   */
  @Test
  public void testAttributesMutator() throws CacheException {
    String name = getUniqueName();
    AttributesFactory fac = new AttributesFactory(getRegionAttributes());
    fac.setStatisticsEnabled(true);
    Region region = createRegion(name, fac.create());

    CacheListener listener = new TestCacheListener() {};
    CacheLoader loader = new TestCacheLoader() {
      @Override
      public Object load2(LoaderHelper helper) {
        fail("Why was I invoked?");
        return null;
      }
    };
    CacheWriter writer = new TestCacheWriter() {};
    CustomExpiry customEntryIdle = new TestExpiry();
    CustomExpiry customTtl = new TestExpiry();
    ExpirationAttributes entryIdle = new ExpirationAttributes(5, ExpirationAction.DESTROY);
    ExpirationAttributes entryTTL = new ExpirationAttributes(6, ExpirationAction.INVALIDATE);
    ExpirationAttributes regionIdle = new ExpirationAttributes(7, ExpirationAction.DESTROY);
    ExpirationAttributes regionTTL = new ExpirationAttributes(8, ExpirationAction.INVALIDATE);

    AttributesMutator mutator = region.getAttributesMutator();
    assertEquals(region, mutator.getRegion());
    assertSame(region, mutator);

    mutator.addCacheListener(listener);
    mutator.setCacheLoader(loader);
    mutator.setCacheWriter(writer);
    mutator.setEntryIdleTimeout(entryIdle);
    mutator.setCustomEntryIdleTimeout(customEntryIdle);
    mutator.setEntryTimeToLive(entryTTL);
    mutator.setCustomEntryTimeToLive(customTtl);
    mutator.setRegionIdleTimeout(regionIdle);
    mutator.setRegionTimeToLive(regionTTL);

    RegionAttributes attrs = region.getAttributes();
    assertSame(region, attrs);

    assertEquals(listener, attrs.getCacheListener());
    assertEquals(loader, attrs.getCacheLoader());
    assertEquals(writer, attrs.getCacheWriter());
    assertEquals(entryIdle, attrs.getEntryIdleTimeout());
    assertEquals(entryTTL, attrs.getEntryTimeToLive());
    assertEquals(customEntryIdle, attrs.getCustomEntryIdleTimeout());
    assertEquals(customTtl, attrs.getCustomEntryTimeToLive());

    CacheListener listener2 = new TestCacheListener() {};
    CacheLoader loader2 = new TestCacheLoader() {
      @Override
      public Object load2(LoaderHelper helper) {
        fail("Why was I invoked?");
        return null;
      }
    };
    CacheWriter writer2 = new TestCacheWriter() {};
    CustomExpiry customEntryIdle2 = new TestExpiry();
    CustomExpiry customTtl2 = new TestExpiry();
    ExpirationAttributes entryIdle2 = new ExpirationAttributes(5, ExpirationAction.DESTROY);
    ExpirationAttributes entryTTL2 = new ExpirationAttributes(6, ExpirationAction.INVALIDATE);
    ExpirationAttributes regionIdle2 = new ExpirationAttributes(7, ExpirationAction.DESTROY);
    ExpirationAttributes regionTTL2 = new ExpirationAttributes(8, ExpirationAction.INVALIDATE);

    mutator.initCacheListeners(new CacheListener[] {listener2});
    assertEquals(listener2, attrs.getCacheListener());
    assertEquals(loader, mutator.setCacheLoader(loader2));
    assertEquals(writer, mutator.setCacheWriter(writer2));
    assertEquals(entryIdle, mutator.setEntryIdleTimeout(entryIdle2));
    assertEquals(customEntryIdle, mutator.setCustomEntryIdleTimeout(customEntryIdle2));
    assertEquals(entryTTL, mutator.setEntryTimeToLive(entryTTL2));
    assertEquals(customTtl, mutator.setCustomEntryTimeToLive(customTtl2));
    assertEquals(regionIdle, mutator.setRegionIdleTimeout(regionIdle2));
    assertEquals(regionTTL, mutator.setRegionTimeToLive(regionTTL2));
  }

  /**
   * Tests sending <code>null</code> or bogus values to an {@link AttributesMutator}.
   */
  @Test
  public void testAttributesMutatorBogus() throws CacheException {
    String name = getUniqueName();
    Region region = createRegion(name);
    AttributesMutator mutator = region.getAttributesMutator();

    try {
      mutator.setEntryIdleTimeout(null);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    try {
      mutator.setEntryTimeToLive(null);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    try {
      mutator.setRegionIdleTimeout(null);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    try {
      mutator.setRegionTimeToLive(null);
      fail("Should have thrown an IllegalArgumentException");

    } catch (IllegalArgumentException ex) {
      // pass...
    }

    // Exception if stats not enabled
    try {
      mutator.setEntryIdleTimeout(new ExpirationAttributes(1, ExpirationAction.DESTROY));
      fail("Should have thrown an IllegalStateException");
    } catch (IllegalStateException ex) {
      // pass...
    }

    // Exception if stats not enabled
    try {
      mutator.setEntryTimeToLive(new ExpirationAttributes(1, ExpirationAction.DESTROY));
      fail("Should have thrown an IllegalStateException");
    } catch (IllegalStateException ex) {
      // pass
    }

    // Exception if stats not enabled
    try {
      mutator.setRegionIdleTimeout(new ExpirationAttributes(1, ExpirationAction.DESTROY));
      fail("Should have thrown an IllegalStateException");
    } catch (IllegalStateException ex) {
      // pass...
    }

    // Exception if stats not enabled
    try {
      mutator.setRegionTimeToLive(new ExpirationAttributes(1, ExpirationAction.DESTROY));
      fail("Should have thrown an IllegalStateException");

    } catch (IllegalStateException ex) {
      // pass

    }
  }

  /** Test to make sure region attributes take */
  @Test
  public void testRegionAttributes() throws CacheException {
    // @todo for now just test concurrencyLevel, add tests for the rest
    AttributesFactory factory = new AttributesFactory();
    factory.setConcurrencyLevel(60);
    factory.setConcurrencyChecksEnabled(true);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    RegionAttributes attrs = factory.create();
    String name = getUniqueName();
    Region region = createRegion(name, attrs);
    assertEquals(60, region.getAttributes().getConcurrencyLevel());
    assertTrue("expected concurrencyChecksEnabled to be true",
        region.getAttributes().getConcurrencyChecksEnabled());
  }

  // public void testCCEWithDNoAck() throws CacheException {
  // AttributesFactory factory = new AttributesFactory();
  // factory.setConcurrencyChecksEnabled(true);
  // factory.setScope(Scope.DISTRIBUTED_NO_ACK);
  // boolean caught = false;
  // try {
  // factory.create();
  // } catch (IllegalStateException expected) {
  // caught = true;
  // }
  // assertTrue("expected an IllegalStateException", caught);
  // }



}
