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
package org.apache.geode.admin.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.admin.GemFireHealth;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.GemFireCacheImpl;

/**
 * Contains simple tests for the {@link CacheHealthEvaluator}
 *
 *
 * @since GemFire 3.5
 */
@SuppressWarnings("deprecation")
public class CacheHealthEvaluatorJUnitTest extends HealthEvaluatorTestCase {

  @Rule
  public TestName testName = new TestName();

  /**
   * Tests that we are in {@link GemFireHealth#OKAY_HEALTH okay} health if cache loads take too
   * long.
   *
   * @see CacheHealthEvaluator#checkLoadTime
   */
  @Test
  public void testCheckLoadTime() throws CacheException {
    Cache cache = CacheFactory.create(system);
    CachePerfStats stats = ((GemFireCacheImpl) cache).getCachePerfStats();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setCacheLoader(new CacheLoader() {
      @Override
      public Object load(LoaderHelper helper) throws CacheLoaderException {

        return "Loaded";
      }

      @Override
      public void close() {}
    });

    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(getName(), attrs);

    GemFireHealthConfig config = new GemFireHealthConfigImpl(null);
    config.setMaxLoadTime(100);

    CacheHealthEvaluator eval =
        new CacheHealthEvaluator(config, system.getDistributionManager());
    for (int i = 0; i < 10; i++) {
      region.get("Test1 " + i);
    }
    long firstLoadTime = stats.getLoadTime();
    long firstLoadsCompleted = stats.getLoadsCompleted();
    assertTrue(firstLoadTime >= 0);
    assertTrue(firstLoadsCompleted > 0);

    // First time should always be empty
    List status = new ArrayList();
    eval.evaluate(status);
    assertEquals(0, status.size());

    config = new GemFireHealthConfigImpl(null);
    config.setMaxLoadTime(10);
    eval = new CacheHealthEvaluator(config, system.getDistributionManager());
    eval.evaluate(status);

    long start = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      region.get("Test2 " + i);
    }
    assertTrue(System.currentTimeMillis() - start < 1000);
    long secondLoadTime = stats.getLoadTime();
    long secondLoadsCompleted = stats.getLoadsCompleted();
    assertTrue("firstLoadTime=" + firstLoadTime + ", secondLoadTime=" + secondLoadTime,
        secondLoadTime >= firstLoadTime);
    assertTrue(secondLoadsCompleted > firstLoadsCompleted);

    // Averge should be less than 10 milliseconds
    status = new ArrayList();
    eval.evaluate(status);
    assertEquals(0, status.size());

    region.getAttributesMutator().setCacheLoader(new CacheLoader() {
      @Override
      public Object load(LoaderHelper helper) throws CacheLoaderException {

        try {
          Thread.sleep(20);

        } catch (InterruptedException ex) {
          fail("Why was I interrupted?");
        }
        return "Loaded";
      }

      @Override
      public void close() {}

    });

    for (int i = 0; i < 50; i++) {
      region.get("Test3 " + i);
    }

    long thirdLoadTime = stats.getLoadTime();
    long thirdLoadsCompleted = stats.getLoadsCompleted();
    assertTrue(thirdLoadTime > secondLoadTime);
    assertTrue(thirdLoadsCompleted > secondLoadsCompleted);

    status = new ArrayList();
    eval.evaluate(status);
    assertEquals(1, status.size());

    AbstractHealthEvaluator.HealthStatus ill = (AbstractHealthEvaluator.HealthStatus) status.get(0);
    assertEquals(GemFireHealth.OKAY_HEALTH, ill.getHealthCode());
    String s = "The average duration of a Cache load";
    assertTrue(ill.getDiagnosis().indexOf(s) != -1);
  }

  /**
   * Tests that we are in {@link GemFireHealth#OKAY_HEALTH okay} health if the hit ratio dips below
   * the threshold.
   */
  @Test
  public void testCheckHitRatio() throws CacheException {
    Cache cache = CacheFactory.create(system);
    // CachePerfStats stats = ((GemFireCache) cache).getCachePerfStats();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setCacheLoader(new CacheLoader() {
      @Override
      public Object load(LoaderHelper helper) throws CacheLoaderException {

        return "Loaded";
      }

      @Override
      public void close() {}
    });

    RegionAttributes attrs = factory.create();
    Region region = cache.createRegion(getName(), attrs);

    GemFireHealthConfig config = new GemFireHealthConfigImpl(null);
    config.setMinHitRatio(0.5);

    CacheHealthEvaluator eval =
        new CacheHealthEvaluator(config, system.getDistributionManager());
    List status = new ArrayList();
    eval.evaluate(status);
    assertEquals(0, status.size());

    region.get("One");
    region.get("One");
    region.get("One");

    status = new ArrayList();
    eval.evaluate(status);
    assertEquals(0, status.size());

    for (int i = 0; i < 50; i++) {
      region.get("Miss " + i);
    }

    status = new ArrayList();
    eval.evaluate(status);

    AbstractHealthEvaluator.HealthStatus ill = (AbstractHealthEvaluator.HealthStatus) status.get(0);
    assertEquals(GemFireHealth.OKAY_HEALTH, ill.getHealthCode());
    String s = "The hit ratio of this Cache";
    assertTrue(ill.getDiagnosis().indexOf(s) != -1);
  }

  private String getName() {
    return getClass().getSimpleName() + "_" + testName.getMethodName();
  }
}
