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
package org.apache.geode.internal.cache;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.util.Map.Entry;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;

@Category(DistributedTest.class)
public class Bug45164DUnitTest extends JUnit4CacheTestCase {
  private static final int count = 10000;
  private static final int stride = 3;
  
  public Bug45164DUnitTest() {
    super();
  }

  @Test
  public void testIterateWhileDestroy() throws Throwable {
    SerializableRunnable destroy = new SerializableRunnable() {
      @Override
      public void run() {
        Region<Integer, Object> region = getCache().getRegion("test");
        for (int j = 0; j < count / stride; j += stride) {
          region.destroy(j);
        }
      }
    };

    SerializableRunnable iterate = new SerializableRunnable() {
      @Override
      public void run() {
        Region<Integer, Object> region = getCache().getRegion("test");

        int i = 0;
        for (Entry<Integer, Object> entry : region.entrySet()) {
          i++;
          if (entry == null) {
            fail("Element " + i + " is null");
            
          }
        }
      }
    };
    
    Host h = Host.getHost(0);
    AsyncInvocation async1 = h.getVM(1).invokeAsync(destroy);
    AsyncInvocation async2 = h.getVM(2).invokeAsync(iterate);
    
    async1.getResult();
    async2.getResult();
  }

  @Override
  public final void postSetUp() throws Exception {
    SerializableRunnable create = new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache(new CacheFactory());
        Region<Integer, Object> region = cache.<Integer, Object>createRegionFactory(RegionShortcut.PARTITION).create("test");
        if (region == null) {
          LogWriterUtils.getLogWriter().error("oops!");
        }
      }
    };
    
    SerializableRunnable load = new SerializableRunnable() {
      @Override
      public void run() {
        Region<Integer, Object> region = getCache().getRegion("test");
        for (int i = 0; i < count; i++) {
          region.put(i, i);
        }
      }
    };
    
    Host h = Host.getHost(0);
    h.getVM(1).invoke(create);
    h.getVM(2).invoke(create);
    h.getVM(3).invoke(create);

    h.getVM(1).invoke(load);
  }
}
