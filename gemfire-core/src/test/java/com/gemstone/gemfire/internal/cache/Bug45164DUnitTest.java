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
package com.gemstone.gemfire.internal.cache;

import java.util.Map.Entry;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

public class Bug45164DUnitTest extends CacheTestCase {
  private static final int count = 10000;
  private static final int stride = 3;
  
  public Bug45164DUnitTest(String name) {
    super(name);
  }

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
  
  public void setUp() throws Exception {
    super.setUp();
    SerializableRunnable create = new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache(new CacheFactory());
        Region<Integer, Object> region = cache.<Integer, Object>createRegionFactory(RegionShortcut.PARTITION).create("test");
        if (region == null) {
          getLogWriter().error("oops!");
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
