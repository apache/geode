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
package com.gemstone.gemfire.cache.server.internal;

import org.junit.experimental.categories.Category;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.gemstone.gemfire.cache.server.ServerLoad;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 *
 */
@Category(UnitTest.class)
public class ConnectionCountProbeJUnitTest extends TestCase {
  
  public void test() {
    ConnectionCountProbe probe = new ConnectionCountProbe();
    ServerMetricsImpl metrics = new ServerMetricsImpl(800);
    ServerLoad load = probe.getLoad(metrics);
    Assert.assertEquals(0f, load.getConnectionLoad(), .0001f);
    Assert.assertEquals(0f, load.getSubscriptionConnectionLoad(), .0001f);
    Assert.assertEquals(1/800f, load.getLoadPerConnection(), .0001f);
    Assert.assertEquals(1f, load.getLoadPerSubscriptionConnection(), .0001f);

    for(int i = 0; i < 100; i++) {
      metrics.incConnectionCount();
    }
    
    load = probe.getLoad(metrics);
    Assert.assertEquals(0.125, load.getConnectionLoad(), .0001f);
  }
    
}
