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
package org.apache.geode.cache.server.internal;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ConnectionCountProbeJUnitTest {
  
  @Test
  public void test() {
    ConnectionCountProbe probe = new ConnectionCountProbe();
    ServerMetricsImpl metrics = new ServerMetricsImpl(800);
    ServerLoad load = probe.getLoad(metrics);
    assertEquals(0f, load.getConnectionLoad(), .0001f);
    assertEquals(0f, load.getSubscriptionConnectionLoad(), .0001f);
    assertEquals(1/800f, load.getLoadPerConnection(), .0001f);
    assertEquals(1f, load.getLoadPerSubscriptionConnection(), .0001f);

    for(int i = 0; i < 100; i++) {
      metrics.incConnectionCount();
    }
    
    load = probe.getLoad(metrics);
    assertEquals(0.125, load.getConnectionLoad(), .0001f);
  }
    
}
