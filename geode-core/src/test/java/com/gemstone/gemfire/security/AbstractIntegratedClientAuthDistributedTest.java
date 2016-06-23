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
package com.gemstone.gemfire.security;

import static com.gemstone.gemfire.security.SecurityTestUtils.*;
import static org.assertj.core.api.Assertions.*;
import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.management.internal.security.JSONAuthorization;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.After;
import org.junit.Before;

public class AbstractIntegratedClientAuthDistributedTest extends JUnit4DistributedTestCase {

  protected VM client1 = null;
  protected VM client2 = null;
  protected VM client3 = null;
  protected int serverPort;

  @Before
  public void before() throws Exception{
    final Host host = Host.getHost(0);
    client1 = host.getVM(1);
    client2 = host.getVM(2);
    client3 = host.getVM(3);

    JSONAuthorization.setUpWithJsonFile("clientServer.json");
    serverPort =  SecurityTestUtils.createCacheServer(JSONAuthorization.class.getName()+".create");
    Region region = getCache().getRegion(SecurityTestUtils.REGION_NAME);
    assertEquals(0, region.size());
    for (int i = 0; i < 5; i++) {
      String key = "key" + i;
      String value = "value" + i;
      region.put(key, value);
    }
    assertEquals(5, region.size());
  }

  @After
  public void after(){
    client1.invoke(() -> closeCache());
    client2.invoke(() -> closeCache());
    client3.invoke(() -> closeCache());
    closeCache();
  }

  public static void assertNotAuthorized(ThrowingCallable shouldRaiseThrowable, String permString) {
    assertThatThrownBy(shouldRaiseThrowable).hasMessageContaining(permString);
  }

}
