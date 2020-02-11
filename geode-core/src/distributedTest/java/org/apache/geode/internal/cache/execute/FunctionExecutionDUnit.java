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
package org.apache.geode.internal.cache.execute;

import static org.apache.geode.internal.lang.ThrowableUtils.hasCauseType;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class FunctionExecutionDUnit implements Serializable {

  public class TestFunction implements Function {

    @Override
    public void execute(FunctionContext context) {
      if (context.getCache().getRegion("testRegion") == null) {
        RegionFactory regionFactory = context.getCache().createRegionFactory();
        regionFactory.create("testRegion");
        context.getResultSender().lastResult(true);
      } else {
        context.getResultSender().lastResult(false);
      }
    }

    @Override
    public String getId() {
      return getClass().getSimpleName();
    }
  }

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Test
  public void test() throws Exception {
    MemberVM locator = cluster.startLocatorVM(0);
    MemberVM server = cluster.startServerVM(1, locator.getPort());
    ClientVM client = cluster.startClientVM(2, c -> c.withLocatorConnection(locator.getPort()));

    server.invoke(() -> {
      Function function = new TestFunction();
      FunctionService.registerFunction(function);
    });
    AsyncInvocation async = client.invokeAsync(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      while (true) {
        FunctionService.onServers(clientCache).execute("TestFunction").getResult();
      }
    });
    server.invoke(() -> {
      Region region = null;
      while (region == null) {
        region = ClusterStartupRule.getCache().getRegion("testRegion");
      }
    });
    cluster.stop(1);
    cluster.stop(0);
    waitForException(async);
  }

  private void waitForException(AsyncInvocation async) {
    try {
      async.await();
    } catch (Throwable e) {
      assertThat(hasCauseType(e, NullPointerException.class)).isFalse();
      assertThat(hasCauseType(e, NoAvailableServersException.class)).isTrue();
    }
  }
}
