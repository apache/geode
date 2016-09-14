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
package org.apache.geode.security;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.internal.cache.functions.TestFunction;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })
public class IntegratedClientExecuteFunctionAuthDistributedTest extends AbstractSecureServerDUnitTest {

  private final static Function function = new TestFunction(true, TestFunction.TEST_FUNCTION1);

  @Test
  public void testExecuteRegionFunction() {

    FunctionService.registerFunction(function);

    client1.invoke("logging in with dataReader", () -> {
      ClientCache cache = createClientCache("dataReader", "1234567", serverPort);

      FunctionService.registerFunction(function);
      assertNotAuthorized(() -> FunctionService.onServer(cache.getDefaultPool())
                                               .withArgs(Boolean.TRUE)
                                               .execute(function.getId()), "DATA:WRITE");
    });

    client2.invoke("logging in with super-user", () -> {
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);

      FunctionService.registerFunction(function);
      ResultCollector rc = FunctionService.onServer(cache.getDefaultPool())
                                          .withArgs(Boolean.TRUE)
                                          .execute(function.getId());
      rc.getResult();
    });
  }
}


