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
package org.apache.geode.security;

import static com.googlecode.catchexception.CatchException.*;
import static com.googlecode.catchexception.apis.BDDCatchException.caughtException;
import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({DistributedTest.class, SecurityTest.class})
public class IntegratedClientAuthDUnitTest extends AbstractSecureServerDUnitTest {

  @Test
  public void authWithCorrectPasswordShouldPass() {
    client1.invoke("logging in super-user with correct password", () -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties("super-user", "1234567"))
          .setPoolSubscriptionEnabled(true).addPoolServer("localhost", serverPort).create();
    });
  }

  @Test
  public void authWithIncorrectPasswordShouldFail() {
    client2.invoke("logging in super-user with wrong password", () -> {
      catchException(new ClientCacheFactory(createClientProperties("data", "wrong"))
          .setPoolSubscriptionEnabled(true).addPoolServer("localhost", serverPort)).create();
      assertThat((Throwable) caughtException()).isInstanceOf(AuthenticationFailedException.class);
    });
  }
}


