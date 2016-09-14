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

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.internal.GetClientPartitionAttributesOp;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })
public class IntegratedClientGetClientPartitionAttrCmdAuthDistributedTest
  extends AbstractSecureServerDUnitTest {

  @Test
  @Ignore("This is not a supported client message")
  // this would fail sporatically because ServerConnection.isInternalMessage would return true for this message,
  // and it won't bind the correct subject on the executing thread.
  public void testGetClientPartitionAttrCmd() {
    client1.invoke("logging in stranger", () -> {
      ClientCache cache = createClientCache("stranger", "1234567", serverPort);

      assertNotAuthorized(() -> GetClientPartitionAttributesOp.execute((PoolImpl) cache.getDefaultPool(), REGION_NAME), "CLUSTER:READ");
    });

    client2.invoke("logging in super-user with correct password", () -> {
      ClientCache cache = createClientCache("super-user", "1234567", serverPort);

      GetClientPartitionAttributesOp.execute((PoolImpl) cache.getDefaultPool(), REGION_NAME);
    });
  }
}


