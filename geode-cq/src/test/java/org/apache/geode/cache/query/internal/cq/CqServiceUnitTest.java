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
package org.apache.geode.cache.query.internal.cq;


import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.apache.geode.cache.query.cq.internal.CqServiceImpl;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.test.fake.Fakes;

public class CqServiceUnitTest {

  @Test
  public void constructCqServerNameShouldReturnSameResultRegardlessOfOptimizedCacheNames() {
    CqServiceImpl cqService = new CqServiceImpl(Fakes.cache());
    ClientProxyMembershipID proxyMembershipID =
        new ClientProxyMembershipID(Fakes.cache().getDistributedSystem().getDistributedMember());
    String name1 = cqService.constructServerCqName("myCq", proxyMembershipID);
    String name2 = cqService.constructServerCqName("myCq", proxyMembershipID);
    assertEquals(name1, name2);
  }

  @Test
  public void constructCqServerNameShouldReturnCorrectResultsEvenAfterCqClosingRemovesValuesFromOptimizedCache()
      throws Exception {
    CqServiceImpl cqService = new CqServiceImpl(Fakes.cache());
    ClientProxyMembershipID proxyMembershipID =
        new ClientProxyMembershipID(Fakes.cache().getDistributedSystem().getDistributedMember());
    String name1 = cqService.constructServerCqName("myCq", proxyMembershipID);
    cqService.closeCq("myCq", proxyMembershipID);
    String name2 = cqService.constructServerCqName("myCq", proxyMembershipID);
    assertEquals(name1, name2);
  }


}
