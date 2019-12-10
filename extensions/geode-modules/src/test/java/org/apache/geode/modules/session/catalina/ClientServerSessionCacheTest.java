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
package org.apache.geode.modules.session.catalina;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;

public class ClientServerSessionCacheTest {


  @Test
  public void registerInterestForSessionRegion() {
    SessionManager manager = mock(SessionManager.class);
    ClientCache clientCache = mock(ClientCache.class);
    Region region = mock(Region.class);
    ClientServerSessionCache cache = spy(new ClientServerSessionCache(manager, clientCache));
    doReturn(region).when(cache).createLocalSessionRegion();

    cache.createLocalSessionRegionWithRegisterInterest();

    verify(region).registerInterestForAllKeys(InterestResultPolicy.KEYS);
  }

}
