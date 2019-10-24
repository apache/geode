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
package org.apache.geode.management.internal;

import static java.util.Collections.singleton;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;

public class MBeanProxyFactoryTest {

  private MBeanJMXAdapter jmxAdapter;
  private Region<String, Object> region;
  private Map.Entry<String, Object> regionEntry;
  private SystemManagementService managementService;

  @Before
  public void setUp() {
    jmxAdapter = mock(MBeanJMXAdapter.class);
    managementService = mock(SystemManagementService.class);
    region = mock(Region.class);
    regionEntry = mock(Map.Entry.class);
  }

  @Test
  public void removeAllProxiesEntryNotFoundLogged() {
    Set<Map.Entry<String, Object>> entrySet = new HashSet<>(singleton(regionEntry));
    when(region.entrySet())
        .thenReturn(entrySet);
    when(regionEntry.getKey())
        .thenThrow(new EntryNotFoundException("test"));

    MBeanProxyFactory mBeanProxyFactory = spy(new MBeanProxyFactory(jmxAdapter, managementService));

    mBeanProxyFactory.removeAllProxies(mock(DistributedMember.class), region);

    // EntryNotFoundException should just result in a warning as it implies
    // the proxy has already been removed and the entry has already been destroyed
    verify(mBeanProxyFactory)
        .logProxyAlreadyRemoved(any(), any());
  }
}
