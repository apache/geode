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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.logging.LogService;

@RunWith(PowerMockRunner.class)
@PrepareForTest(LogService.class)
@PowerMockIgnore("javax.script.*")
public class MBeanProxyFactoryTest {
  @Test
  public void removeAllProxiesEntryNotFoundLogged() {
    mockStatic(LogService.class);
    Logger mockLogger = PowerMockito.mock(Logger.class);
    when(mockLogger.isDebugEnabled()).thenReturn(true);
    when(LogService.getLogger()).thenReturn(mockLogger);

    MBeanProxyFactory mBeanProxyFactory =
        new MBeanProxyFactory(mock(MBeanJMXAdapter.class), mock(SystemManagementService.class));
    Region mockRegion = mock(Region.class);
    Set entrySet = new HashSet<Map.Entry<String, Object>>();

    Map.Entry mockEntry = mock(Map.Entry.class);
    doThrow(new EntryNotFoundException("Test EntryNotFoundException")).when(mockEntry).getKey();

    entrySet.add(mockEntry);

    doReturn(entrySet).when(mockRegion).entrySet();
    mBeanProxyFactory.removeAllProxies(mock(DistributedMember.class), mockRegion);

    // EntryNotFoundException should just result in a warning as it implies
    // the proxy has already been removed and the entry has already been destroyed
    verify(mockLogger, times(1)).warn(anyString(), any(), any());
  }
}
