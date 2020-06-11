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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpSession;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.modules.session.catalina.callback.SessionExpirationCacheListener;
import org.apache.geode.modules.util.RegionConfiguration;
import org.apache.geode.modules.util.SessionCustomExpiry;
import org.apache.geode.modules.util.TouchPartitionedRegionEntriesFunction;
import org.apache.geode.modules.util.TouchReplicatedRegionEntriesFunction;

public class PeerToPeerSessionCacheTest extends AbstractSessionCacheTest {

  private final String localRegionName = sessionRegionName + "_local";
  private final RegionFactory<String, HttpSession> regionFactory = mock(RegionFactory.class);
  private final Region<String, HttpSession> localRegion = mock(Region.class);
  private final Cache cache = mock(Cache.class);

  @Before
  public void setUp() {
    sessionCache = spy(new PeerToPeerSessionCache(sessionManager, cache));
    doReturn(sessionRegion).when((PeerToPeerSessionCache) sessionCache)
        .createRegionUsingHelper(any(RegionConfiguration.class));
    doReturn(true).when((PeerToPeerSessionCache) sessionCache)
        .isFunctionRegistered(any(String.class));

    when(sessionManager.getRegionName()).thenReturn(sessionRegionName);
    when(sessionManager.getRegionAttributesId()).thenReturn(RegionShortcut.PARTITION.toString());
    when(sessionManager.getLogger()).thenReturn(logger);
    when(sessionManager.getEnableLocalCache()).thenReturn(true);
    when(sessionManager.getMaxInactiveInterval())
        .thenReturn(RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL);

    when(sessionRegion.getName()).thenReturn(sessionRegionName);

    doReturn(regionFactory).when(cache).createRegionFactory(RegionShortcut.LOCAL_HEAP_LRU);
    when(regionFactory.create(localRegionName)).thenReturn(localRegion);

    when(cache.getDistributedSystem()).thenReturn(distributedSystem);
  }

  @Test
  public void initializeSessionCacheSucceeds() {
    sessionCache.initialize();

    verify(cache).createRegionFactory(RegionShortcut.LOCAL_HEAP_LRU);
    verify(regionFactory).create(localRegionName);
    verify((PeerToPeerSessionCache) sessionCache)
        .createRegionUsingHelper(any(RegionConfiguration.class));
    verify(regionFactory, times(0)).setStatisticsEnabled(true);
    verify(regionFactory, times(0)).setCustomEntryIdleTimeout(any(SessionCustomExpiry.class));
    verify(regionFactory, times(0)).addCacheListener(any(SessionExpirationCacheListener.class));
  }

  @Test
  public void functionRegistrationDoesNotThrowException() {
    doReturn(false).when((PeerToPeerSessionCache) sessionCache)
        .isFunctionRegistered(any(String.class));

    sessionCache.initialize();

    verify((PeerToPeerSessionCache) sessionCache).registerFunctionWithFunctionService(any(
        TouchPartitionedRegionEntriesFunction.class));
    verify((PeerToPeerSessionCache) sessionCache).registerFunctionWithFunctionService(any(
        TouchReplicatedRegionEntriesFunction.class));
  }

  @Test
  public void initializeSessionCacheSucceedsWhenSessionRegionAlreadyExists() {
    doReturn(sessionRegion).when(cache).getRegion(sessionRegionName);
    doNothing().when((PeerToPeerSessionCache) sessionCache)
        .validateRegionUsingRegionhelper(any(RegionConfiguration.class), any(Region.class));

    sessionCache.initialize();

    verify((PeerToPeerSessionCache) sessionCache, times(0))
        .createRegionUsingHelper(any(RegionConfiguration.class));
  }

  @Test
  public void nonDefaultMaxTimeoutIntervalSetsExpirationDetails() {
    // Setting the mocked return value of getMaxInactiveInterval to something distinctly not equal
    // to the default
    when(sessionManager.getMaxInactiveInterval())
        .thenReturn(RegionConfiguration.DEFAULT_MAX_INACTIVE_INTERVAL + 1);

    sessionCache.initialize();

    verify(regionFactory).setStatisticsEnabled(true);
    verify(regionFactory).setCustomEntryIdleTimeout(any(SessionCustomExpiry.class));
    verify(regionFactory).addCacheListener(any(SessionExpirationCacheListener.class));
  }

  @Test
  public void initializeSessionCacheSucceedsWhenLocalRegionAlreadyExists() {
    doReturn(localRegion).when(cache).getRegion(localRegionName);

    sessionCache.initialize();

    verify(regionFactory, times(0)).create(any(String.class));
  }


  @Test
  public void operationRegionIsCorrectlySetWhenEnableLocalCachingIsFalse() {
    when(sessionManager.getEnableLocalCache()).thenReturn(false);

    sessionCache.initialize();

    verify(cache, times(0)).getRegion(localRegionName);
    assertThat(sessionCache.sessionRegion).isEqualTo(sessionCache.operatingRegion);
  }

  @Test
  public void touchSessionsWithPartitionedRegionSucceeds() {
    final Set<String> sessionIds = new HashSet<>();
    final ResultCollector collector = mock(ResultCollector.class);

    when(sessionManager.getRegionAttributesId()).thenReturn(RegionShortcut.PARTITION.toString());
    doReturn(emptyExecution).when((PeerToPeerSessionCache) sessionCache)
        .getExecutionForFunctionOnRegionWithFilter(sessionIds);
    when(emptyExecution.execute(TouchPartitionedRegionEntriesFunction.ID)).thenReturn(collector);

    sessionCache.touchSessions(sessionIds);

    verify(emptyExecution).execute(TouchPartitionedRegionEntriesFunction.ID);
    verify(collector).getResult();
  }

  @Test
  public void touchSessionsWithReplicatedRegionSucceeds() {
    // Need to invoke this to set the session region
    sessionCache.initialize();

    final Set<String> sessionIds = new HashSet<>();
    final ResultCollector collector = mock(ResultCollector.class);

    when(sessionManager.getRegionAttributesId()).thenReturn(RegionShortcut.REPLICATE.toString());
    doReturn(emptyExecution).when((PeerToPeerSessionCache) sessionCache)
        .getExecutionForFunctionOnMembersWithArguments(any(Object[].class));
    when(emptyExecution.execute(TouchReplicatedRegionEntriesFunction.ID)).thenReturn(collector);

    sessionCache.touchSessions(sessionIds);

    verify(emptyExecution).execute(TouchReplicatedRegionEntriesFunction.ID);
    verify(collector).getResult();
  }

  @Test
  public void touchSessionsCatchesThrownException() {
    final Set<String> sessionIds = new HashSet<>();
    final ResultCollector collector = mock(ResultCollector.class);
    final FunctionException exception = new FunctionException();

    when(sessionManager.getRegionAttributesId()).thenReturn(RegionShortcut.PARTITION.toString());
    doReturn(emptyExecution).when((PeerToPeerSessionCache) sessionCache)
        .getExecutionForFunctionOnRegionWithFilter(sessionIds);
    when(emptyExecution.execute(TouchPartitionedRegionEntriesFunction.ID)).thenReturn(collector);
    doThrow(exception).when(collector).getResult();

    sessionCache.touchSessions(sessionIds);

    verify(logger).warn("Caught unexpected exception:", exception);
  }
}
