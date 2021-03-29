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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.query.internal.cq.CqService;

public class FilterProfileTest {
  private LocalRegion region;
  private FilterProfile filterProfile;
  private final EntryEventImpl event = mock(EntryEventImpl.class);
  private final CqService cqService = mock(CqService.class);
  private final FilterRoutingInfo routingInfo = mock(FilterRoutingInfo.class);
  private final FilterRoutingInfo.FilterInfo filterInfo = mock(FilterRoutingInfo.FilterInfo.class);

  @Before
  public void setUp() {
    region = mock(LocalRegion.class);
    GemFireCacheImpl mockCache = mock(GemFireCacheImpl.class);
    when(mockCache.getCacheServers()).thenReturn(Collections.emptyList());
    when(region.getGemFireCache()).thenReturn(mockCache);
    filterProfile = spy(new FilterProfile(region));
    when(cqService.isRunning()).thenReturn(true);
    when(event.getOperation()).thenReturn(Operation.CREATE);
    when(event.getRegion()).thenReturn(region);
  }

  @Test
  public void getFilterRoutingInfoPart2DoesNotSetRoutingInfoIfNoCacheServer() {
    filterProfile.getLocalProfile().hasCacheServer = false;
    doReturn(cqService).when(filterProfile).getCqService(region);

    assertThat(filterProfile.getFilterRoutingInfoPart2(routingInfo, event)).isEqualTo(routingInfo);

    verify(filterProfile, never()).setLocalCQRoutingInfo(event, routingInfo);
    verify(filterProfile, never()).setLocalInterestRoutingInfo(event, routingInfo, false);
  }

  @Test
  public void getFilterRoutingInfoPart2SetsLocalCQAndInterestRoutingInfo() {
    filterProfile.getLocalProfile().hasCacheServer = true;
    doReturn(cqService).when(filterProfile).getCqService(region);

    assertThat(filterProfile.getFilterRoutingInfoPart2(routingInfo, event)).isEqualTo(routingInfo);

    verify(filterProfile).setLocalCQRoutingInfo(event, routingInfo);
    verify(filterProfile).setLocalInterestRoutingInfo(event, routingInfo, false);
  }

  @Test
  public void getFilterRoutingInfoPart2DoesNotSetLocalCQRoutingInfoIfNoRunningCQService() {
    when(cqService.isRunning()).thenReturn(false);
    filterProfile.getLocalProfile().hasCacheServer = true;
    doReturn(cqService).when(filterProfile).getCqService(region);

    assertThat(filterProfile.getFilterRoutingInfoPart2(routingInfo, event, true)).isEqualTo(
        routingInfo);

    verify(filterProfile, never()).setLocalCQRoutingInfo(event, routingInfo);
    verify(filterProfile).setLocalInterestRoutingInfo(event, routingInfo, true);
  }

  @Test
  public void getFilterRoutingInfoPart2DoesNotSetLocalCQRoutingInfoForEventInConflict() {
    when(event.isConcurrencyConflict()).thenReturn(true);
    filterProfile.getLocalProfile().hasCacheServer = true;
    doReturn(cqService).when(filterProfile).getCqService(region);

    assertThat(filterProfile.getFilterRoutingInfoPart2(routingInfo, event)).isEqualTo(routingInfo);

    verify(filterProfile, never()).setLocalCQRoutingInfo(event, routingInfo);
    verify(filterProfile).setLocalInterestRoutingInfo(event, routingInfo, false);
  }

  @Test
  public void getFilterRoutingInfoPart2DoesNotSetLocalCQRoutingInfoIfRegionIsNull() {
    filterProfile = spy(new FilterProfile());
    filterProfile.getLocalProfile().hasCacheServer = true;
    LocalRegion eventRegion = mock(LocalRegion.class);
    when(event.getRegion()).thenReturn(eventRegion);
    doReturn(cqService).when(filterProfile).getCqService(eventRegion);

    assertThat(filterProfile.getFilterRoutingInfoPart2(routingInfo, event)).isEqualTo(routingInfo);

    verify(filterProfile, never()).setLocalCQRoutingInfo(event, routingInfo);
    verify(filterProfile).setLocalInterestRoutingInfo(event, routingInfo, false);
  }

  @Test
  public void isTransactionalEventReturnsFalseIfNotEntryEvent() {
    CacheEvent event = mock(CacheEvent.class);
    when(event.getOperation()).thenReturn(Operation.REGION_CREATE);

    assertThat(filterProfile.isTransactionalEvent(event)).isFalse();
  }

  @Test
  public void isTransactionalEventReturnsTrueIfATransactionalEntryEvent() {
    EntryEventImpl event = mock(EntryEventImpl.class);
    when(event.getOperation()).thenReturn(Operation.UPDATE);
    when(event.isTransactional()).thenReturn(true);

    assertThat(filterProfile.isTransactionalEvent(event)).isTrue();
  }

  @Test
  public void isTransactionalEventReturnsFalseIfNotATransactionalEntryEvent() {
    EntryEventImpl event = mock(EntryEventImpl.class);
    when(event.getOperation()).thenReturn(Operation.UPDATE);
    when(event.isTransactional()).thenReturn(false);

    assertThat(filterProfile.isTransactionalEvent(event)).isFalse();
  }

  @Test
  public void isCQRoutingNeededReturnsTrueIfEventNotTransactional() {
    doReturn(false).when(filterProfile).isTransactionalEvent(event);

    assertThat(filterProfile.isCQRoutingNeeded(event)).isTrue();
  }

  @Test
  public void isCQRoutingNeededReturnsTrueIfLocalFilterInfoIsNotSetInEvent() {
    doReturn(true).when(filterProfile).isTransactionalEvent(event);
    when(event.getLocalFilterInfo()).thenReturn(null);

    assertThat(filterProfile.isCQRoutingNeeded(event)).isTrue();
  }

  @Test
  public void isCQRoutingNeededReturnsFalseIfLocalFilterInfoIsSetInEvent() {
    doReturn(true).when(filterProfile).isTransactionalEvent(event);
    when(event.getLocalFilterInfo()).thenReturn(filterInfo);

    assertThat(filterProfile.isCQRoutingNeeded(event)).isFalse();
  }

  @Test
  public void setLocalCQRoutingInfoFillsInCQRoutingInfoIfCQRoutingNeeded() {
    doReturn(true).when(filterProfile).isCQRoutingNeeded(event);
    FilterRoutingInfo info = mock(FilterRoutingInfo.class);
    doNothing().when(filterProfile).fillInCQRoutingInfo(event, true, FilterProfile.NO_PROFILES,
        info);

    filterProfile.setLocalCQRoutingInfo(event, info);

    verify(filterProfile).fillInCQRoutingInfo(event, true, FilterProfile.NO_PROFILES, info);
  }

  @Test
  public void setLocalCQRoutingInfoSetsLocalFilterInfoFromEventIfCQRoutingNotNeeded() {
    doReturn(false).when(filterProfile).isCQRoutingNeeded(event);
    when(event.getLocalFilterInfo()).thenReturn(filterInfo);


    filterProfile.setLocalCQRoutingInfo(event, routingInfo);

    verify(routingInfo).setLocalFilterInfo(filterInfo);
  }

  @Test
  public void setLocalInterestRoutingInfoFillsInInterestRoutingInfoIfEventNotTransactional() {
    doReturn(false).when(filterProfile).isTransactionalEvent(event);
    doReturn(routingInfo).when(filterProfile).fillInInterestRoutingInfo(event,
        filterProfile.localProfileArray, routingInfo, Collections.emptySet());

    assertThat(filterProfile.setLocalInterestRoutingInfo(event, routingInfo, true))
        .isEqualTo(routingInfo);

    verify(filterProfile).fillInInterestRoutingInfo(event, filterProfile.localProfileArray,
        routingInfo, Collections.emptySet());
  }

  @Test
  public void setLocalInterestRoutingInfoFillsInInterestRoutingInfoIfNoFilterInfoInTransactionalEvent() {
    doReturn(true).when(filterProfile).isTransactionalEvent(event);
    doReturn(routingInfo).when(filterProfile).fillInInterestRoutingInfo(event,
        filterProfile.localProfileArray, routingInfo, Collections.emptySet());

    assertThat(filterProfile.setLocalInterestRoutingInfo(event, routingInfo, false))
        .isEqualTo(routingInfo);

    verify(filterProfile).fillInInterestRoutingInfo(event, filterProfile.localProfileArray,
        routingInfo, Collections.emptySet());
  }

  @Test
  public void setLocalInterestRoutingInfoFillsInInterestRoutingInfoIfChangeAppliedToCache() {
    doReturn(true).when(filterProfile).isTransactionalEvent(event);
    doReturn(routingInfo).when(filterProfile).fillInInterestRoutingInfo(event,
        filterProfile.localProfileArray, routingInfo, Collections.emptySet());
    when(event.getLocalFilterInfo()).thenReturn(filterInfo);
    when(filterInfo.isChangeAppliedToCache()).thenReturn(true);

    assertThat(filterProfile.setLocalInterestRoutingInfo(event, routingInfo, false))
        .isEqualTo(routingInfo);

    verify(filterProfile).fillInInterestRoutingInfo(event, filterProfile.localProfileArray,
        routingInfo, Collections.emptySet());
  }

  @Test
  public void setLocalInterestRoutingInfoDoesNotFillInInterestRoutingInfoIfNotAppliedToCacheYet() {
    doReturn(true).when(filterProfile).isTransactionalEvent(event);
    doReturn(routingInfo).when(filterProfile).fillInInterestRoutingInfo(event,
        filterProfile.localProfileArray, routingInfo, Collections.emptySet());
    when(event.getLocalFilterInfo()).thenReturn(filterInfo);
    when(filterInfo.isChangeAppliedToCache()).thenReturn(false);

    assertThat(filterProfile.setLocalInterestRoutingInfo(event, routingInfo, false))
        .isEqualTo(routingInfo);

    verify(filterProfile, never()).fillInInterestRoutingInfo(event, filterProfile.localProfileArray,
        routingInfo, Collections.emptySet());
  }

  @Test
  public void setLocalInterestRoutingInfoFillsInInterestRoutingInfoIfNeededToCompute() {
    doReturn(true).when(filterProfile).isTransactionalEvent(event);
    doReturn(routingInfo).when(filterProfile).fillInInterestRoutingInfo(event,
        filterProfile.localProfileArray, routingInfo, Collections.emptySet());
    when(event.getLocalFilterInfo()).thenReturn(filterInfo);
    when(filterInfo.isChangeAppliedToCache()).thenReturn(false);

    assertThat(filterProfile.setLocalInterestRoutingInfo(event, routingInfo, true))
        .isEqualTo(routingInfo);

    verify(filterProfile).fillInInterestRoutingInfo(event, filterProfile.localProfileArray,
        routingInfo, Collections.emptySet());
  }
}
