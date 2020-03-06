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
package org.apache.geode.management.internal.cli.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ClientSubscriptionConfig;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.internal.util.CollectionUtils;
import org.apache.geode.management.internal.cli.domain.DiskStoreDetails;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;

/**
 * The DescribeDiskStoreFunctionJUnitTest test suite class tests the contract and functionality of
 * the DescribeDiskStoreFunction class.
 *
 * @see org.apache.geode.cache.DiskStore
 * @see org.apache.geode.management.internal.cli.domain.DiskStoreDetails
 * @see org.apache.geode.management.internal.cli.functions.DescribeDiskStoreFunction
 * @see org.junit.Test
 * @since GemFire 7.0
 */
public class DescribeDiskStoreFunctionJUnitTest {
  private InternalCache mockCache;

  @Before
  public void setup() {
    mockCache = mock(InternalCache.class, "Cache");
  }

  private void assertAsyncEventQueueDetails(
      final Set<DiskStoreDetails.AsyncEventQueueDetails> expectedAsyncEventQueueDetailsSet,
      final DiskStoreDetails diskStoreDetails) {
    int actualCount = 0;

    for (final DiskStoreDetails.AsyncEventQueueDetails actualAsyncEventQueueDetails : diskStoreDetails
        .iterateAsyncEventQueues()) {
      final DiskStoreDetails.AsyncEventQueueDetails expectedAsyncEventQueueDetails = CollectionUtils
          .findBy(expectedAsyncEventQueueDetailsSet, asyncEventQueueDetails -> ObjectUtils
              .equals(asyncEventQueueDetails.getId(), actualAsyncEventQueueDetails.getId()));

      assertThat(expectedAsyncEventQueueDetails).isNotNull();
      actualCount++;
    }

    assertThat(actualCount).isEqualTo(expectedAsyncEventQueueDetailsSet.size());
  }

  private void assertCacheServerDetails(
      final Set<DiskStoreDetails.CacheServerDetails> expectedCacheServerDetailsSet,
      final DiskStoreDetails diskStoreDetails) {
    int actualCount = 0;

    for (final DiskStoreDetails.CacheServerDetails actualCacheServerDetails : diskStoreDetails
        .iterateCacheServers()) {
      final DiskStoreDetails.CacheServerDetails expectedCacheServerDetails =
          CollectionUtils.findBy(expectedCacheServerDetailsSet,
              cacheServerDetails -> ObjectUtils.equals(cacheServerDetails.getBindAddress(),
                  actualCacheServerDetails.getBindAddress())
                  && ObjectUtils.equals(cacheServerDetails.getPort(),
                      actualCacheServerDetails.getPort()));

      assertThat(expectedCacheServerDetails).isNotNull();
      assertThat(actualCacheServerDetails.getHostName())
          .isEqualTo(expectedCacheServerDetails.getHostName());
      actualCount++;
    }

    assertThat(actualCount).isEqualTo(expectedCacheServerDetailsSet.size());
  }

  private void assertGatewayDetails(
      final Set<DiskStoreDetails.GatewayDetails> expectedGatewayDetailsSet,
      final DiskStoreDetails diskStoreDetails) {
    int actualCount = 0;

    for (final DiskStoreDetails.GatewayDetails actualGatewayDetails : diskStoreDetails
        .iterateGateways()) {
      DiskStoreDetails.GatewayDetails expectedGatewayDetails =
          CollectionUtils.findBy(expectedGatewayDetailsSet, gatewayDetails -> ObjectUtils
              .equals(gatewayDetails.getId(), actualGatewayDetails.getId()));

      assertThat(expectedGatewayDetails).isNotNull();
      assertThat(actualGatewayDetails.isPersistent())
          .isEqualTo(expectedGatewayDetails.isPersistent());
      actualCount++;
    }

    assertThat(actualCount).isEqualTo(expectedGatewayDetailsSet.size());
  }

  private void assertRegionDetails(
      final Set<DiskStoreDetails.RegionDetails> expectedRegionDetailsSet,
      final DiskStoreDetails diskStoreDetails) {
    int actualCount = 0;

    for (final DiskStoreDetails.RegionDetails actualRegionDetails : diskStoreDetails
        .iterateRegions()) {
      final DiskStoreDetails.RegionDetails expectedRegionDetails =
          CollectionUtils.findBy(expectedRegionDetailsSet, regionDetails -> ObjectUtils
              .equals(regionDetails.getFullPath(), actualRegionDetails.getFullPath()));

      assertThat(expectedRegionDetails).isNotNull();
      assertThat(actualRegionDetails.getName()).isEqualTo(expectedRegionDetails.getName());
      assertThat(actualRegionDetails.isPersistent())
          .isEqualTo(expectedRegionDetails.isPersistent());
      assertThat(actualRegionDetails.isOverflowToDisk())
          .isEqualTo(expectedRegionDetails.isOverflowToDisk());
      actualCount++;
    }

    assertThat(actualCount).isEqualTo(expectedRegionDetailsSet.size());
  }

  private DiskStoreDetails.AsyncEventQueueDetails createAsyncEventQueueDetails(final String id) {
    return new DiskStoreDetails.AsyncEventQueueDetails(id);
  }

  private DiskStoreDetails.CacheServerDetails createCacheServerDetails(final String bindAddress,
      final int port, final String hostname) {
    final DiskStoreDetails.CacheServerDetails cacheServerDetails =
        new DiskStoreDetails.CacheServerDetails(bindAddress, port);
    cacheServerDetails.setHostName(hostname);

    return cacheServerDetails;
  }

  private File[] createFileArray(final String... locations) {
    assert locations != null : "The locations argument cannot be null!";

    final File[] directories = new File[locations.length];
    int index = 0;

    for (final String location : locations) {
      directories[index++] = new File(location);
    }

    return directories;
  }

  private DiskStoreDetails.GatewayDetails createGatewayDetails(final String id,
      final boolean persistent) {
    DiskStoreDetails.GatewayDetails gatewayDetails = new DiskStoreDetails.GatewayDetails(id);
    gatewayDetails.setPersistent(persistent);
    return gatewayDetails;
  }

  private int[] createIntArray(final int... array) {
    assert array != null : "The array of int values cannot be null!";
    return array;
  }

  private DiskStore createMockDiskStore(final UUID diskStoreId, final String name,
      final boolean allowForceCompaction, final boolean autoCompact, final int compactionThreshold,
      final long maxOplogSize, final int queueSize, final long timeInterval,
      final int writeBufferSize, final File[] diskDirs, final int[] diskDirSizes,
      final float warningPercentage, final float criticalPercentage) {
    final DiskStore mockDiskStore = mock(DiskStore.class, name);
    when(mockDiskStore.getAllowForceCompaction()).thenReturn(allowForceCompaction);
    when(mockDiskStore.getAutoCompact()).thenReturn(autoCompact);
    when(mockDiskStore.getCompactionThreshold()).thenReturn(compactionThreshold);
    when(mockDiskStore.getDiskStoreUUID()).thenReturn(diskStoreId);
    when(mockDiskStore.getMaxOplogSize()).thenReturn(maxOplogSize);
    when(mockDiskStore.getName()).thenReturn(name);
    when(mockDiskStore.getQueueSize()).thenReturn(queueSize);
    when(mockDiskStore.getTimeInterval()).thenReturn(timeInterval);
    when(mockDiskStore.getWriteBufferSize()).thenReturn(writeBufferSize);
    when(mockDiskStore.getDiskDirs()).thenReturn(diskDirs);
    when(mockDiskStore.getDiskDirSizes()).thenReturn(diskDirSizes);
    when(mockDiskStore.getDiskUsageWarningPercentage()).thenReturn(warningPercentage);
    when(mockDiskStore.getDiskUsageCriticalPercentage()).thenReturn(criticalPercentage);

    return mockDiskStore;
  }

  private DiskStoreDetails.RegionDetails createRegionDetails(final String fullPath,
      final String name, final boolean persistent, final boolean overflow) {
    final DiskStoreDetails.RegionDetails regionDetails =
        new DiskStoreDetails.RegionDetails(fullPath, name);
    regionDetails.setPersistent(persistent);
    regionDetails.setOverflowToDisk(overflow);

    return regionDetails;
  }

  @SuppressWarnings("unchecked")
  private Set<DiskStoreDetails.RegionDetails> setupRegionsForTestExecute(
      final InternalCache mockCache, final String diskStoreName) {
    final Region<Object, Object> mockUserRegion = mock(Region.class, "/UserRegion");
    final Region<Object, Object> mockGuestRegion = mock(Region.class, "/GuestRegion");
    final Region<Object, Object> mockSessionRegion =
        mock(Region.class, "/UserRegion/SessionRegion");
    final RegionAttributes<Object, Object> mockUserRegionAttributes =
        mock(RegionAttributes.class, "UserRegionAttributes");
    final RegionAttributes<Object, Object> mockSessionRegionAttributes =
        mock(RegionAttributes.class, "SessionRegionAttributes");
    final RegionAttributes<Object, Object> mockGuestRegionAttributes =
        mock(RegionAttributes.class, "GuestRegionAttributes");
    final EvictionAttributes mockUserEvictionAttributes =
        mock(EvictionAttributes.class, "UserEvictionAttributes");
    final EvictionAttributes mockSessionEvictionAttributes =
        mock(EvictionAttributes.class, "SessionEvictionAttributes");
    final EvictionAttributes mockGuestEvictionAttributes =
        mock(EvictionAttributes.class, "GuestEvictionAttributes");

    when(mockCache.rootRegions())
        .thenReturn(CollectionUtils.asSet(mockUserRegion, mockGuestRegion));
    when(mockUserRegion.getAttributes()).thenReturn(mockUserRegionAttributes);
    when(mockUserRegion.getFullPath()).thenReturn("/UserRegion");
    when(mockUserRegion.getName()).thenReturn("UserRegion");
    when(mockUserRegion.subregions(false)).thenReturn(CollectionUtils.asSet(mockSessionRegion));
    when(mockUserRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_PARTITION);
    when(mockUserRegionAttributes.getDiskStoreName()).thenReturn(diskStoreName);
    when(mockUserRegionAttributes.getEvictionAttributes()).thenReturn(mockUserEvictionAttributes);
    when(mockUserEvictionAttributes.getAction()).thenReturn(EvictionAction.LOCAL_DESTROY);
    when(mockSessionRegion.getAttributes()).thenReturn(mockSessionRegionAttributes);
    when(mockSessionRegion.getFullPath()).thenReturn("/UserRegion/SessionRegion");
    when(mockSessionRegion.getName()).thenReturn("SessionRegion");
    when(mockSessionRegion.subregions(false)).thenReturn(Collections.emptySet());
    when(mockSessionRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
    when(mockSessionRegionAttributes.getDiskStoreName()).thenReturn(diskStoreName);
    when(mockSessionRegionAttributes.getEvictionAttributes())
        .thenReturn(mockSessionEvictionAttributes);
    when(mockSessionEvictionAttributes.getAction()).thenReturn(EvictionAction.OVERFLOW_TO_DISK);
    when(mockGuestRegion.getAttributes()).thenReturn(mockGuestRegionAttributes);
    when(mockGuestRegion.subregions(false)).thenReturn(Collections.emptySet());
    when(mockGuestRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
    when(mockGuestRegionAttributes.getDiskStoreName())
        .thenReturn(DiskStoreDetails.DEFAULT_DISK_STORE_NAME);
    when(mockGuestRegionAttributes.getEvictionAttributes()).thenReturn(mockGuestEvictionAttributes);
    when(mockGuestEvictionAttributes.getAction()).thenReturn(EvictionAction.OVERFLOW_TO_DISK);

    return CollectionUtils.asSet(createRegionDetails("/UserRegion", "UserRegion", true, false),
        createRegionDetails("/UserRegion/SessionRegion", "SessionRegion", false, true));
  }

  private Set<DiskStoreDetails.GatewayDetails> setupGatewaysForTestExecute(
      final InternalCache mockCache, final String diskStoreName) {
    final GatewaySender mockGatewaySender = mock(GatewaySender.class, "GatewaySender");
    when(mockCache.getGatewaySenders()).thenReturn(CollectionUtils.asSet(mockGatewaySender));
    when(mockGatewaySender.getDiskStoreName()).thenReturn(diskStoreName);
    when(mockGatewaySender.getId()).thenReturn("0123456789");
    when(mockGatewaySender.isPersistenceEnabled()).thenReturn(true);

    return CollectionUtils.asSet(createGatewayDetails("0123456789", true));
  }

  private Set<DiskStoreDetails.CacheServerDetails> setupCacheServersForTestExecute(
      final InternalCache mockCache, final String diskStoreName) {
    final CacheServer mockCacheServer1 = mock(CacheServer.class, "CacheServer1");
    final CacheServer mockCacheServer2 = mock(CacheServer.class, "CacheServer2");
    final CacheServer mockCacheServer3 = mock(CacheServer.class, "CacheServer3");
    final ClientSubscriptionConfig cacheServer1ClientSubscriptionConfig =
        mock(ClientSubscriptionConfig.class, "cacheServer1ClientSubscriptionConfig");
    final ClientSubscriptionConfig cacheServer3ClientSubscriptionConfig =
        mock(ClientSubscriptionConfig.class, "cacheServer3ClientSubscriptionConfig");

    when(mockCache.getCacheServers())
        .thenReturn(Arrays.asList(mockCacheServer1, mockCacheServer2, mockCacheServer3));
    when(mockCacheServer1.getClientSubscriptionConfig())
        .thenReturn(cacheServer1ClientSubscriptionConfig);
    when(cacheServer1ClientSubscriptionConfig.getDiskStoreName()).thenReturn(diskStoreName);
    when(mockCacheServer2.getClientSubscriptionConfig()).thenReturn(null);
    when(mockCacheServer3.getClientSubscriptionConfig())
        .thenReturn(cacheServer3ClientSubscriptionConfig);
    when(cacheServer3ClientSubscriptionConfig.getDiskStoreName()).thenReturn("");
    when(mockCacheServer1.getBindAddress()).thenReturn("10.127.0.1");
    when(mockCacheServer1.getPort()).thenReturn(10123);
    when(mockCacheServer1.getHostnameForClients()).thenReturn("rodan");

    return CollectionUtils.asSet(createCacheServerDetails("10.127.0.1", 10123, "rodan"));
  }

  private Set<DiskStoreDetails.AsyncEventQueueDetails> setupAsyncEventQueuesForTestExecute(
      final InternalCache mockCache, final String diskStoreName) {
    final AsyncEventQueue mockAsyncEventQueue1 = mock(AsyncEventQueue.class, "AsyncEventQueue1");
    final AsyncEventQueue mockAsyncEventQueue2 = mock(AsyncEventQueue.class, "AsyncEventQueue2");
    final AsyncEventQueue mockAsyncEventQueue3 = mock(AsyncEventQueue.class, "AsyncEventQueue3");

    when(mockCache.getAsyncEventQueues()).thenReturn(
        CollectionUtils.asSet(mockAsyncEventQueue1, mockAsyncEventQueue2, mockAsyncEventQueue3));
    when(mockAsyncEventQueue1.isPersistent()).thenReturn(true);
    when(mockAsyncEventQueue1.getDiskStoreName()).thenReturn(diskStoreName);
    when(mockAsyncEventQueue1.getId()).thenReturn("9876543210");
    when(mockAsyncEventQueue2.isPersistent()).thenReturn(false);
    when(mockAsyncEventQueue3.isPersistent()).thenReturn(true);
    when(mockAsyncEventQueue3.getDiskStoreName()).thenReturn("memSto");

    return CollectionUtils.asSet(createAsyncEventQueueDetails("9876543210"));
  }

  @Test
  public void testAssertState() {
    DescribeDiskStoreFunction.assertState(true, "null");
  }

  @Test
  public void testAssertStateThrowsIllegalStateException() {
    assertThatThrownBy(
        () -> DescribeDiskStoreFunction.assertState(false, "Expected (%1$s) message!", "test"))
            .isInstanceOf(IllegalStateException.class).hasMessage("Expected (test) message!");
  }

  @Test
  public void testExecute() throws Throwable {
    // Prepare Mocks
    final UUID diskStoreId = UUID.randomUUID();
    final String diskStoreName = "mockDiskStore";
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";
    final InternalDistributedMember mockMember =
        mock(InternalDistributedMember.class, "DistributedMember");
    @SuppressWarnings("unchecked")
    final FunctionContext<String> mockFunctionContext =
        mock(FunctionContext.class, "testExecute$FunctionContext");
    final DiskStore mockDiskStore =
        createMockDiskStore(diskStoreId, diskStoreName, true, false,
            75, 8192L, 500, 120L, 10240, createFileArray("/export/disk/backup",
                "/export/disk/overflow", "/export/disk/persistence"),
            createIntArray(10240, 204800, 4096000), 50, 75);
    final TestResultSender testResultSender = new TestResultSender();
    when(mockCache.getMyId()).thenReturn(mockMember);
    when(mockCache.findDiskStore(diskStoreName)).thenReturn(mockDiskStore);
    when(mockCache.getPdxPersistent()).thenReturn(true);
    when(mockCache.getPdxDiskStore()).thenReturn("memoryStore");
    when(mockMember.getId()).thenReturn(memberId);
    when(mockMember.getName()).thenReturn(memberName);
    when(mockFunctionContext.getCache()).thenReturn(mockCache);
    when(mockFunctionContext.getArguments()).thenReturn(diskStoreName);
    when(mockFunctionContext.getResultSender()).thenReturn(testResultSender);

    // Expected Results
    final Set<DiskStoreDetails.RegionDetails> expectedRegionDetails =
        setupRegionsForTestExecute(mockCache, diskStoreName);
    final Set<DiskStoreDetails.GatewayDetails> expectedGatewayDetails =
        setupGatewaysForTestExecute(mockCache, diskStoreName);
    final Set<DiskStoreDetails.CacheServerDetails> expectedCacheServerDetails =
        setupCacheServersForTestExecute(mockCache, diskStoreName);
    final Set<DiskStoreDetails.AsyncEventQueueDetails> expectedAsyncEventQueueDetails =
        setupAsyncEventQueuesForTestExecute(mockCache, diskStoreName);

    // Execute Function and assert results
    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();
    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(1);

    final DiskStoreDetails diskStoreDetails = (DiskStoreDetails) results.get(0);
    AssertionsForClassTypes.assertThat(diskStoreDetails).isNotNull();
    assertThat(diskStoreDetails.getId()).isEqualTo(diskStoreId);
    assertThat(diskStoreDetails.getName()).isEqualTo(diskStoreName);
    assertThat(diskStoreDetails.getMemberId()).isEqualTo(memberId);
    assertThat(diskStoreDetails.getMemberName()).isEqualTo(memberName);
    assertThat(diskStoreDetails.getAllowForceCompaction()).isTrue();
    assertThat(diskStoreDetails.getAutoCompact()).isFalse();
    assertThat(diskStoreDetails.getCompactionThreshold().intValue()).isEqualTo(75);
    assertThat(diskStoreDetails.getMaxOplogSize().longValue()).isEqualTo(8192L);
    assertThat(diskStoreDetails.isPdxSerializationMetaDataStored()).isFalse();
    assertThat(diskStoreDetails.getQueueSize().intValue()).isEqualTo(500);
    assertThat(diskStoreDetails.getTimeInterval().longValue()).isEqualTo(120L);
    assertThat(diskStoreDetails.getWriteBufferSize().intValue()).isEqualTo(10240);
    assertThat(diskStoreDetails.getDiskUsageWarningPercentage()).isEqualTo(50.0f);
    assertThat(diskStoreDetails.getDiskUsageCriticalPercentage()).isEqualTo(75.0f);

    final List<Integer> expectedDiskDirSizes = Arrays.asList(10240, 204800, 4096000);
    final List<String> expectedDiskDirs =
        Arrays.asList(new File("/export/disk/backup").getAbsolutePath(),
            new File("/export/disk/overflow").getAbsolutePath(),
            new File("/export/disk/persistence").getAbsolutePath());
    int count = 0;

    for (final DiskStoreDetails.DiskDirDetails diskDirDetails : diskStoreDetails) {
      assertThat(expectedDiskDirSizes.contains(diskDirDetails.getSize())).isTrue();
      assertThat(expectedDiskDirs.contains(diskDirDetails.getAbsolutePath())).isTrue();
      count++;
    }

    verify(mockDiskStore, atLeastOnce()).getName();
    verify(mockDiskStore, atLeastOnce()).getDiskStoreUUID();
    assertThat(count).isEqualTo(expectedDiskDirs.size());
    assertRegionDetails(expectedRegionDetails, diskStoreDetails);
    assertCacheServerDetails(expectedCacheServerDetails, diskStoreDetails);
    assertGatewayDetails(expectedGatewayDetails, diskStoreDetails);
    assertAsyncEventQueueDetails(expectedAsyncEventQueueDetails, diskStoreDetails);
  }

  @Test
  public void testExecuteOnMemberHavingANonGemFireCache() throws Throwable {
    final Cache mockNonGemCache = mock(Cache.class, "NonGemCache");
    @SuppressWarnings("unchecked")
    final FunctionContext<String> mockFunctionContext =
        mock(FunctionContext.class, "FunctionContext");
    final TestResultSender testResultSender = new TestResultSender();
    when(mockFunctionContext.getCache()).thenReturn(mockNonGemCache);
    when(mockFunctionContext.getResultSender()).thenReturn(testResultSender);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();
    assertThat(results).isNotNull();
    assertThat(results.isEmpty()).isTrue();
  }

  @Test
  public void testExecuteThrowingEntityNotFoundException() {
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";
    final String diskStoreName = "testDiskStore";
    final InternalDistributedMember mockMember =
        mock(InternalDistributedMember.class, "DistributedMember");
    @SuppressWarnings("unchecked")
    final FunctionContext<String> mockFunctionContext =
        mock(FunctionContext.class, "FunctionContext");
    final TestResultSender testResultSender = new TestResultSender();
    when(mockCache.getMyId()).thenReturn(mockMember);
    when(mockCache.findDiskStore(diskStoreName)).thenReturn(null);
    when(mockMember.getId()).thenReturn(memberId);
    when(mockMember.getName()).thenReturn(memberName);
    when(mockFunctionContext.getCache()).thenReturn(mockCache);
    when(mockFunctionContext.getArguments()).thenReturn(diskStoreName);
    when(mockFunctionContext.getResultSender()).thenReturn(testResultSender);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    function.execute(mockFunctionContext);
    String expected = String.format("A disk store with name '%1$s' was not found on member '%2$s'.",
        diskStoreName, memberName);
    assertThatThrownBy(testResultSender::getResults).isInstanceOf(EntityNotFoundException.class)
        .hasMessage(expected);
  }

  @Test
  public void testExecuteThrowingRuntimeException() {
    final String diskStoreName = "testDiskStore";
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";
    @SuppressWarnings("unchecked")
    final FunctionContext<String> mockFunctionContext =
        mock(FunctionContext.class, "FunctionContext");
    final InternalDistributedMember mockMember =
        mock(InternalDistributedMember.class, "DistributedMember");
    final TestResultSender testResultSender = new TestResultSender();
    when(mockCache.getMyId()).thenReturn(mockMember);
    when(mockCache.findDiskStore(diskStoreName)).thenThrow(new RuntimeException("ExpectedStrings"));
    when(mockMember.getId()).thenReturn(memberId);
    when(mockMember.getName()).thenReturn(memberName);
    when(mockFunctionContext.getCache()).thenReturn(mockCache);
    when(mockFunctionContext.getArguments()).thenReturn(diskStoreName);
    when(mockFunctionContext.getResultSender()).thenReturn(testResultSender);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    function.execute(mockFunctionContext);
    assertThatThrownBy(testResultSender::getResults).isInstanceOf(RuntimeException.class)
        .hasMessage("ExpectedStrings");
  }

  @Test
  public void testExecuteWithDiskDirsAndDiskSizesMismatch() {
    final String diskStoreName = "mockDiskStore";
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";
    final UUID diskStoreId = UUID.randomUUID();
    @SuppressWarnings("unchecked")
    final FunctionContext<String> mockFunctionContext =
        mock(FunctionContext.class, "FunctionContext");
    final InternalDistributedMember mockMember =
        mock(InternalDistributedMember.class, "DistributedMember");
    final DiskStore mockDiskStore =
        createMockDiskStore(diskStoreId, diskStoreName, false, true, 70, 8192000L, 1000, 300L, 8192,
            createFileArray("/export/disk0/gemfire/backup"), new int[0], 50, 75);
    final TestResultSender testResultSender = new TestResultSender();
    when(mockCache.getMyId()).thenReturn(mockMember);
    when(mockCache.findDiskStore(diskStoreName)).thenReturn(mockDiskStore);
    when(mockMember.getId()).thenReturn(memberId);
    when(mockMember.getName()).thenReturn(memberName);
    when(mockFunctionContext.getCache()).thenReturn(mockCache);
    when(mockFunctionContext.getArguments()).thenReturn(diskStoreName);
    when(mockFunctionContext.getResultSender()).thenReturn(testResultSender);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    function.execute(mockFunctionContext);
    String expected =
        "The number of disk directories with a specified size (0) does not match the number of disk directories (1)!";
    assertThatThrownBy(testResultSender::getResults).hasMessage(expected);
    verify(mockDiskStore, atLeastOnce()).getName();
    verify(mockDiskStore, atLeastOnce()).getDiskStoreUUID();
  }

  @Test
  public void testGetRegionDiskStoreName() {
    final String expectedDiskStoreName = "testDiskStore";
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getDiskStoreName()).thenReturn(expectedDiskStoreName);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.getDiskStoreName(mockRegion)).isEqualTo(expectedDiskStoreName);
  }

  @Test
  public void testGetRegionDiskStoreNameWhenUnspecified() {
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getDiskStoreName()).thenReturn(null);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.getDiskStoreName(mockRegion))
        .isEqualTo(DiskStoreDetails.DEFAULT_DISK_STORE_NAME);
  }

  @Test
  public void testIsRegionOverflowToDiskWhenEvictionActionIsLocalDestroy() {
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    final EvictionAttributes mockEvictionAttributes =
        mock(EvictionAttributes.class, "EvictionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getEvictionAttributes()).thenReturn(mockEvictionAttributes);
    when(mockEvictionAttributes.getAction()).thenReturn(EvictionAction.LOCAL_DESTROY);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isOverflowToDisk(mockRegion)).isFalse();
    verify(mockRegion, times(2)).getAttributes();
    verify(mockRegionAttributes, times(2)).getEvictionAttributes();
  }

  @Test
  public void testIsRegionOverflowToDiskWhenEvictionActionIsOverflowToDisk() {
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    final EvictionAttributes mockEvictionAttributes =
        mock(EvictionAttributes.class, "EvictionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getEvictionAttributes()).thenReturn(mockEvictionAttributes);
    when(mockEvictionAttributes.getAction()).thenReturn(EvictionAction.OVERFLOW_TO_DISK);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isOverflowToDisk(mockRegion)).isTrue();
    verify(mockRegion, times(2)).getAttributes();
    verify(mockRegionAttributes, times(2)).getEvictionAttributes();
  }

  @Test
  public void testIsRegionOverflowToDiskWithNullEvictionAttributes() {
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getEvictionAttributes()).thenReturn(null);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isOverflowToDisk(mockRegion)).isFalse();
  }

  @Test
  public void testIsRegionPersistentWhenDataPolicyIsPersistentPartition() {
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_PARTITION);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isPersistent(mockRegion)).isTrue();
  }

  @Test
  public void testIsRegionPersistentWhenDataPolicyIsPersistentReplicate() {
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_REPLICATE);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isPersistent(mockRegion)).isTrue();
  }

  @Test
  public void testIsRegionPersistentWhenDataPolicyIsNormal() {
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.NORMAL);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isPersistent(mockRegion)).isFalse();
  }

  @Test
  public void testIsRegionPersistentWhenDataPolicyIsPartition() {
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.PARTITION);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isPersistent(mockRegion)).isFalse();
  }

  @Test
  public void testIsRegionPersistentWhenDataPolicyIsPreloaded() {
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.PRELOADED);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isPersistent(mockRegion)).isFalse();
  }

  @Test
  public void testIsRegionPersistentWhenDataPolicyIsReplicate() {
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isPersistent(mockRegion)).isFalse();
  }

  @Test
  public void testIsRegionUsingDiskStoreWhenUsingDefaultDiskStore() {
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_REPLICATE);
    when(mockRegionAttributes.getDiskStoreName()).thenReturn(null);
    when(mockDiskStore.getName()).thenReturn(DiskStoreDetails.DEFAULT_DISK_STORE_NAME);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isUsingDiskStore(mockRegion, mockDiskStore)).isTrue();
    verify(mockRegion, atLeastOnce()).getAttributes();
  }

  @Test
  public void testIsRegionUsingDiskStoreWhenPersistent() {
    final String diskStoreName = "testDiskStore";
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_PARTITION);
    when(mockRegionAttributes.getDiskStoreName()).thenReturn(diskStoreName);
    when(mockDiskStore.getName()).thenReturn(diskStoreName);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isUsingDiskStore(mockRegion, mockDiskStore)).isTrue();
    verify(mockRegion, atLeastOnce()).getAttributes();
  }

  @Test
  public void testIsRegionUsingDiskStoreWhenOverflowing() {
    final String diskStoreName = "testDiskStore";
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    final EvictionAttributes mockEvictionAttributes =
        mock(EvictionAttributes.class, "EvictionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.PARTITION);
    when(mockRegionAttributes.getDiskStoreName()).thenReturn(diskStoreName);
    when(mockRegionAttributes.getEvictionAttributes()).thenReturn(mockEvictionAttributes);
    when(mockEvictionAttributes.getAction()).thenReturn(EvictionAction.OVERFLOW_TO_DISK);
    when(mockDiskStore.getName()).thenReturn(diskStoreName);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isUsingDiskStore(mockRegion, mockDiskStore)).isTrue();
    verify(mockRegion, times(4)).getAttributes();
    verify(mockRegionAttributes, times(2)).getEvictionAttributes();
  }

  @Test
  public void testIsRegionUsingDiskStoreWhenDiskStoresMismatch() {
    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRegion = mock(Region.class, "Region");
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockRegionAttributes =
        mock(RegionAttributes.class, "RegionAttributes");
    when(mockRegion.getAttributes()).thenReturn(mockRegionAttributes);
    when(mockRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_PARTITION);
    when(mockRegionAttributes.getDiskStoreName()).thenReturn("mockDiskStore");
    when(mockDiskStore.getName()).thenReturn("testDiskStore");

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isUsingDiskStore(mockRegion, mockDiskStore)).isFalse();
  }

  @Test
  public void testSetRegionDetails() {
    // Prepare Mocks
    final String diskStoreName = "companyDiskStore";

    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockCompanyRegion = mock(Region.class, "/CompanyRegion");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockCompanyRegionAttributes =
        mock(RegionAttributes.class, "CompanyRegionAttributes");
    final EvictionAttributes mockCompanyEvictionAttributes =
        mock(EvictionAttributes.class, "CompanyEvictionAttributes");
    when(mockCompanyRegion.getAttributes()).thenReturn(mockCompanyRegionAttributes);
    when(mockCompanyRegion.getFullPath()).thenReturn("/CompanyRegion");
    when(mockCompanyRegion.getName()).thenReturn("CompanyRegion");
    when(mockCompanyRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_PARTITION);
    when(mockCompanyRegionAttributes.getDiskStoreName()).thenReturn(diskStoreName);
    when(mockCompanyRegionAttributes.getEvictionAttributes())
        .thenReturn(mockCompanyEvictionAttributes);
    when(mockCompanyEvictionAttributes.getAction()).thenReturn(EvictionAction.LOCAL_DESTROY);

    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockEmployeeRegion =
        mock(Region.class, "/CompanyRegion/EmployeeRegion");
    when(mockEmployeeRegion.getAttributes()).thenReturn(mockCompanyRegionAttributes);
    when(mockEmployeeRegion.getFullPath()).thenReturn("/CompanyRegion/EmployeeRegion");
    when(mockEmployeeRegion.getName()).thenReturn("EmployeeRegion");

    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockProductsRegion =
        mock(Region.class, "/CompanyRegion/ProductsRegion");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockProductsServicesRegionAttributes =
        mock(RegionAttributes.class, "ProductsServicesRegionAttributes");
    when(mockProductsRegion.getAttributes()).thenReturn(mockProductsServicesRegionAttributes);
    when(mockProductsRegion.subregions(false)).thenReturn(Collections.emptySet());
    when(mockProductsServicesRegionAttributes.getDataPolicy())
        .thenReturn(DataPolicy.PERSISTENT_REPLICATE);
    when(mockProductsServicesRegionAttributes.getDiskStoreName())
        .thenReturn("productsServicesDiskStore");

    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockServicesRegion =
        mock(Region.class, "/CompanyRegion/ServicesRegion");
    when(mockServicesRegion.getAttributes()).thenReturn(mockProductsServicesRegionAttributes);
    when(mockServicesRegion.subregions(false)).thenReturn(Collections.emptySet());

    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockContractorsRegion =
        mock(Region.class, "/CompanyRegion/ContractorsRegion");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockContractorsRegionAttributes =
        mock(RegionAttributes.class, "ContractorsRegionAttributes");
    final EvictionAttributes mockContractorsEvictionAttributes =
        mock(EvictionAttributes.class, "ContractorsEvictionAttributes");
    when(mockContractorsRegion.getAttributes()).thenReturn(mockContractorsRegionAttributes);
    when(mockContractorsRegion.getFullPath()).thenReturn("/CompanyRegion/ContractorsRegion");
    when(mockContractorsRegion.getName()).thenReturn("ContractorsRegion");
    when(mockContractorsRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
    when(mockContractorsRegionAttributes.getDiskStoreName()).thenReturn(diskStoreName);
    when(mockContractorsRegionAttributes.getEvictionAttributes())
        .thenReturn(mockContractorsEvictionAttributes);
    when(mockContractorsEvictionAttributes.getAction()).thenReturn(EvictionAction.OVERFLOW_TO_DISK);

    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockRolesRegion =
        mock(Region.class, "/CompanyRegion/EmployeeRegion/RolesRegion");
    when(mockRolesRegion.getAttributes()).thenReturn(mockCompanyRegionAttributes);
    when(mockRolesRegion.getFullPath()).thenReturn("/CompanyRegion/EmployeeRegion/RolesRegion");
    when(mockRolesRegion.getName()).thenReturn("RolesRegion");
    when(mockRolesRegion.subregions(false)).thenReturn(Collections.emptySet());

    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockPartnersRegion = mock(Region.class, "/PartnersRegion");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockPartnersRegionAttributes =
        mock(RegionAttributes.class, "PartnersRegionAttributes");
    when(mockPartnersRegion.getAttributes()).thenReturn(mockPartnersRegionAttributes);
    when(mockPartnersRegion.subregions(false)).thenReturn(Collections.emptySet());
    when(mockPartnersRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.PERSISTENT_PARTITION);
    when(mockPartnersRegionAttributes.getDiskStoreName()).thenReturn("");

    @SuppressWarnings("unchecked")
    final Region<Object, Object> mockCustomersRegion = mock(Region.class, "/CustomersRegion");
    @SuppressWarnings("unchecked")
    final RegionAttributes<Object, Object> mockCustomersRegionAttributes =
        mock(RegionAttributes.class, "CustomersRegionAttributes");
    final EvictionAttributes mockCustomersEvictionAttributes =
        mock(EvictionAttributes.class, "CustomersEvictionAttributes");
    when(mockCustomersRegion.getAttributes()).thenReturn(mockCustomersRegionAttributes);
    when(mockCustomersRegion.subregions(false)).thenReturn(Collections.emptySet());
    when(mockCustomersRegionAttributes.getDataPolicy()).thenReturn(DataPolicy.REPLICATE);
    when(mockCustomersRegionAttributes.getDiskStoreName()).thenReturn(null);
    when(mockCustomersRegionAttributes.getEvictionAttributes())
        .thenReturn(mockCustomersEvictionAttributes);
    when(mockCustomersEvictionAttributes.getAction()).thenReturn(EvictionAction.OVERFLOW_TO_DISK);

    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    when(mockDiskStore.getName()).thenReturn(diskStoreName);

    Set<Region<?, ?>> mockRootRegions = new HashSet<>();
    mockRootRegions.add(mockCompanyRegion);
    mockRootRegions.add(mockPartnersRegion);
    mockRootRegions.add(mockCustomersRegion);
    when(mockCache.rootRegions()).thenReturn(mockRootRegions);
    when(mockCompanyRegion.subregions(false)).thenReturn(CollectionUtils
        .asSet(mockContractorsRegion, mockEmployeeRegion, mockProductsRegion, mockServicesRegion));
    when(mockEmployeeRegion.subregions(false)).thenReturn(CollectionUtils.asSet(mockRolesRegion));
    when(mockContractorsRegion.subregions(false)).thenReturn(Collections.emptySet());

    // Execute Region and assert results
    final Set<DiskStoreDetails.RegionDetails> expectedRegionDetails = CollectionUtils.asSet(
        createRegionDetails("/CompanyRegion", "CompanyRegion", true, false),
        createRegionDetails("/CompanyRegion/EmployeeRegion", "EmployeeRegion", true, false),
        createRegionDetails("/CompanyRegion/EmployeeRegion/RolesRegion", "RolesRegion", true,
            false),
        createRegionDetails("/CompanyRegion/ContractorsRegion", "ContractorsRegion", false, true));
    final DiskStoreDetails diskStoreDetails = new DiskStoreDetails(diskStoreName, "memberOne");
    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    function.setRegionDetails(mockCache, mockDiskStore, diskStoreDetails);
    assertRegionDetails(expectedRegionDetails, diskStoreDetails);
    verify(mockCompanyRegion, times(5)).getAttributes();
    verify(mockEmployeeRegion, times(5)).getAttributes();
    verify(mockRolesRegion, times(5)).getAttributes();
    verify(mockCompanyRegionAttributes, times(6)).getDataPolicy();
    verify(mockCompanyRegionAttributes, times(3)).getDiskStoreName();
    verify(mockCompanyRegionAttributes, times(6)).getEvictionAttributes();
    verify(mockCompanyEvictionAttributes, times(3)).getAction();
    verify(mockContractorsRegion, times(7)).getAttributes();
    verify(mockContractorsRegionAttributes, times(2)).getDataPolicy();
    verify(mockContractorsRegionAttributes, times(4)).getEvictionAttributes();
    verify(mockContractorsEvictionAttributes, times(2)).getAction();
    verify(mockProductsRegion, times(2)).getAttributes();
    verify(mockServicesRegion, times(2)).getAttributes();
    verify(mockProductsServicesRegionAttributes, times(2)).getDataPolicy();
    verify(mockProductsServicesRegionAttributes, times(2)).getDiskStoreName();
    verify(mockPartnersRegion, times(2)).getAttributes();
    verify(mockCustomersRegion, times(4)).getAttributes();
    verify(mockCustomersRegionAttributes, times(2)).getEvictionAttributes();
    verify(mockDiskStore, atLeastOnce()).getName();
  }

  @Test
  public void testGetCacheServerDiskStoreName() {
    final String expectedDiskStoreName = "testDiskStore";
    final CacheServer mockCacheServer = mock(CacheServer.class, "CacheServer");
    final ClientSubscriptionConfig mockClientSubscriptionConfig =
        mock(ClientSubscriptionConfig.class, "ClientSubscriptionConfig");
    when(mockCacheServer.getClientSubscriptionConfig()).thenReturn(mockClientSubscriptionConfig);
    when(mockClientSubscriptionConfig.getDiskStoreName()).thenReturn(expectedDiskStoreName);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.getDiskStoreName(mockCacheServer)).isEqualTo(expectedDiskStoreName);
    verify(mockCacheServer, times(2)).getClientSubscriptionConfig();
  }

  @Test
  public void testGetCacheServerDiskStoreNameWhenUnspecified() {
    final CacheServer mockCacheServer = mock(CacheServer.class, "CacheServer");
    final ClientSubscriptionConfig mockClientSubscriptionConfig =
        mock(ClientSubscriptionConfig.class, "ClientSubscriptionConfig");
    when(mockCacheServer.getClientSubscriptionConfig()).thenReturn(mockClientSubscriptionConfig);
    when(mockClientSubscriptionConfig.getDiskStoreName()).thenReturn(null);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.getDiskStoreName(mockCacheServer))
        .isEqualTo(DiskStoreDetails.DEFAULT_DISK_STORE_NAME);
    verify(mockCacheServer, times(2)).getClientSubscriptionConfig();
  }

  @Test
  public void testGetCacheServerDiskStoreNameWithNullClientSubscriptionConfig() {
    final CacheServer mockCacheServer = mock(CacheServer.class, "CacheServer");
    when(mockCacheServer.getClientSubscriptionConfig()).thenReturn(null);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.getDiskStoreName(mockCacheServer)).isNull();
  }

  @Test
  public void testIsCacheServerUsingDiskStore() {
    final String diskStoreName = "testDiskStore";
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    final CacheServer mockCacheServer = mock(CacheServer.class, "CacheServer");
    final ClientSubscriptionConfig mockClientSubscriptionConfig =
        mock(ClientSubscriptionConfig.class, "ClientSubscriptionConfig");
    when(mockCacheServer.getClientSubscriptionConfig()).thenReturn(mockClientSubscriptionConfig);
    when(mockClientSubscriptionConfig.getDiskStoreName()).thenReturn(diskStoreName);
    when(mockDiskStore.getName()).thenReturn(diskStoreName);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isUsingDiskStore(mockCacheServer, mockDiskStore)).isTrue();
    verify(mockCacheServer, times(2)).getClientSubscriptionConfig();

  }

  @Test
  public void testIsCacheServerUsingDiskStoreWhenDiskStoresMismatch() {
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    final CacheServer mockCacheServer = mock(CacheServer.class, "CacheServer");
    final ClientSubscriptionConfig mockClientSubscriptionConfig =
        mock(ClientSubscriptionConfig.class, "ClientSubscriptionConfig");
    when(mockCacheServer.getClientSubscriptionConfig()).thenReturn(mockClientSubscriptionConfig);
    when(mockClientSubscriptionConfig.getDiskStoreName()).thenReturn(" ");
    when(mockDiskStore.getName()).thenReturn("otherDiskStore");

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isUsingDiskStore(mockCacheServer, mockDiskStore)).isFalse();
    verify(mockCacheServer, times(2)).getClientSubscriptionConfig();
  }

  @Test
  public void testIsCacheServerUsingDiskStoreWhenUsingDefaultDiskStore() {
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    final CacheServer mockCacheServer = mock(CacheServer.class, "CacheServer");
    final ClientSubscriptionConfig mockClientSubscriptionConfig =
        mock(ClientSubscriptionConfig.class, "ClientSubscriptionConfig");
    when(mockCacheServer.getClientSubscriptionConfig()).thenReturn(mockClientSubscriptionConfig);
    when(mockClientSubscriptionConfig.getDiskStoreName()).thenReturn("");
    when(mockDiskStore.getName()).thenReturn(DiskStoreDetails.DEFAULT_DISK_STORE_NAME);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isUsingDiskStore(mockCacheServer, mockDiskStore)).isTrue();
    verify(mockCacheServer, times(2)).getClientSubscriptionConfig();
  }

  @Test
  public void testSetCacheServerDetails() {
    final String diskStoreName = "testDiskStore";
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    final CacheServer mockCacheServer1 = mock(CacheServer.class, "CacheServer1");
    final CacheServer mockCacheServer2 = mock(CacheServer.class, "CacheServer2");
    final CacheServer mockCacheServer3 = mock(CacheServer.class, "CacheServer3");
    final ClientSubscriptionConfig mockCacheServer1ClientSubscriptionConfig =
        mock(ClientSubscriptionConfig.class, "cacheServer1ClientSubscriptionConfig");
    final ClientSubscriptionConfig mockCacheServer2ClientSubscriptionConfig =
        mock(ClientSubscriptionConfig.class, "cacheServer2ClientSubscriptionConfig");
    when(mockCache.getCacheServers())
        .thenReturn(Arrays.asList(mockCacheServer1, mockCacheServer2, mockCacheServer3));
    when(mockCacheServer1.getClientSubscriptionConfig())
        .thenReturn(mockCacheServer1ClientSubscriptionConfig);
    when(mockCacheServer1ClientSubscriptionConfig.getDiskStoreName()).thenReturn(diskStoreName);
    when(mockCacheServer1.getBindAddress()).thenReturn("10.127.255.1");
    when(mockCacheServer1.getPort()).thenReturn(65536);
    when(mockCacheServer1.getHostnameForClients()).thenReturn("gemini");
    when(mockCacheServer2.getClientSubscriptionConfig())
        .thenReturn(mockCacheServer2ClientSubscriptionConfig);
    when(mockCacheServer2ClientSubscriptionConfig.getDiskStoreName()).thenReturn("  ");
    when(mockCacheServer3.getClientSubscriptionConfig()).thenReturn(null);
    when(mockDiskStore.getName()).thenReturn(diskStoreName);

    final Set<DiskStoreDetails.CacheServerDetails> expectedCacheServerDetails =
        CollectionUtils.asSet(createCacheServerDetails("10.127.255.1", 65536, "gemini"));
    final DiskStoreDetails diskStoreDetails = new DiskStoreDetails(diskStoreName, "memberOne");
    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    function.setCacheServerDetails(mockCache, mockDiskStore, diskStoreDetails);
    assertCacheServerDetails(expectedCacheServerDetails, diskStoreDetails);
    verify(mockCacheServer1, times(2)).getClientSubscriptionConfig();
    verify(mockCacheServer2, times(2)).getClientSubscriptionConfig();
    verify(mockDiskStore, times(3)).getName();
  }

  @Test
  public void testGetGatewaySenderDiskStoreName() {
    final String expectedDiskStoreName = "testDiskStore";
    final GatewaySender mockGatewaySender = mock(GatewaySender.class, "GatewaySender");
    when(mockGatewaySender.getDiskStoreName()).thenReturn(expectedDiskStoreName);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.getDiskStoreName(mockGatewaySender)).isEqualTo(expectedDiskStoreName);
  }

  @Test
  public void testGetGatewaySenderDiskStoreNameWhenUnspecified() {
    final GatewaySender mockGatewaySender = mock(GatewaySender.class, "GatewaySender");
    when(mockGatewaySender.getDiskStoreName()).thenReturn(" ");

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.getDiskStoreName(mockGatewaySender))
        .isEqualTo(DiskStoreDetails.DEFAULT_DISK_STORE_NAME);
  }

  @Test
  public void testIsGatewaySenderPersistent() {
    final GatewaySender mockGatewaySender = mock(GatewaySender.class, "GatewaySender");
    when(mockGatewaySender.isPersistenceEnabled()).thenReturn(true);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isPersistent(mockGatewaySender)).isTrue();
  }

  @Test
  public void testIsGatewaySenderPersistentWhenPersistenceIsNotEnabled() {
    final GatewaySender mockGatewaySender = mock(GatewaySender.class, "GatewaySender");
    when(mockGatewaySender.isPersistenceEnabled()).thenReturn(true);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isPersistent(mockGatewaySender)).isTrue();
  }

  @Test
  public void testIsGatewaySenderUsingDiskStore() {
    final String diskStoreName = "testDiskStore";
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    final GatewaySender mockGatewaySender = mock(GatewaySender.class, "GatewaySender");
    when(mockGatewaySender.getDiskStoreName()).thenReturn(diskStoreName);
    when(mockDiskStore.getName()).thenReturn(diskStoreName);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isUsingDiskStore(mockGatewaySender, mockDiskStore)).isTrue();
  }

  @Test
  public void testIsGatewaySenderUsingDiskStoreWhenDiskStoresMismatch() {
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    final GatewaySender mockGatewaySender = mock(GatewaySender.class, "GatewaySender");
    when(mockGatewaySender.getDiskStoreName()).thenReturn("mockDiskStore");
    when(mockDiskStore.getName()).thenReturn("testDiskStore");

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isUsingDiskStore(mockGatewaySender, mockDiskStore)).isFalse();
  }

  @Test
  public void testIsGatewaySenderUsingDiskStoreWhenUsingDefaultDiskStores() {
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    final GatewaySender mockGatewaySender = mock(GatewaySender.class, "GatewaySender");
    when(mockGatewaySender.getDiskStoreName()).thenReturn(" ");
    when(mockDiskStore.getName()).thenReturn(DiskStoreDetails.DEFAULT_DISK_STORE_NAME);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isUsingDiskStore(mockGatewaySender, mockDiskStore)).isTrue();
  }

  @Test
  public void testSetPdxSerializationDetails() {
    final String diskStoreName = "testDiskStore";
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    when(mockCache.getPdxPersistent()).thenReturn(true);
    when(mockCache.getPdxDiskStore()).thenReturn(diskStoreName);
    when(mockDiskStore.getName()).thenReturn(diskStoreName);

    final DiskStoreDetails diskStoreDetails = new DiskStoreDetails(diskStoreName, "memberOne");
    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    function.setPdxSerializationDetails(mockCache, mockDiskStore, diskStoreDetails);
    assertThat(diskStoreDetails.isPdxSerializationMetaDataStored()).isTrue();
  }

  @Test
  public void testSetPdxSerializationDetailsWhenDiskStoreMismatch() {
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    when(mockCache.getPdxPersistent()).thenReturn(true);
    when(mockCache.getPdxDiskStore()).thenReturn("mockDiskStore");
    when(mockDiskStore.getName()).thenReturn("testDiskStore");

    final DiskStoreDetails diskStoreDetails = new DiskStoreDetails("testDiskStore", "memberOne");
    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    function.setPdxSerializationDetails(mockCache, mockDiskStore, diskStoreDetails);
    assertThat(diskStoreDetails.isPdxSerializationMetaDataStored()).isFalse();
  }

  @Test
  public void testSetPdxSerializationDetailsWhenPdxIsNotPersistent() {
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    when(mockCache.getPdxPersistent()).thenReturn(false);

    final DiskStoreDetails diskStoreDetails = new DiskStoreDetails("testDiskStore", "memberOne");
    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    function.setPdxSerializationDetails(mockCache, mockDiskStore, diskStoreDetails);
    assertThat(diskStoreDetails.isPdxSerializationMetaDataStored()).isFalse();
  }

  @Test
  public void testGetAsyncEventQueueDiskStoreName() {
    final String expectedDiskStoreName = "testDiskStore";
    final AsyncEventQueue mockQueue = mock(AsyncEventQueue.class, "AsyncEventQueue");
    when(mockQueue.getDiskStoreName()).thenReturn(expectedDiskStoreName);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.getDiskStoreName(mockQueue)).isEqualTo(expectedDiskStoreName);
  }

  @Test
  public void testGetAsyncEventQueueDiskStoreNameUsingDefaultDiskStore() {
    final AsyncEventQueue mockQueue = mock(AsyncEventQueue.class, "AsyncEventQueue");
    when(mockQueue.getDiskStoreName()).thenReturn(null);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.getDiskStoreName(mockQueue))
        .isEqualTo(DiskStoreDetails.DEFAULT_DISK_STORE_NAME);
  }

  @Test
  public void testIsAsyncEventQueueUsingDiskStore() {
    final String diskStoreName = "testDiskStore";
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    final AsyncEventQueue mockQueue = mock(AsyncEventQueue.class, "AsyncEventQueue");
    when(mockQueue.getDiskStoreName()).thenReturn(diskStoreName);
    when(mockQueue.isPersistent()).thenReturn(true);
    when(mockDiskStore.getName()).thenReturn(diskStoreName);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isUsingDiskStore(mockQueue, mockDiskStore)).isTrue();
  }

  @Test
  public void testIsAsyncEventQueueUsingDiskStoreWhenDiskStoresMismatch() {
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    final AsyncEventQueue mockQueue = mock(AsyncEventQueue.class, "AsyncEventQueue");
    when(mockQueue.getDiskStoreName()).thenReturn("mockDiskStore");
    when(mockQueue.isPersistent()).thenReturn(true);
    when(mockDiskStore.getName()).thenReturn(DiskStoreDetails.DEFAULT_DISK_STORE_NAME);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isUsingDiskStore(mockQueue, mockDiskStore)).isFalse();
  }

  @Test
  public void testIsAsyncEventQueueUsingDiskStoreWhenQueueIsNotPersistent() {
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    final AsyncEventQueue mockQueue = mock(AsyncEventQueue.class, "AsyncEventQueue");
    when(mockQueue.isPersistent()).thenReturn(false);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isUsingDiskStore(mockQueue, mockDiskStore)).isFalse();
  }

  @Test
  public void testIsAsyncEventQueueUsingDiskStoreWhenUsingDefaultDiskStore() {
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    final AsyncEventQueue mockQueue = mock(AsyncEventQueue.class, "AsyncEventQueue");
    when(mockQueue.getDiskStoreName()).thenReturn(" ");
    when(mockQueue.isPersistent()).thenReturn(true);
    when(mockDiskStore.getName()).thenReturn(DiskStoreDetails.DEFAULT_DISK_STORE_NAME);

    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    assertThat(function.isUsingDiskStore(mockQueue, mockDiskStore)).isTrue();
  }

  @Test
  public void testSetAsyncEventQueueDetails() {
    final String diskStoreName = "testDiskStore";
    final DiskStore mockDiskStore = mock(DiskStore.class, "DiskStore");
    final AsyncEventQueue mockQueue1 = mock(AsyncEventQueue.class, "AsyncEvenQueue1");
    final AsyncEventQueue mockQueue2 = mock(AsyncEventQueue.class, "AsyncEvenQueue2");
    final AsyncEventQueue mockQueue3 = mock(AsyncEventQueue.class, "AsyncEvenQueue3");
    when(mockCache.getAsyncEventQueues())
        .thenReturn(CollectionUtils.asSet(mockQueue1, mockQueue2, mockQueue3));
    when(mockQueue1.isPersistent()).thenReturn(true);
    when(mockQueue1.getDiskStoreName()).thenReturn(diskStoreName);
    when(mockQueue1.getId()).thenReturn("q1");
    when(mockQueue2.isPersistent()).thenReturn(true);
    when(mockQueue2.getDiskStoreName()).thenReturn(null);
    when(mockQueue3.isPersistent()).thenReturn(false);
    when(mockDiskStore.getName()).thenReturn(diskStoreName);

    final Set<DiskStoreDetails.AsyncEventQueueDetails> expectedAsyncEventQueueDetails =
        CollectionUtils.asSet(createAsyncEventQueueDetails("q1"));
    final DiskStoreDetails diskStoreDetails = new DiskStoreDetails(diskStoreName, "memberOne");
    final DescribeDiskStoreFunction function = new DescribeDiskStoreFunction();
    function.setAsyncEventQueueDetails(mockCache, mockDiskStore, diskStoreDetails);
    assertAsyncEventQueueDetails(expectedAsyncEventQueueDetails, diskStoreDetails);
    verify(mockDiskStore, atLeastOnce()).getName();
  }

  private static class TestResultSender implements ResultSender<Object> {
    private Throwable t;
    private final List<Object> results = new LinkedList<>();

    protected List<Object> getResults() throws Throwable {
      if (t != null) {
        throw t;
      }

      return Collections.unmodifiableList(results);
    }

    @Override
    public void lastResult(final Object lastResult) {
      results.add(lastResult);
    }

    @Override
    public void sendResult(final Object oneResult) {
      results.add(oneResult);
    }

    @Override
    public void sendException(final Throwable t) {
      this.t = t;
    }
  }
}
