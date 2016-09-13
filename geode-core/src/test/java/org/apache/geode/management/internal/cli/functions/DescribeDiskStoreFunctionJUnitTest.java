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
package com.gemstone.gemfire.management.internal.cli.functions;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.Logger;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.server.ClientSubscriptionConfig;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.internal.lang.Filter;
import com.gemstone.gemfire.internal.lang.ObjectUtils;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.util.CollectionUtils;
import com.gemstone.gemfire.management.internal.cli.domain.DiskStoreDetails;
import com.gemstone.gemfire.management.internal.cli.util.DiskStoreNotFoundException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The DescribeDiskStoreFunctionJUnitTest test suite class tests the contract and functionality of the
 * DescribeDiskStoreFunction class.
 *
 * @see com.gemstone.gemfire.cache.DiskStore
 * @see com.gemstone.gemfire.management.internal.cli.domain.DiskStoreDetails
 * @see com.gemstone.gemfire.management.internal.cli.functions.DescribeDiskStoreFunction
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@SuppressWarnings({ "null", "unused" })
@Category(UnitTest.class)
public class DescribeDiskStoreFunctionJUnitTest {

  private static final Logger logger = LogService.getLogger();
  
  private Mockery mockContext;

  @Before
  public void setup() {
    mockContext = new Mockery();
    mockContext.setImposteriser(ClassImposteriser.INSTANCE);
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  private void assertAsyncEventQueueDetails(final Set<DiskStoreDetails.AsyncEventQueueDetails> expectedAsyncEventQueueDetailsSet, final DiskStoreDetails diskStoreDetails) {
    int actualCount = 0;

    for (final DiskStoreDetails.AsyncEventQueueDetails actualAsyncEventQueueDetails : diskStoreDetails.iterateAsyncEventQueues()) {
      final DiskStoreDetails.AsyncEventQueueDetails expectedAsyncEventQueueDetails = CollectionUtils.findBy(expectedAsyncEventQueueDetailsSet,
        new Filter<DiskStoreDetails.AsyncEventQueueDetails>() {
          @Override public boolean accept(final DiskStoreDetails.AsyncEventQueueDetails asyncEventQueueDetails) {
            return ObjectUtils.equals(asyncEventQueueDetails.getId(), actualAsyncEventQueueDetails.getId());
          }
      });

      assertNotNull(expectedAsyncEventQueueDetails);
      actualCount++;
    }
    assertEquals(expectedAsyncEventQueueDetailsSet.size(), actualCount);
  }

  private void assertCacheServerDetails(final Set<DiskStoreDetails.CacheServerDetails> expectedCacheServerDetailsSet, final DiskStoreDetails diskStoreDetails) {
    int actualCount = 0;

    for (final DiskStoreDetails.CacheServerDetails actualCacheServerDetails : diskStoreDetails.iterateCacheServers()) {
      final DiskStoreDetails.CacheServerDetails expectedCacheServerDetails = CollectionUtils.findBy(expectedCacheServerDetailsSet,
        new Filter<DiskStoreDetails.CacheServerDetails>() {
          public boolean accept(final DiskStoreDetails.CacheServerDetails cacheServerDetails) {
            return ObjectUtils.equals(cacheServerDetails.getBindAddress(), actualCacheServerDetails.getBindAddress())
              && ObjectUtils.equals(cacheServerDetails.getPort(), actualCacheServerDetails.getPort());
          }
      });

      assertNotNull(expectedCacheServerDetails);
      assertEquals(expectedCacheServerDetails.getHostName(), actualCacheServerDetails.getHostName());
      actualCount++;
    }

    assertEquals(expectedCacheServerDetailsSet.size(), actualCount);
  }

  private void assertGatewayDetails(final Set<DiskStoreDetails.GatewayDetails> expectedGatewayDetailsSet, final DiskStoreDetails diskStoreDetails) {
    int actualCount = 0;

    for (final DiskStoreDetails.GatewayDetails actualGatewayDetails : diskStoreDetails.iterateGateways()) {
      DiskStoreDetails.GatewayDetails expectedGatewayDetails = CollectionUtils.findBy(expectedGatewayDetailsSet,
        new Filter<DiskStoreDetails.GatewayDetails>() {
          public boolean accept(final DiskStoreDetails.GatewayDetails gatewayDetails) {
            return ObjectUtils.equals(gatewayDetails.getId(), actualGatewayDetails.getId());
          }
      });

      assertNotNull(expectedGatewayDetails);
      assertEquals(expectedGatewayDetails.isPersistent(), actualGatewayDetails.isPersistent());
      actualCount++;
    }

    assertEquals(expectedGatewayDetailsSet.size(), actualCount);
  }

  private void assertRegionDetails(final Set<DiskStoreDetails.RegionDetails> expectedRegionDetailsSet, final DiskStoreDetails diskStoreDetails) {
    int actualCount = 0;

    for (final DiskStoreDetails.RegionDetails actualRegionDetails : diskStoreDetails.iterateRegions()) {
      final DiskStoreDetails.RegionDetails expectedRegionDetails = CollectionUtils.findBy(expectedRegionDetailsSet,
        new Filter<DiskStoreDetails.RegionDetails>() {
          public boolean accept(final DiskStoreDetails.RegionDetails regionDetails) {
            return ObjectUtils.equals(regionDetails.getFullPath(), actualRegionDetails.getFullPath());
          }
      });

      assertNotNull(expectedRegionDetails);
      assertEquals(expectedRegionDetails.getName(), actualRegionDetails.getName());
      assertEquals(expectedRegionDetails.isOverflowToDisk(), actualRegionDetails.isOverflowToDisk());
      assertEquals(expectedRegionDetails.isPersistent(), actualRegionDetails.isPersistent());
      actualCount++;
    }

    assertEquals(expectedRegionDetailsSet.size(), actualCount);
  }

  private DiskStoreDetails.AsyncEventQueueDetails createAsyncEventQueueDetails(final String id) {
    return new DiskStoreDetails.AsyncEventQueueDetails(id);
  }

  private DiskStoreDetails.CacheServerDetails createCacheServerDetails(final String bindAddress,
                                                                         final int port,
                                                                         final String hostname)
  {
    final DiskStoreDetails.CacheServerDetails cacheServerDetails = new DiskStoreDetails.CacheServerDetails(bindAddress, port);
    cacheServerDetails.setHostName(hostname);
    return cacheServerDetails;
  }

  private DescribeDiskStoreFunction createDescribeDiskStoreFunction(final Cache cache) {
    return new TestDescribeDiskStoreFunction(cache);
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

  private DiskStoreDetails.GatewayDetails createGatewayDetails(final String id, final boolean persistent) {
    DiskStoreDetails.GatewayDetails gatewayDetails = new DiskStoreDetails.GatewayDetails(id);
    gatewayDetails.setPersistent(persistent);
    return gatewayDetails;
  }

  private int[] createIntArray(final int... array) {
    assert array != null : "The array of int values cannot be null!";
    return array;
  }

  private DiskStore createMockDiskStore(final UUID diskStoreId,
                                          final String name,
                                          final boolean allowForceCompaction,
                                          final boolean autoCompact,
                                          final int compactionThreshold,
                                          final long maxOplogSize,
                                          final int queueSize,
                                          final long timeInterval,
                                          final int writeBufferSize,
                                          final File[] diskDirs,
                                          final int[] diskDirSizes,
                                          final float warningPercentage,
                                          final float criticalPercentage)
  {
    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, name);

    mockContext.checking(new Expectations() {{
      oneOf(mockDiskStore).getAllowForceCompaction();
      will(returnValue(allowForceCompaction));
      oneOf(mockDiskStore).getAutoCompact();
      will(returnValue(autoCompact));
      oneOf(mockDiskStore).getCompactionThreshold();
      will(returnValue(compactionThreshold));
      atLeast(1).of(mockDiskStore).getDiskStoreUUID();
      will(returnValue(diskStoreId));
      oneOf(mockDiskStore).getMaxOplogSize();
      will(returnValue(maxOplogSize));
      atLeast(1).of(mockDiskStore).getName();
      will(returnValue(name));
      oneOf(mockDiskStore).getQueueSize();
      will(returnValue(queueSize));
      oneOf(mockDiskStore).getTimeInterval();
      will(returnValue(timeInterval));
      oneOf(mockDiskStore).getWriteBufferSize();
      will(returnValue(writeBufferSize));
      allowing(mockDiskStore).getDiskDirs();
      will(returnValue(diskDirs));
      allowing(mockDiskStore).getDiskDirSizes();
      will(returnValue(diskDirSizes));
      allowing(mockDiskStore).getDiskUsageWarningPercentage();
      will(returnValue(warningPercentage));
      allowing(mockDiskStore).getDiskUsageCriticalPercentage();
      will(returnValue(criticalPercentage));
    }});

    return mockDiskStore;
  }

  private DiskStoreDetails.RegionDetails createRegionDetails(final String fullPath,
                                                               final String name,
                                                               final boolean persistent,
                                                               final boolean overflow)
  {
    final DiskStoreDetails.RegionDetails regionDetails = new DiskStoreDetails.RegionDetails(fullPath, name);
    regionDetails.setPersistent(persistent);
    regionDetails.setOverflowToDisk(overflow);
    return regionDetails;
  }

  @Test
  public void testAssertState() {
    DescribeDiskStoreFunction.assertState(true, "null");
  }

  @Test(expected = IllegalStateException.class)
  public void testAssertStateThrowsIllegalStateException() {
    try {
      DescribeDiskStoreFunction.assertState(false, "Expected (%1$s) message!", "test");
    }
    catch (IllegalStateException e) {
      assertEquals("Expected (test) message!", e.getMessage());
      throw e;
    }
  }

  private void setupEmptyRegionsPdxGatewaysCacheServersAndAsyncEventQueues(final InternalCache mockCache) {
    mockContext.checking(new Expectations() {{
      oneOf(mockCache).rootRegions();
      will(returnValue(Collections.emptySet()));
      oneOf(mockCache).getCacheServers();
      will(returnValue(Collections.emptyList()));
      oneOf(mockCache).getGatewaySenders();
      will(returnValue(Collections.emptyList()));
      will(returnValue(Collections.emptyList()));
      oneOf(mockCache).getPdxPersistent();
      will(returnValue(false));
      oneOf(mockCache).getAsyncEventQueues();
      will(returnValue(Collections.emptySet()));
    }});
  }

  private Set<DiskStoreDetails.RegionDetails> setupRegionsForTestExecute(final InternalCache mockCache, final String diskStoreName) {
    final Region mockUserRegion = mockContext.mock(Region.class, "/UserRegion");
    final Region mockSessionRegion = mockContext.mock(Region.class, "/UserRegion/SessionRegion");
    final Region mockGuestRegion = mockContext.mock(Region.class, "/GuestRegion");

    final RegionAttributes mockUserRegionAttributes = mockContext.mock(RegionAttributes.class, "UserRegionAttributes");
    final RegionAttributes mockSessionRegionAttributes = mockContext.mock(RegionAttributes.class, "SessionRegionAttributes");
    final RegionAttributes mockGuestRegionAttributes = mockContext.mock(RegionAttributes.class, "GuestRegionAttributes");

    final EvictionAttributes mockUserEvictionAttributes = mockContext.mock(EvictionAttributes.class, "UserEvictionAttributes");
    final EvictionAttributes mockSessionEvictionAttributes = mockContext.mock(EvictionAttributes.class, "SessionEvictionAttributes");
    final EvictionAttributes mockGuestEvictionAttributes = mockContext.mock(EvictionAttributes.class, "GuestEvictionAttributes");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).rootRegions();
      will(returnValue(CollectionUtils.asSet(mockUserRegion, mockGuestRegion)));
      exactly(5).of(mockUserRegion).getAttributes();
      will(returnValue(mockUserRegionAttributes));
      oneOf(mockUserRegion).getFullPath();
      will(returnValue("/UserRegion"));
      oneOf(mockUserRegion).getName();
      will(returnValue("UserRegion"));
      oneOf(mockUserRegion).subregions(false);
      will(returnValue(CollectionUtils.asSet(mockSessionRegion)));
      exactly(2).of(mockUserRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.PERSISTENT_PARTITION));
      oneOf(mockUserRegionAttributes).getDiskStoreName();
      will(returnValue(diskStoreName));
      exactly(2).of(mockUserRegionAttributes).getEvictionAttributes();
      will(returnValue(mockUserEvictionAttributes));
      oneOf(mockUserEvictionAttributes).getAction();
      will(returnValue(EvictionAction.LOCAL_DESTROY));
      exactly(7).of(mockSessionRegion).getAttributes();
      will(returnValue(mockSessionRegionAttributes));
      oneOf(mockSessionRegion).getFullPath();
      will(returnValue("/UserRegion/SessionRegion"));
      oneOf(mockSessionRegion).getName();
      will(returnValue("SessionRegion"));
      oneOf(mockSessionRegion).subregions(false);
      will(returnValue(Collections.emptySet()));
      exactly(2).of(mockSessionRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.REPLICATE));
      oneOf(mockSessionRegionAttributes).getDiskStoreName();
      will(returnValue(diskStoreName));
      exactly(4).of(mockSessionRegionAttributes).getEvictionAttributes();
      will(returnValue(mockSessionEvictionAttributes));
      exactly(2).of(mockSessionEvictionAttributes).getAction();
      will(returnValue(EvictionAction.OVERFLOW_TO_DISK));
      exactly(4).of(mockGuestRegion).getAttributes();
      will(returnValue(mockGuestRegionAttributes));
      oneOf(mockGuestRegion).subregions(false);
      will(returnValue(Collections.emptySet()));
      oneOf(mockGuestRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.REPLICATE));
      oneOf(mockGuestRegionAttributes).getDiskStoreName();
      will(returnValue(DiskStoreDetails.DEFAULT_DISK_STORE_NAME));
      exactly(2).of(mockGuestRegionAttributes).getEvictionAttributes();
      will(returnValue(mockGuestEvictionAttributes));
      oneOf(mockGuestEvictionAttributes).getAction();
      will(returnValue(EvictionAction.OVERFLOW_TO_DISK));
    }});

    return CollectionUtils.asSet(createRegionDetails("/UserRegion", "UserRegion", true, false),
      createRegionDetails("/UserRegion/SessionRegion", "SessionRegion", false, true));
  }

  private Set<DiskStoreDetails.GatewayDetails> setupGatewaysForTestExecute(final InternalCache mockCache, final String diskStoreName) {
    final GatewaySender mockGatewaySender = mockContext.mock(GatewaySender.class, "GatewaySender");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getGatewaySenders();
      will(returnValue(CollectionUtils.asSet(mockGatewaySender)));
      oneOf(mockGatewaySender).getDiskStoreName();
      will(returnValue(diskStoreName));
      oneOf(mockGatewaySender).getId();
      will(returnValue("0123456789"));
      oneOf(mockGatewaySender).isPersistenceEnabled();
      will(returnValue(true));
    }});

    return CollectionUtils.asSet(createGatewayDetails("0123456789", true));
  }

  private Set<DiskStoreDetails.CacheServerDetails> setupCacheServersForTestExecute(final InternalCache mockCache, final String diskStoreName) {
    final CacheServer mockCacheServer1 = mockContext.mock(CacheServer.class, "CacheServer1");
    final CacheServer mockCacheServer2 = mockContext.mock(CacheServer.class, "CacheServer2");
    final CacheServer mockCacheServer3 = mockContext.mock(CacheServer.class, "CacheServer3");

    final ClientSubscriptionConfig cacheServer1ClientSubscriptionConfig = mockContext.mock(ClientSubscriptionConfig.class,
      "cacheServer1ClientSubscriptionConfig");
    final ClientSubscriptionConfig cacheServer3ClientSubscriptionConfig = mockContext.mock(ClientSubscriptionConfig.class,
      "cacheServer3ClientSubscriptionConfig");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getCacheServers();
      will(returnValue(Arrays.asList(mockCacheServer1, mockCacheServer2, mockCacheServer3)));
      exactly(2).of(mockCacheServer1).getClientSubscriptionConfig();
      will(returnValue(cacheServer1ClientSubscriptionConfig));
      oneOf(cacheServer1ClientSubscriptionConfig).getDiskStoreName();
      will(returnValue(diskStoreName));
      oneOf(mockCacheServer2).getClientSubscriptionConfig();
      will(returnValue(null));
      exactly(2).of(mockCacheServer3).getClientSubscriptionConfig();
      will(returnValue(cacheServer3ClientSubscriptionConfig));
      oneOf(cacheServer3ClientSubscriptionConfig).getDiskStoreName();
      will(returnValue(""));
      oneOf(mockCacheServer1).getBindAddress();
      will(returnValue("10.127.0.1"));
      oneOf(mockCacheServer1).getPort();
      will(returnValue(10123));
      oneOf(mockCacheServer1).getHostnameForClients();
      will(returnValue("rodan"));
    }});

    return CollectionUtils.asSet(createCacheServerDetails("10.127.0.1", 10123, "rodan"));
  }

  private Set<DiskStoreDetails.AsyncEventQueueDetails> setupAsyncEventQueuesForTestExecute(final InternalCache mockCache, final String diskStoreName) {
    final AsyncEventQueue mockAsyncEventQueue1 = mockContext.mock(AsyncEventQueue.class, "AsyncEventQueue1");
    final AsyncEventQueue mockAsyncEventQueue2 = mockContext.mock(AsyncEventQueue.class, "AsyncEventQueue2");
    final AsyncEventQueue mockAsyncEventQueue3 = mockContext.mock(AsyncEventQueue.class, "AsyncEventQueue3");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getAsyncEventQueues();
      will(returnValue(CollectionUtils.asSet(mockAsyncEventQueue1, mockAsyncEventQueue2, mockAsyncEventQueue3)));
      oneOf(mockAsyncEventQueue1).isPersistent();
      will(returnValue(true));
      oneOf(mockAsyncEventQueue1).getDiskStoreName();
      will(returnValue(diskStoreName));
      oneOf(mockAsyncEventQueue1).getId();
      will(returnValue("9876543210"));
      oneOf(mockAsyncEventQueue2).isPersistent();
      will(returnValue(false));
      oneOf(mockAsyncEventQueue3).isPersistent();
      will(returnValue(true));
      oneOf(mockAsyncEventQueue3).getDiskStoreName();
      will(returnValue("memSto"));
    }});

    return CollectionUtils.asSet(createAsyncEventQueueDetails("9876543210"));
  }

  @Test
  public void testExecute() throws Throwable {
    final UUID diskStoreId = UUID.randomUUID();

    final String diskStoreName = "mockDiskStore";
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";

    final InternalCache mockCache = mockContext.mock(InternalCache.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final DiskStore mockDiskStore = createMockDiskStore(diskStoreId, diskStoreName, true, false, 75, 8192l, 500, 120l, 10240,
      createFileArray("/export/disk/backup", "/export/disk/overflow", "/export/disk/persistence"),
        createIntArray(10240, 204800, 4096000), 50, 75);

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "testExecute$FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getMyId();
      will(returnValue(mockMember));
      oneOf(mockCache).findDiskStore(diskStoreName);
      will(returnValue(mockDiskStore));
      oneOf(mockCache).getPdxPersistent();
      will(returnValue(true));
      oneOf(mockCache).getPdxDiskStore();
      will(returnValue("memoryStore"));
      oneOf(mockMember).getId();
      will(returnValue(memberId));
      oneOf(mockMember).getName();
      will(returnValue(memberName));
      oneOf(mockFunctionContext).getArguments();
      will(returnValue(diskStoreName));
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final Set<DiskStoreDetails.RegionDetails> expectedRegionDetails = setupRegionsForTestExecute(mockCache, diskStoreName);

    final Set<DiskStoreDetails.GatewayDetails> expectedGatewayDetails = setupGatewaysForTestExecute(mockCache, diskStoreName);

    final Set<DiskStoreDetails.CacheServerDetails> expectedCacheServerDetails = setupCacheServersForTestExecute(mockCache, diskStoreName);

    final Set<DiskStoreDetails.AsyncEventQueueDetails> expectedAsyncEventQueueDetails = setupAsyncEventQueuesForTestExecute(mockCache, diskStoreName);

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(mockCache);

    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertEquals(1, results.size());

    final DiskStoreDetails diskStoreDetails = (DiskStoreDetails) results.get(0);

    assertNotNull(diskStoreDetails);
    assertEquals(diskStoreId, diskStoreDetails.getId());
    assertEquals(diskStoreName, diskStoreDetails.getName());
    assertEquals(memberId, diskStoreDetails.getMemberId());
    assertEquals(memberName, diskStoreDetails.getMemberName());
    assertTrue(diskStoreDetails.getAllowForceCompaction());
    assertFalse(diskStoreDetails.getAutoCompact());
    assertEquals(75, diskStoreDetails.getCompactionThreshold().intValue());
    assertEquals(8192l, diskStoreDetails.getMaxOplogSize().longValue());
    assertFalse(diskStoreDetails.isPdxSerializationMetaDataStored());
    assertEquals(500, diskStoreDetails.getQueueSize().intValue());
    assertEquals(120l, diskStoreDetails.getTimeInterval().longValue());
    assertEquals(10240, diskStoreDetails.getWriteBufferSize().intValue());
    assertEquals(50.0f, diskStoreDetails.getDiskUsageWarningPercentage().floatValue(), 0.0f);
    assertEquals(75.0f, diskStoreDetails.getDiskUsageCriticalPercentage().floatValue(), 0.0f);

    final List<String> expectedDiskDirs = Arrays.asList(
      new File("/export/disk/backup").getAbsolutePath(),
      new File("/export/disk/overflow").getAbsolutePath(),
      new File("/export/disk/persistence").getAbsolutePath()
    );

    final List<Integer> expectdDiskDirSizes = Arrays.asList(10240, 204800, 4096000);

    int count = 0;

    for (final DiskStoreDetails.DiskDirDetails diskDirDetails : diskStoreDetails) {
      assertTrue(expectedDiskDirs.contains(diskDirDetails.getAbsolutePath()));
      assertTrue(expectdDiskDirSizes.contains(diskDirDetails.getSize()));
      count++;
    }

    assertEquals(expectedDiskDirs.size(), count);
    assertRegionDetails(expectedRegionDetails, diskStoreDetails);
    assertCacheServerDetails(expectedCacheServerDetails, diskStoreDetails);
    assertGatewayDetails(expectedGatewayDetails, diskStoreDetails);
    assertAsyncEventQueueDetails(expectedAsyncEventQueueDetails, diskStoreDetails);
  }

  @Test
  public void testExecuteOnMemberHavingANonGemFireCache() throws Throwable {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      exactly(0).of(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(mockCache);

    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertTrue(results.isEmpty());
  }

  @Test(expected = DiskStoreNotFoundException.class)
  public void testExecuteThrowingDiskStoreNotFoundException() throws Throwable{
    final String diskStoreName = "testDiskStore";
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";

    final InternalCache mockCache = mockContext.mock(InternalCache.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getMyId();
      will(returnValue(mockMember));
      oneOf(mockCache).findDiskStore(diskStoreName);
      will(returnValue(null));
      oneOf(mockMember).getId();
      will(returnValue(memberId));
      oneOf(mockMember).getName();
      will(returnValue(memberName));
      oneOf(mockFunctionContext).getArguments();
      will(returnValue(diskStoreName));
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(mockCache);

    function.execute(mockFunctionContext);

    try {
      testResultSender.getResults();
    }
    catch (DiskStoreNotFoundException e) {
      assertEquals(String.format("A disk store with name (%1$s) was not found on member (%2$s).",
        diskStoreName, memberName), e.getMessage());
      throw e;
    }
  }

  @Test(expected = RuntimeException.class)
  public void testExecuteThrowingRuntimeException() throws Throwable {
    final String diskStoreName = "testDiskStore";
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";

    final InternalCache mockCache = mockContext.mock(InternalCache.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getMyId();
      will(returnValue(mockMember));
      oneOf(mockCache).findDiskStore(diskStoreName);
      will(throwException(new RuntimeException("ExpectedStrings")));
      oneOf(mockMember).getId();
      will(returnValue(memberId));
      oneOf(mockMember).getName();
      will(returnValue(memberName));
      oneOf(mockFunctionContext).getArguments();
      will(returnValue(diskStoreName));
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(mockCache);

    function.execute(mockFunctionContext);

    try {
      testResultSender.getResults();
    }
    catch (RuntimeException e) {
      assertEquals("ExpectedStrings", e.getMessage());
      throw e;
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testExecuteWithDiskDirsAndDiskSizesMismatch() throws Throwable {
    logger.info("<ExpectedException action=add>"+IllegalStateException.class.getName()+"</ExpectedException>");
    try {
      doTestExecuteWithDiskDirsAndDiskSizesMismatch();
    }
    finally {
      logger.info("<ExpectedException action=remove>"+IllegalStateException.class.getName()+"</ExpectedException>");
    }
  }
  
  private void doTestExecuteWithDiskDirsAndDiskSizesMismatch() throws Throwable { 
    final String diskStoreName = "mockDiskStore";
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";

    final UUID diskStoreId = UUID.randomUUID();

    final InternalCache mockCache = mockContext.mock(InternalCache.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final DiskStore mockDiskStore = createMockDiskStore(diskStoreId, diskStoreName, false, true, 70, 8192000l, 1000, 300l, 8192,
      createFileArray("/export/disk0/gemfire/backup"), new int[0], 50, 75);

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getMyId();
      will(returnValue(mockMember));
      oneOf(mockCache).findDiskStore(diskStoreName);
      will(returnValue(mockDiskStore));
      oneOf(mockMember).getId();
      will(returnValue(memberId));
      oneOf(mockMember).getName();
      will(returnValue(memberName));
      oneOf(mockFunctionContext).getArguments();
      will(returnValue(diskStoreName));
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(mockCache);

    function.execute(mockFunctionContext);

    try {
      testResultSender.getResults();
    }
    catch (IllegalStateException e) {
      assertEquals("The number of disk directories with a specified size (0) does not match the number of disk directories (1)!",
        e.getMessage());
      throw e;
    }
  }

  @Test
  public void testGetRegionDiskStoreName() {
    final String expectedDiskStoreName = "testDiskStore";

    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");

    mockContext.checking(new Expectations() {{
      oneOf(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      oneOf(mockRegionAttributes).getDiskStoreName();
      will(returnValue(expectedDiskStoreName));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertEquals(expectedDiskStoreName, function.getDiskStoreName(mockRegion));
  }

  @Test
  public void testGetRegionDiskStoreNameWhenUnspecified() {
    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");

    mockContext.checking(new Expectations() {{
      oneOf(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      oneOf(mockRegionAttributes).getDiskStoreName();
      will(returnValue(null));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertEquals(DiskStoreDetails.DEFAULT_DISK_STORE_NAME, function.getDiskStoreName(mockRegion));
  }

  @Test
  public void testIsRegionOverflowToDiskWhenEvictionActionIsLocalDestroy() {
    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");
    final EvictionAttributes mockEvictionAttributes = mockContext.mock(EvictionAttributes.class, "EvictionAttributes");

    mockContext.checking(new Expectations() {{
      exactly(2).of(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      exactly(2).of(mockRegionAttributes).getEvictionAttributes();
      will(returnValue(mockEvictionAttributes));
      oneOf(mockEvictionAttributes).getAction();
      will(returnValue(EvictionAction.LOCAL_DESTROY));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertFalse(function.isOverflowToDisk(mockRegion));
  }

  @Test
  public void testIsRegionOverflowToDiskWhenEvictionActionIsOverflowToDisk() {
    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");
    final EvictionAttributes mockEvictionAttributes = mockContext.mock(EvictionAttributes.class, "EvictionAttributes");

    mockContext.checking(new Expectations() {{
      exactly(2).of(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      exactly(2).of(mockRegionAttributes).getEvictionAttributes();
      will(returnValue(mockEvictionAttributes));
      oneOf(mockEvictionAttributes).getAction();
      will(returnValue(EvictionAction.OVERFLOW_TO_DISK));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertTrue(function.isOverflowToDisk(mockRegion));
  }

  @Test
  public void testIsRegionOverflowToDiskWithNullEvictionAttributes() {
    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");

    mockContext.checking(new Expectations() {{
      oneOf(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      oneOf(mockRegionAttributes).getEvictionAttributes();
      will(returnValue(null));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertFalse(function.isOverflowToDisk(mockRegion));
  }

  @Test
  public void testIsRegionPersistentWhenDataPolicyIsPersistentPartition() {
    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");

    mockContext.checking(new Expectations() {{
      oneOf(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      oneOf(mockRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.PERSISTENT_PARTITION));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertTrue(function.isPersistent(mockRegion));
  }

  @Test
  public void testIsRegionPersistentWhenDataPolicyIsPersistentReplicate() {
    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");

    mockContext.checking(new Expectations() {{
      oneOf(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      oneOf(mockRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.PERSISTENT_REPLICATE));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertTrue(function.isPersistent(mockRegion));
  }

  @Test
  public void testIsRegionPersistentWhenDataPolicyIsNormal() {
    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");

    mockContext.checking(new Expectations() {{
      oneOf(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      oneOf(mockRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.NORMAL));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertFalse(function.isPersistent(mockRegion));
  }

  @Test
  public void testIsRegionPersistentWhenDataPolicyIsPartition() {
    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");

    mockContext.checking(new Expectations() {{
      oneOf(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      oneOf(mockRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.PARTITION));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertFalse(function.isPersistent(mockRegion));
  }

  @Test
  public void testIsRegionPersistentWhenDataPolicyIsPreloaded() {
    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");

    mockContext.checking(new Expectations() {{
      oneOf(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      oneOf(mockRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.PRELOADED));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertFalse(function.isPersistent(mockRegion));
  }

  @Test
  public void testIsRegionPersistentWhenDataPolicyIsReplicate() {
    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");

    mockContext.checking(new Expectations() {{
      oneOf(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      oneOf(mockRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.REPLICATE));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertFalse(function.isPersistent(mockRegion));
  }

  @Test
  public void testIsRegionUsingDiskStoreWhenUsingDefaultDiskStore() {
    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");
    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      atLeast(1).of(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      oneOf(mockRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.PERSISTENT_REPLICATE));
      oneOf(mockRegionAttributes).getDiskStoreName();
      will(returnValue(null));
      oneOf(mockDiskStore).getName();
      will(returnValue(DiskStoreDetails.DEFAULT_DISK_STORE_NAME));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertTrue(function.isUsingDiskStore(mockRegion, mockDiskStore));
  }

  @Test
  public void testIsRegionUsingDiskStoreWhenPersistent() {
    final String diskStoreName = "testDiskStore";

    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");
    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      atLeast(1).of(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      oneOf(mockRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.PERSISTENT_PARTITION));
      oneOf(mockRegionAttributes).getDiskStoreName();
      will(returnValue(diskStoreName));
      oneOf(mockDiskStore).getName();
      will(returnValue(diskStoreName));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertTrue(function.isUsingDiskStore(mockRegion, mockDiskStore));
  }

  @Test
  public void testIsRegionUsingDiskStoreWhenOverflowing() {
    final String diskStoreName = "testDiskStore";

    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");
    final EvictionAttributes mockEvictionAttributes = mockContext.mock(EvictionAttributes.class, "EvictionAttributes");
    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      exactly(4).of(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      oneOf(mockRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.PARTITION));
      oneOf(mockRegionAttributes).getDiskStoreName();
      will(returnValue(diskStoreName));
      exactly(2).of(mockRegionAttributes).getEvictionAttributes();
      will(returnValue(mockEvictionAttributes));
      oneOf(mockEvictionAttributes).getAction();
      will(returnValue(EvictionAction.OVERFLOW_TO_DISK));
      oneOf(mockDiskStore).getName();
      will(returnValue(diskStoreName));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertTrue(function.isUsingDiskStore(mockRegion, mockDiskStore));
  }

  @Test
  public void testIsRegionUsingDiskStoreWhenDiskStoresMismatch() {
    final Region mockRegion = mockContext.mock(Region.class, "Region");
    final RegionAttributes mockRegionAttributes = mockContext.mock(RegionAttributes.class, "RegionAttributes");
    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      atLeast(1).of(mockRegion).getAttributes();
      will(returnValue(mockRegionAttributes));
      oneOf(mockRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.PERSISTENT_PARTITION));
      oneOf(mockRegionAttributes).getDiskStoreName();
      will(returnValue("mockDiskStore"));
      oneOf(mockDiskStore).getName();
      will(returnValue("testDiskStore"));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertFalse(function.isUsingDiskStore(mockRegion, mockDiskStore));
  }

  @Test
  public void testSetRegionDetails() {
    final String diskStoreName = "companyDiskStore";

    final InternalCache mockCache = mockContext.mock(InternalCache.class, "Cache");

    final Region mockCompanyRegion = mockContext.mock(Region.class, "/CompanyRegion");
    final Region mockContractorsRegion = mockContext.mock(Region.class, "/CompanyRegion/ContractorsRegion");
    final Region mockEmployeeRegion = mockContext.mock(Region.class, "/CompanyRegion/EmployeeRegion");
    final Region mockRolesRegion = mockContext.mock(Region.class, "/CompanyRegion/EmployeeRegion/RolesRegion");
    final Region mockProductsRegion = mockContext.mock(Region.class, "/CompanyRegion/ProductsRegion");
    final Region mockServicesRegion = mockContext.mock(Region.class, "/CompanyRegion/ServicesRegion");
    final Region mockPartnersRegion = mockContext.mock(Region.class, "/PartnersRegion");
    final Region mockCustomersRegion = mockContext.mock(Region.class, "/CustomersRegion");

    final RegionAttributes mockCompanyRegionAttributes = mockContext.mock(RegionAttributes.class, "CompanyRegionAttributes");
    final RegionAttributes mockContractorsRegionAttributes =  mockContext.mock(RegionAttributes.class, "ContractorsRegionAttributes");
    final RegionAttributes mockProductsServicesRegionAttributes = mockContext.mock(RegionAttributes.class, "ProductsServicesRegionAttributes");
    final RegionAttributes mockPartnersRegionAttributes = mockContext.mock(RegionAttributes.class, "PartnersRegionAttributes");
    final RegionAttributes mockCustomersRegionAttributes = mockContext.mock(RegionAttributes.class, "CustomersRegionAttributes");

    final EvictionAttributes mockCompanyEvictionAttributes = mockContext.mock(EvictionAttributes.class, "CompanyEvictionAttributes");
    final EvictionAttributes mockContractorsEvictionAttributes = mockContext.mock(EvictionAttributes.class, "ContractorsEvictionAttributes");
    final EvictionAttributes mockCustomersEvictionAttributes = mockContext.mock(EvictionAttributes.class, "CustomersEvictionAttributes");

    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).rootRegions();
      will(returnValue(CollectionUtils.asSet(mockCompanyRegion, mockPartnersRegion, mockCustomersRegion)));
      exactly(5).of(mockCompanyRegion).getAttributes();
      will(returnValue(mockCompanyRegionAttributes));
      oneOf(mockCompanyRegion).getFullPath();
      will(returnValue("/CompanyRegion"));
      oneOf(mockCompanyRegion).getName();
      will(returnValue("CompanyRegion"));
      oneOf(mockCompanyRegion).subregions(false);
      will(returnValue(CollectionUtils.asSet(mockContractorsRegion, mockEmployeeRegion, mockProductsRegion,
        mockServicesRegion)));
      exactly(5).of(mockEmployeeRegion).getAttributes();
      will(returnValue(mockCompanyRegionAttributes));
      oneOf(mockEmployeeRegion).getFullPath();
      will(returnValue("/CompanyRegion/EmployeeRegion"));
      oneOf(mockEmployeeRegion).getName();
      will(returnValue("EmployeeRegion"));
      oneOf(mockEmployeeRegion).subregions(false);
      will(returnValue(CollectionUtils.asSet(mockRolesRegion)));
      exactly(5).of(mockRolesRegion).getAttributes();
      will(returnValue(mockCompanyRegionAttributes));
      oneOf(mockRolesRegion).getFullPath();
      will(returnValue("/CompanyRegion/EmployeeRegion/RolesRegion"));
      oneOf(mockRolesRegion).getName();
      will(returnValue("RolesRegion"));
      oneOf(mockRolesRegion).subregions(false);
      will(returnValue(Collections.emptySet()));
      exactly(6).of(mockCompanyRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.PERSISTENT_PARTITION));
      exactly(3).of(mockCompanyRegionAttributes).getDiskStoreName();
      will(returnValue(diskStoreName));
      exactly(6).of(mockCompanyRegionAttributes).getEvictionAttributes();
      will(returnValue(mockCompanyEvictionAttributes));
      exactly(3).of(mockCompanyEvictionAttributes).getAction();
      will(returnValue(EvictionAction.LOCAL_DESTROY));

      exactly(7).of(mockContractorsRegion).getAttributes();
      will(returnValue(mockContractorsRegionAttributes));
      oneOf(mockContractorsRegion).getFullPath();
      will(returnValue("/CompanyRegion/ContractorsRegion"));
      oneOf(mockContractorsRegion).getName();
      will(returnValue("ContractorsRegion"));
      oneOf(mockContractorsRegion).subregions(false);
      will(returnValue(Collections.emptySet()));
      exactly(2).of(mockContractorsRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.REPLICATE));
      oneOf(mockContractorsRegionAttributes).getDiskStoreName();
      will(returnValue(diskStoreName));
      exactly(4).of(mockContractorsRegionAttributes).getEvictionAttributes();
      will(returnValue(mockContractorsEvictionAttributes));
      exactly(2).of(mockContractorsEvictionAttributes).getAction();
      will(returnValue(EvictionAction.OVERFLOW_TO_DISK));

      exactly(2).of(mockProductsRegion).getAttributes();
      will(returnValue(mockProductsServicesRegionAttributes));
      oneOf(mockProductsRegion).subregions(false);
      will(returnValue(Collections.emptySet()));
      exactly(2).of(mockServicesRegion).getAttributes();
      will(returnValue(mockProductsServicesRegionAttributes));
      oneOf(mockServicesRegion).subregions(false);
      will(returnValue(Collections.emptySet()));
      exactly(2).of(mockProductsServicesRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.PERSISTENT_REPLICATE));
      exactly(2).of(mockProductsServicesRegionAttributes).getDiskStoreName();
      will(returnValue("productsServicesDiskStore"));

      exactly(2).of(mockPartnersRegion).getAttributes();
      will(returnValue(mockPartnersRegionAttributes));
      oneOf(mockPartnersRegion).subregions(false);
      will(returnValue(Collections.emptySet()));
      oneOf(mockPartnersRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.PERSISTENT_PARTITION));
      oneOf(mockPartnersRegionAttributes).getDiskStoreName();
      will(returnValue(""));

      exactly(4).of(mockCustomersRegion).getAttributes();
      will(returnValue(mockCustomersRegionAttributes));
      oneOf(mockCustomersRegion).subregions(false);
      will(returnValue(Collections.emptySet()));
      oneOf(mockCustomersRegionAttributes).getDataPolicy();
      will(returnValue(DataPolicy.REPLICATE));
      oneOf(mockCustomersRegionAttributes).getDiskStoreName();
      will(returnValue(null));
      exactly(2).of(mockCustomersRegionAttributes).getEvictionAttributes();
      will(returnValue(mockCustomersEvictionAttributes));
      oneOf(mockCustomersEvictionAttributes).getAction();
      will(returnValue(EvictionAction.OVERFLOW_TO_DISK));

      atLeast(1).of(mockDiskStore).getName();
      will(returnValue(diskStoreName));
    }});

    final Set<DiskStoreDetails.RegionDetails> expectedRegionDetails = CollectionUtils.asSet(
      createRegionDetails("/CompanyRegion", "CompanyRegion", true, false),
      createRegionDetails("/CompanyRegion/EmployeeRegion", "EmployeeRegion", true, false),
      createRegionDetails("/CompanyRegion/EmployeeRegion/RolesRegion", "RolesRegion", true, false),
      createRegionDetails("/CompanyRegion/ContractorsRegion", "ContractorsRegion", false, true));

    final DiskStoreDetails diskStoreDetails = new DiskStoreDetails(diskStoreName, "memberOne");

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(mockCache);

    function.setRegionDetails(mockCache, mockDiskStore, diskStoreDetails);

    assertRegionDetails(expectedRegionDetails, diskStoreDetails);
  }

  @Test
  public void testGetCacheServerDiskStoreName() {
    final String expectedDiskStoreName = "testDiskStore";

    final CacheServer mockCacheServer = mockContext.mock(CacheServer.class, "CacheServer");
    final ClientSubscriptionConfig mockClientSubscriptionConfig = mockContext.mock(ClientSubscriptionConfig.class, "ClientSubscriptionConfig");

    mockContext.checking(new Expectations() {{
      exactly(2).of(mockCacheServer).getClientSubscriptionConfig();
      will(returnValue(mockClientSubscriptionConfig));
      oneOf(mockClientSubscriptionConfig).getDiskStoreName();
      will(returnValue(expectedDiskStoreName));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertEquals(expectedDiskStoreName, function.getDiskStoreName(mockCacheServer));
  }

  @Test
  public void testGetCacheServerDiskStoreNameWhenUnspecified() {
    final CacheServer mockCacheServer = mockContext.mock(CacheServer.class, "CacheServer");
    final ClientSubscriptionConfig mockClientSubscriptionConfig = mockContext.mock(ClientSubscriptionConfig.class, "ClientSubscriptionConfig");

    mockContext.checking(new Expectations() {{
      exactly(2).of(mockCacheServer).getClientSubscriptionConfig();
      will(returnValue(mockClientSubscriptionConfig));
      oneOf(mockClientSubscriptionConfig).getDiskStoreName();
      will(returnValue(null));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertEquals(DiskStoreDetails.DEFAULT_DISK_STORE_NAME, function.getDiskStoreName(mockCacheServer));
  }

  @Test
  public void testGetCacheServerDiskStoreNameWithNullClientSubscriptionConfig() {
    final CacheServer mockCacheServer = mockContext.mock(CacheServer.class, "CacheServer");

    mockContext.checking(new Expectations() {{
      oneOf(mockCacheServer).getClientSubscriptionConfig();
      will(returnValue(null));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertNull(function.getDiskStoreName(mockCacheServer));
  }

  @Test
  public void testIsCacheServerUsingDiskStore() {
    final String diskStoreName = "testDiskStore";

    final CacheServer mockCacheServer = mockContext.mock(CacheServer.class, "CacheServer");
    final ClientSubscriptionConfig mockClientSubscriptionConfig = mockContext.mock(ClientSubscriptionConfig.class, "ClientSubscriptionConfig");
    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      exactly(2).of(mockCacheServer).getClientSubscriptionConfig();
      will(returnValue(mockClientSubscriptionConfig));
      oneOf(mockClientSubscriptionConfig).getDiskStoreName();
      will(returnValue(diskStoreName));
      oneOf(mockDiskStore).getName();
      will(returnValue(diskStoreName));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertTrue(function.isUsingDiskStore(mockCacheServer, mockDiskStore));
  }

  @Test
  public void testIsCacheServerUsingDiskStoreWhenDiskStoresMismatch() {
    final CacheServer mockCacheServer = mockContext.mock(CacheServer.class, "CacheServer");
    final ClientSubscriptionConfig mockClientSubscriptionConfig = mockContext.mock(ClientSubscriptionConfig.class, "ClientSubscriptionConfig");
    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      exactly(2).of(mockCacheServer).getClientSubscriptionConfig();
      will(returnValue(mockClientSubscriptionConfig));
      oneOf(mockClientSubscriptionConfig).getDiskStoreName();
      will(returnValue(" "));
      oneOf(mockDiskStore).getName();
      will(returnValue("otherDiskStore"));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertFalse(function.isUsingDiskStore(mockCacheServer, mockDiskStore));
  }

  @Test
  public void testIsCacheServerUsingDiskStoreWhenUsingDefaultDiskStore() {
    final CacheServer mockCacheServer = mockContext.mock(CacheServer.class, "CacheServer");
    final ClientSubscriptionConfig mockClientSubscriptionConfig = mockContext.mock(ClientSubscriptionConfig.class, "ClientSubscriptionConfig");
    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      exactly(2).of(mockCacheServer).getClientSubscriptionConfig();
      will(returnValue(mockClientSubscriptionConfig));
      oneOf(mockClientSubscriptionConfig).getDiskStoreName();
      will(returnValue(""));
      oneOf(mockDiskStore).getName();
      will(returnValue(DiskStoreDetails.DEFAULT_DISK_STORE_NAME));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertTrue(function.isUsingDiskStore(mockCacheServer, mockDiskStore));
  }

  @Test
  public void testSetCacheServerDetails() {
    final String diskStoreName = "testDiskStore";

    final InternalCache mockCache = mockContext.mock(InternalCache.class, "Cache");

    final CacheServer mockCacheServer1 = mockContext.mock(CacheServer.class, "CacheServer1");
    final CacheServer mockCacheServer2 = mockContext.mock(CacheServer.class, "CacheServer2");
    final CacheServer mockCacheServer3 = mockContext.mock(CacheServer.class, "CacheServer3");

    final ClientSubscriptionConfig mockCacheServer1ClientSubscriptionConfig = mockContext.mock(ClientSubscriptionConfig.class, "cacheServer1ClientSubscriptionConfig");
    final ClientSubscriptionConfig mockCacheServer2ClientSubscriptionConfig = mockContext.mock(ClientSubscriptionConfig.class, "cacheServer2ClientSubscriptionConfig");

    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getCacheServers();
      will(returnValue(Arrays.asList(mockCacheServer1, mockCacheServer2, mockCacheServer3)));
      exactly(2).of(mockCacheServer1).getClientSubscriptionConfig();
      will(returnValue(mockCacheServer1ClientSubscriptionConfig));
      oneOf(mockCacheServer1ClientSubscriptionConfig).getDiskStoreName();
      will(returnValue(diskStoreName));
      oneOf(mockCacheServer1).getBindAddress();
      will(returnValue("10.127.255.1"));
      oneOf(mockCacheServer1).getPort();
      will(returnValue(65536));
      oneOf(mockCacheServer1).getHostnameForClients();
      will(returnValue("gemini"));
      exactly(2).of(mockCacheServer2).getClientSubscriptionConfig();
      will(returnValue(mockCacheServer2ClientSubscriptionConfig));
      oneOf(mockCacheServer2ClientSubscriptionConfig).getDiskStoreName();
      will(returnValue("  "));
      oneOf(mockCacheServer3).getClientSubscriptionConfig();
      will(returnValue(null));
      exactly(3).of(mockDiskStore).getName();
      will(returnValue(diskStoreName));
    }});

    final Set<DiskStoreDetails.CacheServerDetails> expectedCacheServerDetails = CollectionUtils.asSet(
      createCacheServerDetails("10.127.255.1", 65536, "gemini"));

    final DiskStoreDetails diskStoreDetails = new DiskStoreDetails(diskStoreName, "memberOne");

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    function.setCacheServerDetails(mockCache, mockDiskStore, diskStoreDetails);

    assertCacheServerDetails(expectedCacheServerDetails, diskStoreDetails);
  }
  
  @Test
  public void testGetGatewaySenderDiskStoreName() {
    final String expectedDiskStoreName = "testDiskStore";

    final GatewaySender mockGatewaySender = mockContext.mock(GatewaySender.class, "GatewaySender");

    mockContext.checking(new Expectations() {{
      oneOf(mockGatewaySender).getDiskStoreName();
      will(returnValue(expectedDiskStoreName));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertEquals(expectedDiskStoreName, function.getDiskStoreName(mockGatewaySender));
  }

  @Test
  public void testGetGatewaySenderDiskStoreNameWhenUnspecified() {
    final GatewaySender mockGatewaySender = mockContext.mock(GatewaySender.class, "GatewaySender");

    mockContext.checking(new Expectations() {{
      oneOf(mockGatewaySender).getDiskStoreName();
      will(returnValue(" "));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertEquals(DiskStoreDetails.DEFAULT_DISK_STORE_NAME, function.getDiskStoreName(mockGatewaySender));
  }

  @Test
  public void testIsGatewaySenderPersistent() {
    final GatewaySender mockGatewaySender = mockContext.mock(GatewaySender.class, "GatewaySender");

    mockContext.checking(new Expectations() {{
      oneOf(mockGatewaySender).isPersistenceEnabled();
      will(returnValue(true));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertTrue(function.isPersistent(mockGatewaySender));
  }

  @Test
  public void testIsGatewaySenderPersistentWhenPersistenceIsNotEnabled() {
    final GatewaySender mockGatewaySender = mockContext.mock(GatewaySender.class, "GatewaySender");

    mockContext.checking(new Expectations() {{
      oneOf(mockGatewaySender).isPersistenceEnabled();
      will(returnValue(true));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertTrue(function.isPersistent(mockGatewaySender));
  }

  @Test
  public void testIsGatewaySenderUsingDiskStore() {
    final String diskStoreName = "testDiskStore";

    final GatewaySender mockGatewaySender = mockContext.mock(GatewaySender.class, "GatewaySender");

    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      oneOf(mockGatewaySender).getDiskStoreName();
      will(returnValue(diskStoreName));
      oneOf(mockDiskStore).getName();
      will(returnValue(diskStoreName));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertTrue(function.isUsingDiskStore(mockGatewaySender, mockDiskStore));
  }

  @Test
  public void testIsGatewaySenderUsingDiskStoreWhenDiskStoresMismatch() {
    final GatewaySender mockGatewaySender = mockContext.mock(GatewaySender.class, "GatewaySender");

    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      oneOf(mockGatewaySender).getDiskStoreName();
      will(returnValue("mockDiskStore"));
      oneOf(mockDiskStore).getName();
      will(returnValue("testDiskStore"));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertFalse(function.isUsingDiskStore(mockGatewaySender, mockDiskStore));
  }

  @Test
  public void testIsGatewaySenderUsingDiskStoreWhenUsingDefaultDiskStores() {
    final GatewaySender mockGatewaySender = mockContext.mock(GatewaySender.class, "GatewaySender");

    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      oneOf(mockGatewaySender).getDiskStoreName();
      will(returnValue(" "));
      oneOf(mockDiskStore).getName();
      will(returnValue(DiskStoreDetails.DEFAULT_DISK_STORE_NAME));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertTrue(function.isUsingDiskStore(mockGatewaySender, mockDiskStore));
  }

  @Test
  public void testSetPdxSerializationDetails() {
    final String diskStoreName = "testDiskStore";

    final InternalCache mockCache = mockContext.mock(InternalCache.class, "Cache");

    final DiskStore mockDiskStore =  mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getPdxPersistent();
      will(returnValue(true));
      oneOf(mockCache).getPdxDiskStore();
      will(returnValue(diskStoreName));
      oneOf(mockDiskStore).getName();
      will(returnValue(diskStoreName));
    }});

    final DiskStoreDetails diskStoreDetails = new DiskStoreDetails(diskStoreName, "memberOne");

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(mockCache);

    function.setPdxSerializationDetails(mockCache, mockDiskStore, diskStoreDetails);

    assertTrue(diskStoreDetails.isPdxSerializationMetaDataStored());
  }

  @Test
  public void testSetPdxSerializationDetailsWhenDiskStoreMismatch() {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "Cache");

    final DiskStore mockDiskStore =  mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getPdxPersistent();
      will(returnValue(true));
      oneOf(mockCache).getPdxDiskStore();
      will(returnValue("mockDiskStore"));
      oneOf(mockDiskStore).getName();
      will(returnValue("testDiskStore"));
    }});

    final DiskStoreDetails diskStoreDetails = new DiskStoreDetails("testDiskStore", "memberOne");

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(mockCache);

    function.setPdxSerializationDetails(mockCache, mockDiskStore, diskStoreDetails);

    assertFalse(diskStoreDetails.isPdxSerializationMetaDataStored());
  }

  @Test
  public void testSetPdxSerializationDetailsWhenPdxIsNotPersistent() {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "Cache");

    final DiskStore mockDiskStore =  mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getPdxPersistent();
      will(returnValue(false));
    }});

    final DiskStoreDetails diskStoreDetails = new DiskStoreDetails("testDiskStore", "memberOne");

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(mockCache);

    function.setPdxSerializationDetails(mockCache, mockDiskStore, diskStoreDetails);

    assertFalse(diskStoreDetails.isPdxSerializationMetaDataStored());
  }

  @Test
  public void testGetAsyncEventQueueDiskStoreName() {
    final String expectedDiskStoreName = "testDiskStore";

    final AsyncEventQueue mockQueue = mockContext.mock(AsyncEventQueue.class, "AsyncEventQueue");

    mockContext.checking(new Expectations() {{
      oneOf(mockQueue).getDiskStoreName();
      will(returnValue(expectedDiskStoreName));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertEquals(expectedDiskStoreName, function.getDiskStoreName(mockQueue));
  }

  @Test
  public void testGetAsyncEventQueueDiskStoreNameUsingDefaultDiskStore() {
    final AsyncEventQueue mockQueue = mockContext.mock(AsyncEventQueue.class, "AsyncEventQueue");

    mockContext.checking(new Expectations() {{
      oneOf(mockQueue).getDiskStoreName();
      will(returnValue(null));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertEquals(DiskStoreDetails.DEFAULT_DISK_STORE_NAME, function.getDiskStoreName(mockQueue));
  }

  @Test
  public void testIsAsyncEventQueueUsingDiskStore() {
    final String diskStoreName = "testDiskStore";

    final AsyncEventQueue mockQueue = mockContext.mock(AsyncEventQueue.class, "AsyncEventQueue");

    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      oneOf(mockQueue).getDiskStoreName();
      will(returnValue(diskStoreName));
      oneOf(mockQueue).isPersistent();
      will(returnValue(true));
      oneOf(mockDiskStore).getName();
      will(returnValue(diskStoreName));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertTrue(function.isUsingDiskStore(mockQueue, mockDiskStore));
  }

  @Test
  public void testIsAsyncEventQueueUsingDiskStoreWhenDiskStoresMismatch() {
    final AsyncEventQueue mockQueue = mockContext.mock(AsyncEventQueue.class, "AsyncEventQueue");

    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      oneOf(mockQueue).getDiskStoreName();
      will(returnValue("mockDiskStore"));
      oneOf(mockQueue).isPersistent();
      will(returnValue(true));
      oneOf(mockDiskStore).getName();
      will(returnValue(DiskStoreDetails.DEFAULT_DISK_STORE_NAME));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertFalse(function.isUsingDiskStore(mockQueue, mockDiskStore));
  }

  @Test
  public void testIsAsyncEventQueueUsingDiskStoreWhenQueueIsNotPersistent() {
    final String diskStoreName = "testDiskStore";

    final AsyncEventQueue mockQueue = mockContext.mock(AsyncEventQueue.class, "AsyncEventQueue");

    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      oneOf(mockQueue).isPersistent();
      will(returnValue(false));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertFalse(function.isUsingDiskStore(mockQueue, mockDiskStore));
  }

  @Test
  public void testIsAsyncEventQueueUsingDiskStoreWhenUsingDefaultDiskStore() {
    final AsyncEventQueue mockQueue = mockContext.mock(AsyncEventQueue.class, "AsyncEventQueue");

    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      oneOf(mockQueue).getDiskStoreName();
      will(returnValue(" "));
      oneOf(mockQueue).isPersistent();
      will(returnValue(true));
      oneOf(mockDiskStore).getName();
      will(returnValue(DiskStoreDetails.DEFAULT_DISK_STORE_NAME));
    }});

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(null);

    assertTrue(function.isUsingDiskStore(mockQueue, mockDiskStore));
  }

  @Test
  public void testSetAsyncEventQueueDetails() {
    final String diskStoreName = "testDiskStore";

    final InternalCache mockCache = mockContext.mock(InternalCache.class, "Cache");

    final AsyncEventQueue mockQueue1 = mockContext.mock(AsyncEventQueue.class, "AsyncEvenQueue1");
    final AsyncEventQueue mockQueue2 = mockContext.mock(AsyncEventQueue.class, "AsyncEvenQueue2");
    final AsyncEventQueue mockQueue3 = mockContext.mock(AsyncEventQueue.class, "AsyncEvenQueue3");

    final DiskStore mockDiskStore = mockContext.mock(DiskStore.class, "DiskStore");

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getAsyncEventQueues();
      will(returnValue(CollectionUtils.asSet(mockQueue1, mockQueue2, mockQueue3)));
      oneOf(mockQueue1).isPersistent();
      will(returnValue(true));
      oneOf(mockQueue1).getDiskStoreName();
      will(returnValue(diskStoreName));
      oneOf(mockQueue1).getId();
      will(returnValue("q1"));
      oneOf(mockQueue2).isPersistent();
      will(returnValue(true));
      oneOf(mockQueue2).getDiskStoreName();
      will(returnValue(null));
      oneOf(mockQueue3).isPersistent();
      will(returnValue(false));
      atLeast(1).of(mockDiskStore).getName();
      will(returnValue(diskStoreName));
    }});

    final Set<DiskStoreDetails.AsyncEventQueueDetails> expectedAsyncEventQueueDetails = CollectionUtils.asSet(
      createAsyncEventQueueDetails("q1"));

    final DiskStoreDetails diskStoreDetails = new DiskStoreDetails(diskStoreName, "memberOne");

    final DescribeDiskStoreFunction function = createDescribeDiskStoreFunction(mockCache);

    function.setAsyncEventQueueDetails(mockCache, mockDiskStore, diskStoreDetails);

    assertAsyncEventQueueDetails(expectedAsyncEventQueueDetails, diskStoreDetails);
  }

  private static class TestDescribeDiskStoreFunction extends DescribeDiskStoreFunction {

    private final Cache cache;

    public TestDescribeDiskStoreFunction(final Cache cache) {
      this.cache = cache;
    }

    @Override
    protected Cache getCache() {
      return this.cache;
    }
  }

  private static class TestResultSender implements ResultSender {

    private final List<Object> results = new LinkedList<Object>();

    private Throwable t;

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
