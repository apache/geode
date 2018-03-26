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
package org.apache.geode.management.internal.cli.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.util.CollectionUtils;
import org.apache.geode.management.internal.cli.domain.DiskStoreDetails;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.functions.DescribeDiskStoreFunction;
import org.apache.geode.management.internal.cli.functions.ListDiskStoresFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * The DiskStoreCommandsJUnitTest class is a test suite of test cases testing the contract and
 * functionality of the command classes relating to disk stores that implement commands in the
 * GemFire shell (gfsh) that access and modify disk stores in GemFire.
 *
 * @see org.apache.geode.management.internal.cli.commands.DescribeDiskStoreCommand
 * @see ListDiskStoresCommand
 * @see org.apache.geode.management.internal.cli.domain.DiskStoreDetails
 * @see org.apache.geode.management.internal.cli.functions.DescribeDiskStoreFunction
 * @see org.apache.geode.management.internal.cli.functions.ListDiskStoresFunction
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.jmock.lib.legacy.ClassImposteriser
 * @see org.junit.Assert
 * @see org.junit.Test
 *
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class DiskStoreCommandsJUnitTest {

  private Mockery mockContext;

  @Before
  public void setUp() {
    mockContext = new Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
        setThreadingPolicy(new Synchroniser());
      }
    };
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  private DescribeDiskStoreCommand createDescribeDiskStoreCommand(final InternalCache cache,
      final DistributedMember distributedMember, final Execution functionExecutor) {
    return new TestDescribeDiskStoreCommand(cache, distributedMember, functionExecutor);
  }

  private ListDiskStoresCommand createListDiskStoreCommand(final InternalCache cache,
      final DistributedMember distributedMember, final Execution functionExecutor) {
    return new TestListDiskStoresCommand(cache, distributedMember, functionExecutor);
  }

  private DiskStoreDetails createDiskStoreDetails(final String memberId,
      final String diskStoreName) {
    return new DiskStoreDetails(diskStoreName, memberId);
  }

  @Test
  public void testGetDiskStoreDescription() {
    final String diskStoreName = "mockDiskStore";
    final String memberId = "mockMember";

    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final DistributedMember mockMember =
        mockContext.mock(DistributedMember.class, "DistributedMember");

    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    final ResultCollector mockResultCollector =
        mockContext.mock(ResultCollector.class, "ResultCollector");

    final DiskStoreDetails expectedDiskStoredDetails =
        createDiskStoreDetails(memberId, diskStoreName);

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).setArguments(with(equal(diskStoreName)));
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(DescribeDiskStoreFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(Collections.singletonList(expectedDiskStoredDetails)));
      }
    });

    final DescribeDiskStoreCommand describeCommand =
        createDescribeDiskStoreCommand(mockCache, mockMember, mockFunctionExecutor);

    final DiskStoreDetails actualDiskStoreDetails =
        describeCommand.getDiskStoreDescription(memberId, diskStoreName);

    assertNotNull(actualDiskStoreDetails);
    assertEquals(expectedDiskStoredDetails, actualDiskStoreDetails);
  }

  @Test(expected = EntityNotFoundException.class)
  public void testGetDiskStoreDescriptionThrowsEntityNotFoundException() {
    final String diskStoreName = "mockDiskStore";
    final String memberId = "mockMember";

    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final DistributedMember mockMember =
        mockContext.mock(DistributedMember.class, "DistributedMember");

    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).setArguments(with(equal(diskStoreName)));
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(DescribeDiskStoreFunction.class)));
        will(throwException(new EntityNotFoundException("expected")));
      }
    });

    final DescribeDiskStoreCommand describeCommand =
        createDescribeDiskStoreCommand(mockCache, mockMember, mockFunctionExecutor);

    try {
      describeCommand.getDiskStoreDescription(memberId, diskStoreName);
    } catch (EntityNotFoundException expected) {
      assertEquals("expected", expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = RuntimeException.class)
  public void testGetDiskStoreDescriptionThrowsRuntimeException() {
    final String diskStoreName = "mockDiskStore";
    final String memberId = "mockMember";

    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final DistributedMember mockMember =
        mockContext.mock(DistributedMember.class, "DistributedMember");

    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).setArguments(with(equal(diskStoreName)));
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(DescribeDiskStoreFunction.class)));
        will(throwException(new RuntimeException("expected")));
      }
    });

    final DescribeDiskStoreCommand describeCommand =
        createDescribeDiskStoreCommand(mockCache, mockMember, mockFunctionExecutor);

    try {
      describeCommand.getDiskStoreDescription(memberId, diskStoreName);
    } catch (RuntimeException expected) {
      assertEquals("expected", expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = RuntimeException.class)
  public void testGetDiskStoreDescriptionWithInvalidFunctionResultReturnType() {
    final String diskStoreName = "mockDiskStore";
    final String memberId = "mockMember";

    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final DistributedMember mockMember =
        mockContext.mock(DistributedMember.class, "DistributedMember");

    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    final ResultCollector mockResultCollector =
        mockContext.mock(ResultCollector.class, "ResultCollector");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).setArguments(with(equal(diskStoreName)));
        will(returnValue(mockFunctionExecutor));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(DescribeDiskStoreFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(Collections.singletonList(new Object())));
      }
    });

    final DescribeDiskStoreCommand describeCommand =
        createDescribeDiskStoreCommand(mockCache, mockMember, mockFunctionExecutor);

    try {
      describeCommand.getDiskStoreDescription(memberId, diskStoreName);
    } catch (RuntimeException expected) {
      assertEquals(
          CliStrings.format(CliStrings.UNEXPECTED_RETURN_TYPE_EXECUTING_COMMAND_ERROR_MESSAGE,
              Object.class.getName(), CliStrings.DESCRIBE_DISK_STORE),
          expected.getMessage());
      assertNull(expected.getCause());
      throw expected;
    }
  }

  @Test
  public void testGetDiskStoreList() {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final DistributedMember mockDistributedMember =
        mockContext.mock(DistributedMember.class, "DistributedMember");

    final AbstractExecution mockFunctionExecutor =
        mockContext.mock(AbstractExecution.class, "Function Executor");

    final ResultCollector mockResultCollector =
        mockContext.mock(ResultCollector.class, "ResultCollector");

    final DiskStoreDetails diskStoreDetails1 =
        createDiskStoreDetails("memberOne", "cacheServerDiskStore");
    final DiskStoreDetails diskStoreDetails2 =
        createDiskStoreDetails("memberOne", "gatewayDiskStore");
    final DiskStoreDetails diskStoreDetails3 = createDiskStoreDetails("memberTwo", "pdxDiskStore");
    final DiskStoreDetails diskStoreDetails4 =
        createDiskStoreDetails("memberTwo", "regionDiskStore");

    final List<DiskStoreDetails> expectedDiskStores =
        Arrays.asList(diskStoreDetails1, diskStoreDetails2, diskStoreDetails3, diskStoreDetails4);

    final List<Set<DiskStoreDetails>> results = new ArrayList<>();

    results.add(CollectionUtils.asSet(diskStoreDetails4, diskStoreDetails3));
    results.add(CollectionUtils.asSet(diskStoreDetails1, diskStoreDetails2));

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).setIgnoreDepartedMembers(with(equal(true)));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(ListDiskStoresFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(results));
      }
    });

    final ListDiskStoresCommand listCommand =
        createListDiskStoreCommand(mockCache, mockDistributedMember, mockFunctionExecutor);

    final List<DiskStoreDetails> actualDiskStores =
        listCommand.getDiskStoreListing(Collections.singleton(mockDistributedMember));

    Assert.assertNotNull(actualDiskStores);
    assertEquals(expectedDiskStores, actualDiskStores);
  }

  @Test(expected = RuntimeException.class)
  public void testGetDiskStoreListThrowsRuntimeException() {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final DistributedMember mockDistributedMember =
        mockContext.mock(DistributedMember.class, "DistributedMember");

    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).execute(with(aNonNull(ListDiskStoresFunction.class)));
        will(throwException(new RuntimeException("expected")));
      }
    });

    final ListDiskStoresCommand listCommand =
        createListDiskStoreCommand(mockCache, mockDistributedMember, mockFunctionExecutor);

    try {
      listCommand.getDiskStoreListing(Collections.singleton(mockDistributedMember));
    } catch (RuntimeException expected) {
      assertEquals("expected", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testGetDiskStoreListReturnsFunctionInvocationTargetExceptionInResults() {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final DistributedMember mockDistributedMember =
        mockContext.mock(DistributedMember.class, "DistributedMember");

    final AbstractExecution mockFunctionExecutor =
        mockContext.mock(AbstractExecution.class, "Function Executor");

    final ResultCollector mockResultCollector =
        mockContext.mock(ResultCollector.class, "ResultCollector");

    final DiskStoreDetails diskStoreDetails =
        createDiskStoreDetails("memberOne", "cacheServerDiskStore");

    final List<DiskStoreDetails> expectedDiskStores = Collections.singletonList(diskStoreDetails);

    final List<Object> results = new ArrayList<>();

    results.add(CollectionUtils.asSet(diskStoreDetails));
    results.add(new FunctionInvocationTargetException("expected"));

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).setIgnoreDepartedMembers(with(equal(true)));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(ListDiskStoresFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(results));
      }
    });

    final ListDiskStoresCommand listCommand =
        createListDiskStoreCommand(mockCache, mockDistributedMember, mockFunctionExecutor);

    final List<DiskStoreDetails> actualDiskStores =
        listCommand.getDiskStoreListing(Collections.singleton(mockDistributedMember));

    Assert.assertNotNull(actualDiskStores);
    assertEquals(expectedDiskStores, actualDiskStores);
  }

  private static class TestDescribeDiskStoreCommand extends DescribeDiskStoreCommand {

    private final InternalCache cache;
    private final DistributedMember distributedMember;
    private final Execution functionExecutor;

    TestDescribeDiskStoreCommand(final InternalCache cache,
        final DistributedMember distributedMember, final Execution functionExecutor) {
      assert cache != null : "The Cache cannot be null!";
      this.cache = cache;
      this.distributedMember = distributedMember;
      this.functionExecutor = functionExecutor;
    }

    @Override
    public Cache getCache() {
      return this.cache;
    }

    @Override
    public Set<DistributedMember> getAllMembers() {
      assertSame(getCache(), cache);
      return Collections.singleton(this.distributedMember);
    }

    @Override
    public DistributedMember getMember(String nameOrId) {
      assertSame(getCache(), cache);
      return this.distributedMember;
    }

    @Override
    public Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
      Assert.assertNotNull(members);
      return this.functionExecutor;
    }
  }

  private static class TestListDiskStoresCommand extends ListDiskStoresCommand {
    private final InternalCache cache;
    private final DistributedMember distributedMember;
    private final Execution functionExecutor;

    TestListDiskStoresCommand(final InternalCache cache, final DistributedMember distributedMember,
        final Execution functionExecutor) {
      assert cache != null : "The Cache cannot be null!";
      this.cache = cache;
      this.distributedMember = distributedMember;
      this.functionExecutor = functionExecutor;
    }

    @Override
    public Cache getCache() {
      return this.cache;
    }

    @Override
    public Set<DistributedMember> getAllMembers() {
      assertSame(getCache(), cache);
      return Collections.singleton(this.distributedMember);
    }

    @Override
    public DistributedMember getMember(String nameOrId) {
      assertSame(getCache(), cache);
      return this.distributedMember;
    }

    @Override
    public Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
      Assert.assertNotNull(members);
      return this.functionExecutor;
    }
  }
}
