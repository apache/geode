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
package org.apache.geode.management.internal.cli.commands;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.jmock.Expectations;
import org.jmock.Mockery;
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
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.util.CollectionUtils;
import org.apache.geode.management.internal.cli.domain.DiskStoreDetails;
import org.apache.geode.management.internal.cli.functions.DescribeDiskStoreFunction;
import org.apache.geode.management.internal.cli.functions.ListDiskStoresFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.DiskStoreNotFoundException;
import org.apache.geode.management.internal.cli.util.MemberNotFoundException;
import org.apache.geode.test.junit.categories.UnitTest;


/**
 * The DiskStoreCommandsJUnitTest class is a test suite of test cases testing the contract and functionality of the
 * DiskStoreCommands class implementing commands in the GemFire shell (gfsh) that access and modify disk stores in
 * GemFire.
 * </p>
 * @see org.apache.geode.management.internal.cli.commands.DiskStoreCommands
 * @see org.apache.geode.management.internal.cli.domain.DiskStoreDetails
 * @see org.apache.geode.management.internal.cli.functions.DescribeDiskStoreFunction
 * @see org.apache.geode.management.internal.cli.functions.ListDiskStoresFunction
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.jmock.lib.legacy.ClassImposteriser
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class DiskStoreCommandsJUnitTest {

  private Mockery mockContext;

  @Before
  public void setUp() {
    mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
    }};
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  private DiskStoreCommands createDiskStoreCommands(final Cache cache,
                                                      final DistributedMember distributedMember,
                                                      final Execution functionExecutor)
  {
    return new TestDiskStoreCommands(cache, distributedMember, functionExecutor);
  }

  private DiskStoreDetails createDiskStoreDetails(final String memberId, final String diskStoreName) {
    return new DiskStoreDetails(diskStoreName, memberId);
  }

  @Test
  public void testGetDiskStoreDescription() {
    final String diskStoreName = "mockDiskStore";
    final String memberId = "mockMember";

    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");

    final DiskStoreDetails expectedDiskStoredDetails = createDiskStoreDetails(memberId, diskStoreName);

    mockContext.checking(new Expectations() {{
      oneOf(mockMember).getName();
      will(returnValue(null));
      oneOf(mockMember).getId();
      will(returnValue(memberId));
      oneOf(mockFunctionExecutor).withArgs(with(equal(diskStoreName)));
      will(returnValue(mockFunctionExecutor));
      oneOf(mockFunctionExecutor).execute(with(aNonNull(DescribeDiskStoreFunction.class)));
      will(returnValue(mockResultCollector));
      oneOf(mockResultCollector).getResult();
      will(returnValue(Arrays.asList(expectedDiskStoredDetails)));
    }});

    final DiskStoreCommands commands = createDiskStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    final DiskStoreDetails actualDiskStoreDetails = commands.getDiskStoreDescription(memberId, diskStoreName);

    assertNotNull(actualDiskStoreDetails);
    assertEquals(expectedDiskStoredDetails, actualDiskStoreDetails);
  }

  @Test(expected = MemberNotFoundException.class)
  public void testGetDiskStoreDescriptionThrowsMemberNotFoundException() {
    final String diskStoreName = "mockDiskStore";
    final String memberId = "mockMember";

    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    mockContext.checking(new Expectations() {{
      oneOf(mockMember).getName();
      will(returnValue(null));
      oneOf(mockMember).getId();
      will(returnValue("testMember"));
    }});

    final DiskStoreCommands commands = createDiskStoreCommands(mockCache, mockMember, null);

    try {
      commands.getDiskStoreDescription(memberId, diskStoreName);
    }
    catch (MemberNotFoundException expected) {
      assertEquals(CliStrings.format(CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, memberId), expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = DiskStoreNotFoundException.class)
  public void testGetDiskStoreDescriptionThrowsDiskStoreNotFoundException() {
    final String diskStoreName = "mockDiskStore";
    final String memberId = "mockMember";

    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    mockContext.checking(new Expectations() {{
      oneOf(mockMember).getName();
      will(returnValue(null));
      oneOf(mockMember).getId();
      will(returnValue(memberId));
      oneOf(mockFunctionExecutor).withArgs(with(equal(diskStoreName)));
      will(returnValue(mockFunctionExecutor));
      oneOf(mockFunctionExecutor).execute(with(aNonNull(DescribeDiskStoreFunction.class)));
      will(throwException(new DiskStoreNotFoundException("expected")));
    }});

    final DiskStoreCommands commands = createDiskStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    try {
      commands.getDiskStoreDescription(memberId, diskStoreName);
    }
    catch (DiskStoreNotFoundException expected) {
      assertEquals("expected", expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = RuntimeException.class)
  public void testGetDiskStoreDescriptionThrowsRuntimeException() {
    final String diskStoreName = "mockDiskStore";
    final String memberId = "mockMember";

    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    mockContext.checking(new Expectations() {{
      oneOf(mockMember).getName();
      will(returnValue(null));
      oneOf(mockMember).getId();
      will(returnValue(memberId));
      oneOf(mockFunctionExecutor).withArgs(with(equal(diskStoreName)));
      will(returnValue(mockFunctionExecutor));
      oneOf(mockFunctionExecutor).execute(with(aNonNull(DescribeDiskStoreFunction.class)));
      will(throwException(new RuntimeException("expected")));
    }});

    final DiskStoreCommands commands = createDiskStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    try {
      commands.getDiskStoreDescription(memberId, diskStoreName);
    }
    catch (RuntimeException expected) {
      assertEquals("expected", expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = RuntimeException.class)
  public void testGetDiskStoreDescriptionWithInvalidFunctionResultReturnType() {
    final String diskStoreName = "mockDiskStore";
    final String memberId = "mockMember";

    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");

    mockContext.checking(new Expectations() {{
      oneOf(mockMember).getName();
      will(returnValue(null));
      oneOf(mockMember).getId();
      will(returnValue(memberId));
      oneOf(mockFunctionExecutor).withArgs(with(equal(diskStoreName)));
      will(returnValue(mockFunctionExecutor));
      oneOf(mockFunctionExecutor).execute(with(aNonNull(DescribeDiskStoreFunction.class)));
      will(returnValue(mockResultCollector));
      oneOf(mockResultCollector).getResult();
      will(returnValue(Arrays.asList(new Object())));
    }});

    final DiskStoreCommands commands = createDiskStoreCommands(mockCache, mockMember, mockFunctionExecutor);

    try {
      commands.getDiskStoreDescription(memberId, diskStoreName);
    }
    catch (RuntimeException expected) {
      assertEquals(CliStrings.format(CliStrings.UNEXPECTED_RETURN_TYPE_EXECUTING_COMMAND_ERROR_MESSAGE,
        Object.class.getName(), CliStrings.DESCRIBE_DISK_STORE), expected.getMessage());
      assertNull(expected.getCause());
      throw expected;
    }
  }

  @Test
  public void testGetDiskStoreList() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedMember mockDistributedMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final AbstractExecution mockFunctionExecutor = mockContext.mock(AbstractExecution.class, "Function Executor");

    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");

    final DiskStoreDetails diskStoreDetails1 = createDiskStoreDetails("memberOne", "cacheServerDiskStore");
    final DiskStoreDetails diskStoreDetails2 = createDiskStoreDetails("memberOne", "gatewayDiskStore");
    final DiskStoreDetails diskStoreDetails3 = createDiskStoreDetails("memberTwo", "pdxDiskStore");
    final DiskStoreDetails diskStoreDetails4 = createDiskStoreDetails("memberTwo", "regionDiskStore");

    final List<DiskStoreDetails> expectedDiskStores = Arrays.asList(
      diskStoreDetails1,
      diskStoreDetails2,
      diskStoreDetails3,
      diskStoreDetails4
    );

    final List<Set<DiskStoreDetails>> results = new ArrayList<Set<DiskStoreDetails>>();

    results.add(CollectionUtils.asSet(diskStoreDetails4, diskStoreDetails3));
    results.add(CollectionUtils.asSet(diskStoreDetails1, diskStoreDetails2));

    mockContext.checking(new Expectations() {{
      oneOf(mockFunctionExecutor).setIgnoreDepartedMembers(with(equal(true)));
      oneOf(mockFunctionExecutor).execute(with(aNonNull(ListDiskStoresFunction.class)));
      will(returnValue(mockResultCollector));
      oneOf(mockResultCollector).getResult();
      will(returnValue(results));
    }});

    final DiskStoreCommands commands = createDiskStoreCommands(mockCache, mockDistributedMember, mockFunctionExecutor);

    final List<DiskStoreDetails> actualDiskStores = commands.getDiskStoreListing(commands.getNormalMembers(mockCache));

    Assert.assertNotNull(actualDiskStores);
    assertEquals(expectedDiskStores, actualDiskStores);
  }

  @Test(expected = RuntimeException.class)
  public void testGetDiskStoreListThrowsRuntimeException() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedMember mockDistributedMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    mockContext.checking(new Expectations() {{
      oneOf(mockFunctionExecutor).execute(with(aNonNull(ListDiskStoresFunction.class)));
      will(throwException(new RuntimeException("expected")));
    }});

    final DiskStoreCommands commands = createDiskStoreCommands(mockCache, mockDistributedMember, mockFunctionExecutor);

    try {
      commands.getDiskStoreListing(commands.getNormalMembers(mockCache));
    }
    catch (RuntimeException expected) {
      assertEquals("expected", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testGetDiskStoreListReturnsFunctionInvocationTargetExceptionInResults() {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedMember mockDistributedMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final AbstractExecution mockFunctionExecutor = mockContext.mock(AbstractExecution.class, "Function Executor");

    final ResultCollector mockResultCollector = mockContext.mock(ResultCollector.class, "ResultCollector");

    final DiskStoreDetails diskStoreDetails = createDiskStoreDetails("memberOne", "cacheServerDiskStore");

    final List<DiskStoreDetails> expectedDiskStores = Arrays.asList(diskStoreDetails);

    final List<Object> results = new ArrayList<Object>();

    results.add(CollectionUtils.asSet(diskStoreDetails));
    results.add(new FunctionInvocationTargetException("expected"));

    mockContext.checking(new Expectations() {{
      oneOf(mockFunctionExecutor).setIgnoreDepartedMembers(with(equal(true)));
      oneOf(mockFunctionExecutor).execute(with(aNonNull(ListDiskStoresFunction.class)));
      will(returnValue(mockResultCollector));
      oneOf(mockResultCollector).getResult();
      will(returnValue(results));
    }});

    final DiskStoreCommands commands = createDiskStoreCommands(mockCache, mockDistributedMember, mockFunctionExecutor);

    final List<DiskStoreDetails> actualDiskStores = commands.getDiskStoreListing(commands.getNormalMembers(mockCache));

    Assert.assertNotNull(actualDiskStores);
    assertEquals(expectedDiskStores, actualDiskStores);
  }

  private static class TestDiskStoreCommands extends DiskStoreCommands {

    private final Cache cache;
    private final DistributedMember distributedMember;
    private final Execution functionExecutor;

    public TestDiskStoreCommands(final Cache cache, final DistributedMember distributedMember, final Execution functionExecutor) {
      assert cache != null : "The Cache cannot be null!";
      this.cache = cache;
      this.distributedMember = distributedMember;
      this.functionExecutor = functionExecutor;
    }

    @Override
    protected Cache getCache() {
      return this.cache;
    }

    @Override
    protected Set<DistributedMember> getMembers(final Cache cache) {
      assertSame(getCache(), cache);
      return Collections.singleton(this.distributedMember);
    }

    @Override
    protected Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
      Assert.assertNotNull(members);
      return this.functionExecutor;
    }
    
    @Override
    protected Set<DistributedMember> getNormalMembers(final Cache cache) {
      assertSame(getCache(), cache);
      return Collections.singleton(this.distributedMember);
    }
  }

}
