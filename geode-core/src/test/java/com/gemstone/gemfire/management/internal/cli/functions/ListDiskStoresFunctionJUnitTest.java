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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.management.internal.cli.domain.DiskStoreDetails;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The ListDiskStoreFunctionJUnitTest test suite class tests the contract and functionality of the
 * ListDiskStoresFunction.
 * </p>
 * @see com.gemstone.gemfire.internal.cache.DiskStoreImpl
 * @see com.gemstone.gemfire.management.internal.cli.domain.DiskStoreDetails
 * @see com.gemstone.gemfire.management.internal.cli.functions.ListDiskStoresFunction
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class ListDiskStoresFunctionJUnitTest {

  private Mockery mockContext;

  @Before
  public void setup() {
    mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
    }};
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  private DiskStoreDetails createDiskStoreDetails(final UUID id,
                                                    final String name,
                                                    final String memberName,
                                                    final String memberId) {
    return new DiskStoreDetails(id, name, memberId, memberName);
  }

  private ListDiskStoresFunction createListDiskStoresFunction(final Cache cache) {
    return new TestListDiskStoresFunction(cache);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecute() throws Throwable {
    final UUID mockDiskStoreOneId = UUID.randomUUID();
    final UUID mockDiskStoreTwoId = UUID.randomUUID();
    final UUID mockDiskStoreThreeId = UUID.randomUUID();

    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";

    final InternalCache mockCache = mockContext.mock(InternalCache.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final DiskStoreImpl mockDiskStoreOne = mockContext.mock(DiskStoreImpl.class, "DiskStoreOne");
    final DiskStoreImpl mockDiskStoreTwo = mockContext.mock(DiskStoreImpl.class, "DiskStoreTwo");
    final DiskStoreImpl mockDiskStoreThree = mockContext.mock(DiskStoreImpl.class, "DiskStoreThree");

    final Collection<DiskStoreImpl> mockDiskStores = new ArrayList<DiskStoreImpl>();

    mockDiskStores.add(mockDiskStoreOne);
    mockDiskStores.add(mockDiskStoreTwo);
    mockDiskStores.add(mockDiskStoreThree);

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getMyId();
      will(returnValue(mockMember));
      oneOf(mockCache).listDiskStoresIncludingRegionOwned();
      will(returnValue(mockDiskStores));
      exactly(3).of(mockMember).getId();
      will(returnValue(memberId));
      exactly(3).of(mockMember).getName();
      will(returnValue(memberName));
      oneOf(mockDiskStoreOne).getDiskStoreUUID();
      will(returnValue(mockDiskStoreOneId));
      oneOf(mockDiskStoreOne).getName();
      will(returnValue("ds-backup"));
      oneOf(mockDiskStoreTwo).getDiskStoreUUID();
      will(returnValue(mockDiskStoreTwoId));
      oneOf(mockDiskStoreTwo).getName();
      will(returnValue("ds-overflow"));
      oneOf(mockDiskStoreThree).getDiskStoreUUID();
      will(returnValue(mockDiskStoreThreeId));
      oneOf(mockDiskStoreThree).getName();
      will(returnValue("ds-persistence"));
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final ListDiskStoresFunction function = createListDiskStoresFunction(mockCache);

    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertEquals(1, results.size());

    final Set<DiskStoreDetails> diskStoreDetails = (Set<DiskStoreDetails>) results.get(0);

    assertNotNull(diskStoreDetails);
    assertEquals(3, diskStoreDetails.size());
    diskStoreDetails.containsAll(Arrays.asList(
      createDiskStoreDetails(mockDiskStoreOneId, "ds-backup", memberId, memberName),
      createDiskStoreDetails(mockDiskStoreTwoId, "ds-overflow", memberId, memberName),
      createDiskStoreDetails(mockDiskStoreThreeId, "ds-persistence", memberId, memberName)));
  }

  @Test(expected = CacheClosedException.class)
  public void testExecuteOnMemberWithNoCache() throws Throwable {
    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "MockFunctionContext");

    final ListDiskStoresFunction testListDiskStoresFunction = new TestListDiskStoresFunction(mockContext.mock(Cache.class, "MockCache")) {
      @Override protected Cache getCache() {
        throw new CacheClosedException("Expected");
      }
    };

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    testListDiskStoresFunction.execute(mockFunctionContext);

    try {
      testResultSender.getResults();
    }
    catch (CacheClosedException expected) {
      assertEquals("Expected", expected.getMessage());
      throw expected;
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteOnMemberHavingNoDiskStores() throws Throwable {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getMyId();
      will(returnValue(mockMember));
      oneOf(mockCache).listDiskStoresIncludingRegionOwned();
      will(returnValue(Collections.emptyList()));
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final ListDiskStoresFunction function = createListDiskStoresFunction(mockCache);

    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertEquals(1, results.size());

    final Set<DiskStoreDetails> diskStoreDetails = (Set<DiskStoreDetails>) results.get(0);

    assertNotNull(diskStoreDetails);
    assertTrue(diskStoreDetails.isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteOnMemberWithANonGemFireCache() throws Throwable {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final ListDiskStoresFunction function = createListDiskStoresFunction(mockCache);

    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertEquals(1, results.size());

    final Set<DiskStoreDetails> diskStoreDetails = (Set<DiskStoreDetails>) results.get(0);

    assertNotNull(diskStoreDetails);
    assertTrue(diskStoreDetails.isEmpty());
  }

  @Test(expected = RuntimeException.class)
  public void testExecuteThrowsRuntimeException() throws Throwable {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getMyId();
      will(returnValue(mockMember));
      oneOf(mockCache).listDiskStoresIncludingRegionOwned();
      will(throwException(new RuntimeException("expected")));
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final ListDiskStoresFunction function = createListDiskStoresFunction(mockCache);

    function.execute(mockFunctionContext);

    try {
      testResultSender.getResults();
    }
    catch (Throwable throwable) {
      assertTrue(throwable instanceof RuntimeException);
      assertEquals("expected", throwable.getMessage());
      throw throwable;
    }
  }

  private static class TestListDiskStoresFunction extends ListDiskStoresFunction {

    private final Cache cache;

    public TestListDiskStoresFunction(final Cache cache) {
      assert cache != null : "The Cache cannot be null!";
      this.cache = cache;
    }

    @Override
    protected Cache getCache() {
      return cache;
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
