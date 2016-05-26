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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexStatistics;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.lang.Filter;
import com.gemstone.gemfire.internal.lang.ObjectUtils;
import com.gemstone.gemfire.internal.util.CollectionUtils;
import com.gemstone.gemfire.management.internal.cli.domain.IndexDetails;
import com.gemstone.gemfire.management.internal.cli.domain.IndexDetails.IndexStatisticsDetails;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The ListIndexFunctionJUnitTest class is test suite of test cases testing the contract and functionality of the
 * ListIndexFunction GemFire function.
 * </p>
 * </p>
 * @see com.gemstone.gemfire.management.internal.cli.functions.ListIndexFunction
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.jmock.lib.legacy.ClassImposteriser
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class ListIndexFunctionJUnitTest {

  private Mockery mockContext;
  private AtomicLong mockCounter;

  @Before
  public void setup() {
    mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
    }};
    mockCounter = new AtomicLong(0l);
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  private void assertIndexDetailsEquals(final IndexDetails expectedIndexDetails, final IndexDetails actualIndexDetails) {
    assertEquals(expectedIndexDetails.getFromClause(), actualIndexDetails.getFromClause());
    assertEquals(expectedIndexDetails.getIndexedExpression(), actualIndexDetails.getIndexedExpression());
    assertEquals(expectedIndexDetails.getIndexName(), actualIndexDetails.getIndexName());
    assertIndexStatisticsDetailsEquals(expectedIndexDetails.getIndexStatisticsDetails(), actualIndexDetails.getIndexStatisticsDetails());
    assertEquals(expectedIndexDetails.getIndexType(), actualIndexDetails.getIndexType());
    assertEquals(expectedIndexDetails.getMemberId(), actualIndexDetails.getMemberId());
    assertEquals(expectedIndexDetails.getMemberName(), actualIndexDetails.getMemberName());
    assertEquals(expectedIndexDetails.getProjectionAttributes(), actualIndexDetails.getProjectionAttributes());
    assertEquals(expectedIndexDetails.getRegionName(), actualIndexDetails.getRegionName());
    assertEquals(expectedIndexDetails.getRegionPath(), actualIndexDetails.getRegionPath());
  }

  private void assertIndexStatisticsDetailsEquals(final IndexStatisticsDetails expectedIndexStatisticsDetails, final IndexStatisticsDetails actualIndexStatisticsDetails) {
    if (expectedIndexStatisticsDetails != null) {
      assertNotNull(actualIndexStatisticsDetails);
      assertEquals(expectedIndexStatisticsDetails.getNumberOfKeys(), actualIndexStatisticsDetails.getNumberOfKeys());
      assertEquals(expectedIndexStatisticsDetails.getNumberOfUpdates(), actualIndexStatisticsDetails.getNumberOfUpdates());
      assertEquals(expectedIndexStatisticsDetails.getNumberOfValues(), actualIndexStatisticsDetails.getNumberOfValues());
      assertEquals(expectedIndexStatisticsDetails.getTotalUpdateTime(), actualIndexStatisticsDetails.getTotalUpdateTime());
      assertEquals(expectedIndexStatisticsDetails.getTotalUses(), actualIndexStatisticsDetails.getTotalUses());

    } else {
      assertNull(actualIndexStatisticsDetails);
    }
  }

  private IndexDetails createIndexDetails(final String memberId,
                                            final String regionPath,
                                            final String indexName,
                                            final IndexType indexType,
                                            final String fromClause,
                                            final String indexedExpression,
                                            final String memberName,
                                            final String projectionAttributes,
                                            final String regionName) {
    final IndexDetails indexDetails = new IndexDetails(memberId, regionPath, indexName);

    indexDetails.setFromClause(fromClause);
    indexDetails.setIndexedExpression(indexedExpression);
    indexDetails.setIndexType(indexType);
    indexDetails.setMemberName(memberName);
    indexDetails.setProjectionAttributes(projectionAttributes);
    indexDetails.setRegionName(regionName);

    return indexDetails;
  }

  private IndexStatisticsDetails createIndexStatisticsDetails(final Long numberOfKeys,
                                                                final Long numberOfUpdates,
                                                                final Long numberOfValues,
                                                                final Long totalUpdateTime,
                                                                final Long totalUses) {
    final IndexStatisticsDetails indexStatisticsDetails = new IndexStatisticsDetails();

    indexStatisticsDetails.setNumberOfKeys(numberOfKeys);
    indexStatisticsDetails.setNumberOfUpdates(numberOfUpdates);
    indexStatisticsDetails.setNumberOfValues(numberOfValues);
    indexStatisticsDetails.setTotalUpdateTime(totalUpdateTime);
    indexStatisticsDetails.setTotalUses(totalUses);

    return indexStatisticsDetails;
  }

  private ListIndexFunction createListIndexFunction(final Cache cache) {
    return new TestListIndexFunction(cache);
  }

  private Index createMockIndex(final IndexDetails indexDetails) {
    final Index mockIndex = mockContext.mock(Index.class, "Index " + indexDetails.getIndexName() + " " + mockCounter.getAndIncrement());

    final Region mockRegion = mockContext.mock(Region.class, "Region " + indexDetails.getRegionPath() + " " + mockCounter.getAndIncrement());

    mockContext.checking(new Expectations() {{
      oneOf(mockIndex).getFromClause();
      will(returnValue(indexDetails.getFromClause()));
      oneOf(mockIndex).getIndexedExpression();
      will(returnValue(indexDetails.getIndexedExpression()));
      oneOf(mockIndex).getName();
      will(returnValue(indexDetails.getIndexName()));
      oneOf(mockIndex).getProjectionAttributes();
      will(returnValue(indexDetails.getProjectionAttributes()));
      exactly(2).of(mockIndex).getRegion();
      will(returnValue(mockRegion));
      oneOf(mockIndex).getType();
      will(returnValue(getIndexType(indexDetails.getIndexType())));
      oneOf(mockRegion).getName();
      will(returnValue(indexDetails.getRegionName()));
      oneOf(mockRegion).getFullPath();
      will(returnValue(indexDetails.getRegionPath()));
    }});

    if (indexDetails.getIndexStatisticsDetails() != null) {
      final IndexStatistics mockIndexStatistics = mockContext.mock(IndexStatistics.class, "IndexStatistics " + indexDetails.getIndexName() + " " + mockCounter.getAndIncrement());

      mockContext.checking(new Expectations() {{
        exactly(2).of(mockIndex).getStatistics();
        will(returnValue(mockIndexStatistics));
        oneOf(mockIndexStatistics).getNumUpdates();
        will(returnValue(indexDetails.getIndexStatisticsDetails().getNumberOfUpdates()));
        oneOf(mockIndexStatistics).getNumberOfKeys();
        will(returnValue(indexDetails.getIndexStatisticsDetails().getNumberOfKeys()));
        oneOf(mockIndexStatistics).getNumberOfValues();
        will(returnValue(indexDetails.getIndexStatisticsDetails().getNumberOfValues()));
        oneOf(mockIndexStatistics).getTotalUpdateTime();
        will(returnValue(indexDetails.getIndexStatisticsDetails().getTotalUpdateTime()));
        oneOf(mockIndexStatistics).getTotalUses();
        will(returnValue(indexDetails.getIndexStatisticsDetails().getTotalUses()));
      }});

    } else {
      mockContext.checking(new Expectations() {{
        oneOf(mockIndex).getStatistics();
        will(returnValue(null));
      }});
    }

    return mockIndex;
  }

  private IndexType getIndexType(final IndexDetails.IndexType type) {
    switch (type) {
      case FUNCTIONAL:
        return IndexType.FUNCTIONAL;
      case PRIMARY_KEY:
        return IndexType.PRIMARY_KEY;
      default:
        return null;
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecute() throws Throwable {
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";

    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedSystem mockDistributedSystem = mockContext.mock(DistributedSystem.class, "DistributedSystem");
    final DistributedMember mockDistributedMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final IndexDetails indexDetailsOne = createIndexDetails(memberId, "/Employees", "empIdIdx", IndexType.PRIMARY_KEY,
      "/Employees", "id", memberName, "id, firstName, lastName", "Employees");

    indexDetailsOne.setIndexStatisticsDetails(createIndexStatisticsDetails(10124l, 4096l, 10124l, 1284100l, 280120l));

    final IndexDetails indexDetailsTwo = createIndexDetails(memberId, "/Employees", "empGivenNameIdx",
      IndexType.FUNCTIONAL, "/Employees", "lastName", memberName, "id, firstName, lastName", "Employees");

    final IndexDetails indexDetailsThree = createIndexDetails(memberId, "/Contractors", "empIdIdx",
      IndexType.PRIMARY_KEY, "/Contrators", "id", memberName, "id, firstName, lastName", "Contractors");

    indexDetailsThree.setIndexStatisticsDetails(createIndexStatisticsDetails(1024l, 256l, 20248l, 768001l, 24480l));

    final IndexDetails indexDetailsFour = createIndexDetails(memberId, "/Employees", "empIdIdx", IndexType.FUNCTIONAL,
      "/Employees", "emp_id", memberName, "id, surname, givenname", "Employees");

    final Set<IndexDetails> expectedIndexDetailsSet = new HashSet<IndexDetails>(3);

    expectedIndexDetailsSet.add(indexDetailsOne);
    expectedIndexDetailsSet.add(indexDetailsTwo);
    expectedIndexDetailsSet.add(indexDetailsThree);

    final QueryService mockQueryService = mockContext.mock(QueryService.class, "QueryService");

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getDistributedSystem();
      will(returnValue(mockDistributedSystem));
      oneOf(mockCache).getQueryService();
      will(returnValue(mockQueryService));
      oneOf(mockDistributedSystem).getDistributedMember();
      will(returnValue(mockDistributedMember));
      exactly(4).of(mockDistributedMember).getId();
      will(returnValue(memberId));
      exactly(4).of(mockDistributedMember).getName();
      will(returnValue(memberName));
      oneOf(mockQueryService).getIndexes();
      will(returnValue(Arrays.asList(createMockIndex(indexDetailsOne), createMockIndex(indexDetailsTwo),
        createMockIndex(indexDetailsThree), createMockIndex(indexDetailsFour))));
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final ListIndexFunction function = createListIndexFunction(mockCache);

    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertEquals(1, results.size());

    final Set<IndexDetails> actualIndexDetailsSet = (Set<IndexDetails>) results.get(0);

    assertNotNull(actualIndexDetailsSet);
    assertEquals(expectedIndexDetailsSet.size(), actualIndexDetailsSet.size());

    for (final IndexDetails expectedIndexDetails : expectedIndexDetailsSet) {
      final IndexDetails actualIndexDetails = CollectionUtils.findBy(actualIndexDetailsSet, new Filter<IndexDetails>() {
        @Override public boolean accept(final IndexDetails indexDetails) {
          return ObjectUtils.equals(expectedIndexDetails, indexDetails);
        }
      });

      assertNotNull(actualIndexDetails);
      assertIndexDetailsEquals(expectedIndexDetails, actualIndexDetails);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteWithNoIndexes() throws Throwable {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedSystem mockDistributedSystem = mockContext.mock(DistributedSystem.class, "DistributedSystem");
    final DistributedMember mockDistributedMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final QueryService mockQueryService = mockContext.mock(QueryService.class, "QueryService");

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getDistributedSystem();
      will(returnValue(mockDistributedSystem));
      oneOf(mockCache).getQueryService();
      will(returnValue(mockQueryService));
      oneOf(mockDistributedSystem).getDistributedMember();
      will(returnValue(mockDistributedMember));
      oneOf(mockQueryService).getIndexes();
      will(returnValue(Collections.emptyList()));
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final ListIndexFunction function = createListIndexFunction(mockCache);

    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertEquals(1, results.size());

    final Set<IndexDetails> actualIndexDetailsSet = (Set<IndexDetails>) results.get(0);

    assertNotNull(actualIndexDetailsSet);
    assertTrue(actualIndexDetailsSet.isEmpty());
  }

  @Test(expected = RuntimeException.class)
  public void testExecuteThrowsException() throws Throwable {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final DistributedSystem mockDistributedSystem = mockContext.mock(DistributedSystem.class, "DistributedSystem");
    final DistributedMember mockDistributedMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final QueryService mockQueryService = mockContext.mock(QueryService.class, "QueryService");

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getDistributedSystem();
      will(returnValue(mockDistributedSystem));
      oneOf(mockCache).getQueryService();
      will(returnValue(mockQueryService));
      oneOf(mockDistributedSystem).getDistributedMember();
      will(returnValue(mockDistributedMember));
      oneOf(mockQueryService).getIndexes();
      will(throwException(new RuntimeException("expected")));
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final ListIndexFunction function = createListIndexFunction(mockCache);

    function.execute(mockFunctionContext);

    try {
      testResultSender.getResults();
    } catch (Throwable t) {
      assertTrue(t instanceof RuntimeException);
      assertEquals("expected", t.getMessage());
      throw t;
    }
  }

  private static class TestListIndexFunction extends ListIndexFunction {

    private final Cache cache;

    protected TestListIndexFunction(final Cache cache) {
      assert cache != null : "The Cache cannot be null!";
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
