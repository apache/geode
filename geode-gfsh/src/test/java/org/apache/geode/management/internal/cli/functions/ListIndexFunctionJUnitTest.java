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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.lang.ObjectUtils;
import org.apache.geode.internal.util.CollectionUtils;
import org.apache.geode.management.internal.cli.domain.IndexDetails;
import org.apache.geode.management.internal.cli.domain.IndexDetails.IndexStatisticsDetails;

/**
 * The ListIndexFunctionJUnitTest class is test suite of test cases testing the contract and
 * functionality of the ListIndexFunction GemFire function.
 * </p>
 * </p>
 *
 * @see org.apache.geode.management.internal.cli.functions.ListIndexFunction
 * @see org.junit.Test
 * @since GemFire 7.0
 */
public class ListIndexFunctionJUnitTest {
  private AtomicLong counter;
  private QueryService mockQueryService;
  private TestResultSender testResultSender;
  private FunctionContext<Void> mockFunctionContext;
  private final String mockMemberId = "mockMemberId";
  private final String mockMemberName = "mockMemberName";

  @Before
  @SuppressWarnings("unchecked")
  public void setup() {
    counter = new AtomicLong(0L);
    testResultSender = new TestResultSender();
    mockQueryService = mock(QueryService.class, "QueryService");
    mockFunctionContext = mock(FunctionContext.class, "FunctionContext");

    final Cache mockCache = mock(Cache.class, "Cache");
    final DistributedSystem mockDistributedSystem =
        mock(DistributedSystem.class, "DistributedSystem");
    final DistributedMember mockDistributedMember =
        mock(DistributedMember.class, "DistributedMember");
    when(mockCache.getQueryService()).thenReturn(mockQueryService);
    when(mockCache.getDistributedSystem()).thenReturn(mockDistributedSystem);
    when(mockDistributedSystem.getDistributedMember()).thenReturn(mockDistributedMember);
    when(mockDistributedMember.getId()).thenReturn(mockMemberId);
    when(mockDistributedMember.getName()).thenReturn(mockMemberName);
    when(mockFunctionContext.getCache()).thenReturn(mockCache);
    when(mockFunctionContext.getResultSender()).thenReturn(testResultSender);
  }

  private void assertIndexStatisticsDetailsEquals(
      final IndexStatisticsDetails actualIndexStatisticsDetails,
      final IndexStatisticsDetails expectedIndexStatisticsDetails) {
    if (expectedIndexStatisticsDetails != null) {
      assertThat(actualIndexStatisticsDetails).isNotNull();
      assertThat(actualIndexStatisticsDetails.getNumberOfKeys())
          .isEqualTo(expectedIndexStatisticsDetails.getNumberOfKeys());
      assertThat(actualIndexStatisticsDetails.getNumberOfUpdates())
          .isEqualTo(expectedIndexStatisticsDetails.getNumberOfUpdates());
      assertThat(actualIndexStatisticsDetails.getNumberOfValues())
          .isEqualTo(expectedIndexStatisticsDetails.getNumberOfValues());
      assertThat(actualIndexStatisticsDetails.getTotalUpdateTime())
          .isEqualTo(expectedIndexStatisticsDetails.getTotalUpdateTime());
      assertThat(actualIndexStatisticsDetails.getTotalUses())
          .isEqualTo(expectedIndexStatisticsDetails.getTotalUses());
    } else {
      assertThat(actualIndexStatisticsDetails).isNull();
    }
  }

  private void assertIndexDetailsEquals(final IndexDetails actualIndexDetails,
      final IndexDetails expectedIndexDetails) {
    assertThat(actualIndexDetails).isNotNull();
    assertThat(actualIndexDetails.getFromClause()).isEqualTo(expectedIndexDetails.getFromClause());
    assertThat(actualIndexDetails.getIndexedExpression())
        .isEqualTo(expectedIndexDetails.getIndexedExpression());
    assertThat(actualIndexDetails.getIndexName()).isEqualTo(expectedIndexDetails.getIndexName());
    assertThat(actualIndexDetails.getIndexType()).isEqualTo(expectedIndexDetails.getIndexType());
    assertThat(actualIndexDetails.getMemberId()).isEqualTo(expectedIndexDetails.getMemberId());
    assertThat(actualIndexDetails.getMemberName()).isEqualTo(expectedIndexDetails.getMemberName());
    assertThat(actualIndexDetails.getProjectionAttributes())
        .isEqualTo(expectedIndexDetails.getProjectionAttributes());
    assertThat(actualIndexDetails.getRegionName()).isEqualTo(expectedIndexDetails.getRegionName());
    assertThat(actualIndexDetails.getRegionPath()).isEqualTo(expectedIndexDetails.getRegionPath());
    assertIndexStatisticsDetailsEquals(actualIndexDetails.getIndexStatisticsDetails(),
        expectedIndexDetails.getIndexStatisticsDetails());
  }

  @SuppressWarnings("deprecation")
  private IndexDetails createIndexDetails(final String regionPath, final String indexName,
      final org.apache.geode.cache.query.IndexType indexType, final String fromClause,
      final String indexedExpression,
      final String projectionAttributes, final String regionName) {
    final IndexDetails indexDetails = new IndexDetails(mockMemberId, regionPath, indexName);
    indexDetails.setFromClause(fromClause);
    indexDetails.setIndexedExpression(indexedExpression);
    indexDetails.setIndexType(indexType);
    indexDetails.setMemberName(mockMemberName);
    indexDetails.setProjectionAttributes(projectionAttributes);
    indexDetails.setRegionName(regionName);

    return indexDetails;
  }

  private IndexStatisticsDetails createIndexStatisticsDetails(final Long numberOfKeys,
      final Long numberOfUpdates, final Long numberOfValues, final Long totalUpdateTime,
      final Long totalUses) {
    final IndexStatisticsDetails indexStatisticsDetails = new IndexStatisticsDetails();
    indexStatisticsDetails.setNumberOfKeys(numberOfKeys);
    indexStatisticsDetails.setNumberOfUpdates(numberOfUpdates);
    indexStatisticsDetails.setNumberOfValues(numberOfValues);
    indexStatisticsDetails.setTotalUpdateTime(totalUpdateTime);
    indexStatisticsDetails.setTotalUses(totalUses);

    return indexStatisticsDetails;
  }

  @SuppressWarnings("unchecked")
  private Index createMockIndex(final IndexDetails indexDetails) {
    @SuppressWarnings("rawtypes")
    final Region mockRegion =
        mock(Region.class, "Region " + indexDetails.getRegionPath() + " " + counter
            .getAndIncrement());
    when(mockRegion.getName()).thenReturn(indexDetails.getRegionName());
    when(mockRegion.getFullPath()).thenReturn(indexDetails.getRegionPath());

    final Index mockIndex = mock(Index.class, "Index " + indexDetails.getIndexName() + " " + counter
        .getAndIncrement());
    when(mockIndex.getRegion()).thenReturn(mockRegion);
    when(mockIndex.getType()).thenReturn(indexDetails.getIndexType());
    when(mockIndex.getName()).thenReturn(indexDetails.getIndexName());
    when(mockIndex.getFromClause()).thenReturn(indexDetails.getFromClause());
    when(mockIndex.getIndexedExpression()).thenReturn(indexDetails.getIndexedExpression());
    when(mockIndex.getProjectionAttributes()).thenReturn(indexDetails.getProjectionAttributes());

    if (indexDetails.getIndexStatisticsDetails() != null) {
      final IndexStatistics mockIndexStatistics = mock(IndexStatistics.class,
          "IndexStatistics " + indexDetails.getIndexName() + " " + counter
              .getAndIncrement());
      when(mockIndex.getStatistics()).thenReturn(mockIndexStatistics);
      when(mockIndexStatistics.getNumUpdates())
          .thenReturn(indexDetails.getIndexStatisticsDetails().getNumberOfUpdates());
      when(mockIndexStatistics.getNumberOfKeys())
          .thenReturn(indexDetails.getIndexStatisticsDetails().getNumberOfKeys());
      when(mockIndexStatistics.getNumberOfValues())
          .thenReturn(indexDetails.getIndexStatisticsDetails().getNumberOfValues());
      when(mockIndexStatistics.getTotalUpdateTime())
          .thenReturn(indexDetails.getIndexStatisticsDetails().getTotalUpdateTime());
      when(mockIndexStatistics.getTotalUses())
          .thenReturn(indexDetails.getIndexStatisticsDetails().getTotalUses());
    } else {
      when(mockIndex.getStatistics()).thenReturn(null);
    }

    return mockIndex;
  }

  @Test
  @SuppressWarnings({"unchecked", "deprecation"})
  public void testExecute() throws Throwable {
    // Expected Results
    final IndexDetails indexDetailsOne = createIndexDetails("/Employees", "empIdIdx",
        org.apache.geode.cache.query.IndexType.PRIMARY_KEY, "/Employees", "id",
        "id, firstName, lastName", "Employees");
    indexDetailsOne.setIndexStatisticsDetails(
        createIndexStatisticsDetails(10124L, 4096L, 10124L, 1284100L, 280120L));
    final IndexDetails indexDetailsTwo = createIndexDetails("/Employees", "empGivenNameIdx",
        org.apache.geode.cache.query.IndexType.FUNCTIONAL, "/Employees", "lastName",
        "id, firstName, lastName", "Employees");
    final IndexDetails indexDetailsThree = createIndexDetails("/Contractors", "empIdIdx",
        org.apache.geode.cache.query.IndexType.PRIMARY_KEY, "/Contrators", "id",
        "id, firstName, lastName", "Contractors");
    indexDetailsThree.setIndexStatisticsDetails(
        createIndexStatisticsDetails(1024L, 256L, 20248L, 768001L, 24480L));
    final IndexDetails indexDetailsFour = createIndexDetails("/Employees", "empIdIdx",
        org.apache.geode.cache.query.IndexType.FUNCTIONAL, "/Employees", "emp_id",
        "id, surname, givenname", "Employees");
    final Set<IndexDetails> expectedIndexDetailsSet =
        new HashSet<>(Arrays.asList(indexDetailsOne, indexDetailsTwo, indexDetailsThree));

    // Prepare Mocks
    List<Index> indexes =
        Arrays.asList(createMockIndex(indexDetailsOne), createMockIndex(indexDetailsTwo),
            createMockIndex(indexDetailsThree), createMockIndex(indexDetailsFour));
    when(mockQueryService.getIndexes()).thenReturn(indexes);

    // Execute Function and Assert Results
    final ListIndexFunction function = new ListIndexFunction();
    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();
    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(1);

    final Set<IndexDetails> actualIndexDetailsSet = (Set<IndexDetails>) results.get(0);
    assertThat(actualIndexDetailsSet).isNotNull();
    assertThat(actualIndexDetailsSet.size()).isEqualTo(expectedIndexDetailsSet.size());

    for (final IndexDetails expectedIndexDetails : expectedIndexDetailsSet) {
      final IndexDetails actualIndexDetails = CollectionUtils.findBy(actualIndexDetailsSet,
          indexDetails -> ObjectUtils.equals(expectedIndexDetails, indexDetails));
      assertIndexDetailsEquals(actualIndexDetails, expectedIndexDetails);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteWithNoIndexes() throws Throwable {
    // Prepare Mocks
    when(mockQueryService.getIndexes()).thenReturn(Collections.emptyList());

    // Execute Function and assert results
    final ListIndexFunction function = new ListIndexFunction();
    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();
    assertThat(results).isNotNull();
    assertThat(results.size()).isEqualTo(1);

    final Set<IndexDetails> actualIndexDetailsSet = (Set<IndexDetails>) results.get(0);
    assertThat(actualIndexDetailsSet).isNotNull();
    assertThat(actualIndexDetailsSet.isEmpty()).isTrue();
  }

  @Test
  public void testExecuteThrowsException() {
    // Prepare Mocks
    when(mockQueryService.getIndexes()).thenThrow(new RuntimeException("Mocked Exception"));

    // Execute Function and assert results
    final ListIndexFunction function = new ListIndexFunction();
    function.execute(mockFunctionContext);
    assertThatThrownBy(() -> testResultSender.getResults()).isInstanceOf(RuntimeException.class)
        .hasMessage("Mocked Exception");
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
