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
import org.apache.geode.management.internal.cli.domain.IndexDetails;
import org.apache.geode.management.internal.cli.functions.ListIndexFunction;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * The ListIndexCommandJUnitTest class is a test suite of test cases testing the contract and
 * functionality of the ListIndexCommand class.
 * </p>
 *
 * @see org.apache.geode.management.internal.cli.commands.ClearDefinedIndexesCommand
 * @see org.apache.geode.management.internal.cli.commands.CreateDefinedIndexesCommand
 * @see org.apache.geode.management.internal.cli.commands.CreateIndexCommand
 * @see org.apache.geode.management.internal.cli.commands.DefineIndexCommand
 * @see org.apache.geode.management.internal.cli.commands.DestroyIndexCommand
 * @see org.apache.geode.management.internal.cli.commands.ListIndexCommand
 * @see org.apache.geode.management.internal.cli.domain.IndexDetails
 * @see org.apache.geode.management.internal.cli.functions.ListIndexFunction
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.jmock.lib.legacy.ClassImposteriser
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
public class ListIndexCommandJUnitTest {
  private Mockery mockContext;

  @Before
  public void setup() {
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

  private ListIndexCommand createListIndexCommand(final InternalCache cache,
      final Execution functionExecutor) {
    return new TestListIndexCommands(cache, functionExecutor);
  }

  private IndexDetails createIndexDetails(final String memberId, final String indexName) {
    return new IndexDetails(memberId, "/Employees", indexName);
  }

  @Test
  public void testGetIndexListing() {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final AbstractExecution mockFunctionExecutor =
        mockContext.mock(AbstractExecution.class, "Function Executor");

    final ResultCollector mockResultCollector =
        mockContext.mock(ResultCollector.class, "ResultCollector");

    final IndexDetails indexDetails1 = createIndexDetails("memberOne", "empIdIdx");
    final IndexDetails indexDetails2 = createIndexDetails("memberOne", "empLastNameIdx");
    final IndexDetails indexDetails3 = createIndexDetails("memberTwo", "empDobIdx");

    final List<IndexDetails> expectedIndexDetails =
        Arrays.asList(indexDetails1, indexDetails2, indexDetails3);

    final List<Set<IndexDetails>> results = new ArrayList<>(2);

    results.add(CollectionUtils.asSet(indexDetails2, indexDetails1));
    results.add(CollectionUtils.asSet(indexDetails3));

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).setIgnoreDepartedMembers(with(equal(true)));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(ListIndexFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(results));
      }
    });

    final ListIndexCommand commands = createListIndexCommand(mockCache, mockFunctionExecutor);
    final List<IndexDetails> actualIndexDetails = commands.getIndexListing();

    assertNotNull(actualIndexDetails);
    assertEquals(expectedIndexDetails, actualIndexDetails);
  }

  @Test(expected = RuntimeException.class)
  public void testGetIndexListingThrowsRuntimeException() {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");
    final Execution mockFunctionExecutor = mockContext.mock(Execution.class, "Function Executor");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).execute(with(aNonNull(ListIndexFunction.class)));
        will(throwException(new RuntimeException("expected")));
      }
    });

    final ListIndexCommand commands = createListIndexCommand(mockCache, mockFunctionExecutor);

    try {
      commands.getIndexListing();
    } catch (RuntimeException expected) {
      assertEquals("expected", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testGetIndexListingReturnsFunctionInvocationTargetExceptionInResults() {
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "InternalCache");

    final AbstractExecution mockFunctionExecutor =
        mockContext.mock(AbstractExecution.class, "Function Executor");

    final ResultCollector mockResultCollector =
        mockContext.mock(ResultCollector.class, "ResultCollector");

    final IndexDetails indexDetails = createIndexDetails("memberOne", "empIdIdx");

    final List<IndexDetails> expectedIndexDetails = Collections.singletonList(indexDetails);

    final List<Object> results = new ArrayList<>(2);

    results.add(CollectionUtils.asSet(indexDetails));
    results.add(new FunctionInvocationTargetException("expected"));

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionExecutor).setIgnoreDepartedMembers(with(equal(true)));
        oneOf(mockFunctionExecutor).execute(with(aNonNull(ListIndexFunction.class)));
        will(returnValue(mockResultCollector));
        oneOf(mockResultCollector).getResult();
        will(returnValue(results));
      }
    });

    final ListIndexCommand commands = createListIndexCommand(mockCache, mockFunctionExecutor);

    final List<IndexDetails> actualIndexDetails = commands.getIndexListing();

    assertNotNull(actualIndexDetails);
    assertEquals(expectedIndexDetails, actualIndexDetails);
  }

  private static class TestListIndexCommands extends ListIndexCommand {
    private final InternalCache cache;
    private final Execution functionExecutor;

    TestListIndexCommands(final InternalCache cache, final Execution functionExecutor) {
      assert cache != null : "The InternalCache cannot be null!";
      assert functionExecutor != null : "The function executor cannot be null!";
      this.cache = cache;
      this.functionExecutor = functionExecutor;
    }

    @Override
    public Cache getCache() {
      return this.cache;
    }

    @Override
    public Set<DistributedMember> getAllMembers() {
      assertSame(getCache(), cache);
      return Collections.emptySet();
    }

    @Override
    public Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
      Assert.assertNotNull(members);
      return functionExecutor;
    }
  }
}
