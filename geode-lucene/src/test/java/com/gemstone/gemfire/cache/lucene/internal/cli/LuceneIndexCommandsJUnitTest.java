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
package com.gemstone.gemfire.cache.lucene.internal.cli;
import static com.gemstone.gemfire.internal.lang.StringUtils.trim;
import static org.junit.Assert.*;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionInvocationTargetException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.lucene.internal.cli.functions.LuceneListIndexFunction;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.execute.AbstractExecution;
import com.gemstone.gemfire.internal.util.CollectionUtils;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.functions.ListIndexFunction;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The LuceneIndexCommandsJUnitTest class is a test suite of test cases testing the contract and functionality of the
 * LuceneIndexCommands class.
 * </p>
 * @see LuceneIndexCommands
 * @see LuceneIndexDetails
 * @see com.gemstone.gemfire.cache.lucene.internal.cli.functions.LuceneListIndexFunction
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.jmock.lib.legacy.ClassImposteriser
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class LuceneIndexCommandsJUnitTest {

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

  private LuceneIndexCommands createIndexCommands(final Cache cache, final Execution functionExecutor) {
    return new LuceneTestIndexCommands(cache, functionExecutor);
  }

  private LuceneIndexDetails createIndexDetails(final String indexName, final String regionPath, final String[] searchableFields, final Map<String, Analyzer> fieldAnalyzers) {
    return new LuceneIndexDetails(indexName, regionPath, searchableFields, fieldAnalyzers);
  }

  @Test
  public void testListIndex() {

    final Cache mockCache = mock(Cache.class, "Cache");
    final AbstractExecution mockFunctionExecutor = mock(AbstractExecution.class, "Function Executor");
    final ResultCollector mockResultCollector = mock(ResultCollector.class, "ResultCollector");

    String[] searchableFields={"field1","field2","field3"};
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new StandardAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    fieldAnalyzers.put("field3", null);
    final LuceneIndexDetails indexDetails1 = createIndexDetails("memberFive", "/Employees", searchableFields, fieldAnalyzers);
    final LuceneIndexDetails indexDetails2 = createIndexDetails("memberSix", "/Employees", searchableFields, fieldAnalyzers);
    final LuceneIndexDetails indexDetails3 = createIndexDetails("memberTen", "/Employees", searchableFields, fieldAnalyzers);


    final List<LuceneIndexDetails> expectedIndexDetails = new ArrayList<>(3);
    expectedIndexDetails.add(indexDetails1);
    expectedIndexDetails.add(indexDetails2);
    expectedIndexDetails.add(indexDetails3);

    final List<Set<LuceneIndexDetails>> results = new ArrayList<Set<LuceneIndexDetails>>();

    results.add(CollectionUtils.asSet(indexDetails2, indexDetails1,indexDetails3));


    when(mockFunctionExecutor.execute(isA(LuceneListIndexFunction.class)))
      .thenReturn(mockResultCollector);
    when(mockResultCollector.getResult())
      .thenReturn(results);

    final LuceneIndexCommands commands = createIndexCommands(mockCache, mockFunctionExecutor);

    CommandResult result = (CommandResult) commands.listIndex();
    TabularResultData data = (TabularResultData) result.getResultData();
    assertEquals(Arrays.asList("memberFive", "memberSix", "memberTen"), data.retrieveAllValues("Index Name"));
    assertEquals(Arrays.asList("/Employees", "/Employees", "/Employees"), data.retrieveAllValues("Region Path"));
    assertEquals(Arrays.asList("[field1, field2, field3]", "[field1, field2, field3]", "[field1, field2, field3]"), data.retrieveAllValues("Indexed Fields"));
    assertEquals(Arrays.asList("{field1=StandardAnalyzer, field2=KeywordAnalyzer}", "{field1=StandardAnalyzer, field2=KeywordAnalyzer}", "{field1=StandardAnalyzer, field2=KeywordAnalyzer}"), data.retrieveAllValues("Field Analyzer"));
  }


  private static class LuceneTestIndexCommands extends LuceneIndexCommands {

    private final Cache cache;
    private final Execution functionExecutor;

    protected LuceneTestIndexCommands(final Cache cache, final Execution functionExecutor) {
      assert cache != null : "The Cache cannot be null!";
      assert functionExecutor != null : "The function executor cannot be null!";
      this.cache = cache;
      this.functionExecutor = functionExecutor;
    }

    @Override
    protected Cache getCache() {
      return this.cache;
    }

    @Override
    protected Set<DistributedMember> getMembers(final Cache cache) {
      assertSame(getCache(), cache);
      return Collections.emptySet();
    }

    @Override
    protected Execution getMembersFunctionExecutor(final Set<DistributedMember> members) {
      Assert.assertNotNull(members);
      return functionExecutor;
    }
  }

}
