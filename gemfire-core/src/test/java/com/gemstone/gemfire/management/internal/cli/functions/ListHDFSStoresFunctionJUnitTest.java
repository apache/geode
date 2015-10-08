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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.internal.cli.functions.ListHDFSStoresFunction.HdfsStoreDetails;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * The ListHDFSStoreFunctionJUnitTest test suite class tests the contract and functionality of the
 * ListHDFSStoreFunction.
 * </p>
 * @author Namrata Thanvi
 * @see com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl
 * @see com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder
 * @see com.gemstone.gemfire.management.internal.cli.functions.ListHDFSStoresFunction
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 */

@Category({IntegrationTest.class, HoplogTest.class})
public class ListHDFSStoresFunctionJUnitTest {
  private Mockery mockContext;

  @Before
  public void setup() {
    mockContext = new Mockery() {
      {
        setImposteriser(ClassImposteriser.INSTANCE);
      }
    };
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  
  @Test
  public void testExecute() throws Throwable {
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";
    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");
    final TestResultSender testResultSender = new TestResultSender();

    final HDFSStoreImpl mockHdfsStoreOne = mockContext.mock(HDFSStoreImpl.class, "HDFSStoreOne");
    final HDFSStoreImpl mockHdfsStoreTwo = mockContext.mock(HDFSStoreImpl.class, "HDFSStoreTwo");
    final HDFSStoreImpl mockHdfsStoreThree = mockContext.mock(HDFSStoreImpl.class, "HDFSStoreThree");

    final List<HDFSStoreImpl> mockHdfsStores = new ArrayList<HDFSStoreImpl>();

    mockHdfsStores.add(mockHdfsStoreOne);
    mockHdfsStores.add(mockHdfsStoreTwo);
    mockHdfsStores.add(mockHdfsStoreThree);

    final List<String> storeNames = new ArrayList<String>();
    storeNames.add("hdfsStoreOne");
    storeNames.add("hdfsStoreTwo");
    storeNames.add("hdfsStoreThree");

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).getHDFSStores();
        will(returnValue(mockHdfsStores));
        exactly(3).of(mockMember).getId();
        will(returnValue(memberId));
        exactly(3).of(mockMember).getName();
        will(returnValue(memberName));
        oneOf(mockHdfsStoreOne).getName();
        will(returnValue(storeNames.get(0)));       
        oneOf(mockHdfsStoreTwo).getName();
        will(returnValue(storeNames.get(1)));        
        oneOf(mockHdfsStoreThree).getName();
        will(returnValue(storeNames.get(2)));        
        oneOf(mockFunctionContext).getResultSender();
        will(returnValue(testResultSender));
      }
    });

    final ListHDFSStoresFunction function = createListHDFSStoresFunction(mockCache, mockMember);

    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertEquals(1, results.size());

    final Set<HdfsStoreDetails> listHdfsStoreFunctionresults = (Set<HdfsStoreDetails>)results.get(0);

    assertNotNull(listHdfsStoreFunctionresults);
    assertEquals(3, listHdfsStoreFunctionresults.size());

    Collections.sort(storeNames);

    for (HdfsStoreDetails listHdfsStoreFunctionresult : listHdfsStoreFunctionresults) {
      assertTrue(storeNames.contains(listHdfsStoreFunctionresult.getStoreName()));
      assertTrue(storeNames.remove(listHdfsStoreFunctionresult.getStoreName()));
      assertEquals(memberId, listHdfsStoreFunctionresult.getMemberId());
      assertEquals(memberName, listHdfsStoreFunctionresult.getMemberName());
    }
  }
  
  
  @Test(expected = CacheClosedException.class)
  public void testExecuteOnMemberWithNoCache() throws Throwable {
    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "MockFunctionContext");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final TestListHDFSStoresFunction testListHdfsStoresFunction = 
          new TestListHDFSStoresFunction(mockContext.mock(Cache.class, "MockCache"), mockMember) {
      @Override protected Cache getCache() {
        throw new CacheClosedException("Expected");
      }
    };

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    testListHdfsStoresFunction.execute(mockFunctionContext);

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
  public void testExecuteOnMemberHavingNoHDFSStores() throws Throwable {
    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).getHDFSStores();
      will(returnValue(Collections.emptyList()));
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final ListHDFSStoresFunction function = createListHDFSStoresFunction(mockCache, mockMember);

    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertEquals(1, results.size());

    final Set<HdfsStoreDetails> hdfsStoreDetails = (Set<HdfsStoreDetails>) results.get(0);

    assertNotNull(hdfsStoreDetails);
    assertTrue(hdfsStoreDetails.isEmpty());
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteOnMemberWithANonGemFireCache() throws Throwable {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionContext).getResultSender();
        will(returnValue(testResultSender));
      }
    });

    final ListHDFSStoresFunction function = createListHDFSStoresFunction(mockCache, null);

    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertEquals(1, results.size());

    final Set<HdfsStoreDetails> hdfsStoreDetails = (Set<HdfsStoreDetails>)results.get(0);

    assertNotNull(hdfsStoreDetails);
    assertTrue(hdfsStoreDetails.isEmpty());
  }
  
  
  @Test(expected = RuntimeException.class)
  public void testExecuteThrowsRuntimeException() throws Throwable {
    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).getHDFSStores();
        will(throwException(new RuntimeException("expected")));
        oneOf(mockFunctionContext).getResultSender();
        will(returnValue(testResultSender));
      }
    });

    final ListHDFSStoresFunction function = createListHDFSStoresFunction(mockCache, mockMember);

    function.execute(mockFunctionContext);

    try {
      testResultSender.getResults();
    } catch (Throwable throwable) {
      assertTrue(throwable instanceof RuntimeException);
      assertEquals("expected", throwable.getMessage());
      throw throwable;
    }
  }
  
  protected ListHDFSStoresFunction createListHDFSStoresFunction(final Cache cache, DistributedMember member) {
    return new TestListHDFSStoresFunction(cache, member);
  }
    
  protected static class TestListHDFSStoresFunction extends ListHDFSStoresFunction {
    private static final long serialVersionUID = 1L;

    private final Cache cache;

    DistributedMember member;

    @Override
    protected DistributedMember getDistributedMemberId(Cache cache) {
      return member;
    }

    public TestListHDFSStoresFunction(final Cache cache, DistributedMember member) {
      assert cache != null: "The Cache cannot be null!";
      this.cache = cache;
      this.member = member;
    }

    @Override
    protected Cache getCache() {
      return cache;
    }
  }

  protected static class TestResultSender implements ResultSender {

    private final List<Object> results = new LinkedList<Object>();

    private Throwable t;

    protected List<Object> getResults() throws Throwable {
      if (t != null) {
        throw t;
      }
      return Collections.unmodifiableList(results);
    }

    public void lastResult(final Object lastResult) {
      results.add(lastResult);
    }

    public void sendResult(final Object oneResult) {
      results.add(oneResult);
    }

    public void sendException(final Throwable t) {
      this.t = t;
    }
  }
}
