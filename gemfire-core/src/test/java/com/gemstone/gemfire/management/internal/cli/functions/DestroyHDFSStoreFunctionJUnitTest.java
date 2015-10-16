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
import static org.junit.Assert.assertNull;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.Logger;
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
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;

/**
 * The DestroyHDFSStoreFunctionJUnitTest test suite class tests the contract and
 * functionality of the DestroyHDFSStoreFunction class. </p>
 * 
 * @author Namrata Thanvi
 * @see com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl
 * @see com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder
 * @see com.gemstone.gemfire.management.internal.cli.functions.DestroyHDFSStoreFunction
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 */
@SuppressWarnings( { "unused" })
@Category({IntegrationTest.class, HoplogTest.class})
public class DestroyHDFSStoreFunctionJUnitTest {

  private static final Logger logger = LogService.getLogger();

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
    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final XmlEntity xmlEntity = mockContext.mock(XmlEntity.class, "XmlEntity");
    final HDFSStoreImpl mockHdfsStore = mockContext.mock(HDFSStoreImpl.class, "HDFSStoreImpl");
    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";
    final TestResultSender testResultSender = new TestResultSender();
    final DestroyHDFSStoreFunction function = createDestroyHDFSStoreFunction(mockCache, mockMember, xmlEntity);

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).findHDFSStore(hdfsStoreName);
        will(returnValue(mockHdfsStore));
        one(mockHdfsStore).destroy();
        oneOf(mockMember).getId();
        will(returnValue(memberId));
        exactly(2).of(mockMember).getName();
        will(returnValue(memberName));
        oneOf(mockFunctionContext).getArguments();
        will(returnValue(hdfsStoreName));
        oneOf(mockFunctionContext).getResultSender();
        will(returnValue(testResultSender));
      }
    });

    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertEquals(1, results.size());

    final CliFunctionResult result = (CliFunctionResult)results.get(0);
    assertEquals(memberName, result.getMemberIdOrName());
    assertEquals("Success", result.getMessage());

  }

  @Test
  @SuppressWarnings("unchecked")
  public void testExecuteOnMemberHavingNoHDFSStore() throws Throwable {
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";

    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");
    final XmlEntity xmlEntity = mockContext.mock(XmlEntity.class, "XmlEntity");

    final TestResultSender testResultSender = new TestResultSender();
    final DestroyHDFSStoreFunction function = createDestroyHDFSStoreFunction(mockCache, mockMember, xmlEntity);

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).findHDFSStore(hdfsStoreName);
        will(returnValue(null));
        oneOf(mockMember).getId();
        will(returnValue(memberId));
        exactly(2).of(mockMember).getName();
        will(returnValue(memberName));
        oneOf(mockFunctionContext).getArguments();
        will(returnValue(hdfsStoreName));
        oneOf(mockFunctionContext).getResultSender();
        will(returnValue(testResultSender));
      }
    });

    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertEquals(1, results.size());

    final CliFunctionResult result = (CliFunctionResult)results.get(0);
    assertEquals(memberName, result.getMemberIdOrName());
    assertEquals("Hdfs store not found on this member", result.getMessage());
  }

  @Test
  public void testExecuteOnMemberWithNoCache() throws Throwable {
    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "MockFunctionContext");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class, "Cache");
    final XmlEntity xmlEntity = mockContext.mock(XmlEntity.class, "XmlEntity");

    final String hdfsStoreName = "mockHdfsStore";

    final TestResultSender testResultSender = new TestResultSender();
    final DestroyHDFSStoreFunction function = new TestDestroyHDFSStoreFunction(mockCache, mockMember, xmlEntity) {
      private static final long serialVersionUID = 1L;

      @Override
      protected Cache getCache() {
        throw new CacheClosedException("Expected");
      }
    };

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionContext).getArguments();
        will(returnValue(hdfsStoreName));
        oneOf(mockFunctionContext).getResultSender();
        will(returnValue(testResultSender));
      }
    });

    function.execute(mockFunctionContext);
    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertEquals(1, results.size());

    final CliFunctionResult result = (CliFunctionResult)results.get(0);
    assertEquals("", result.getMemberIdOrName());
    assertNull(result.getMessage());
  }

  @Test
  public void testExecuteHandleRuntimeException() throws Throwable {
    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");
    final XmlEntity xmlEntity = mockContext.mock(XmlEntity.class, "XmlEntity");

    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";

    final TestResultSender testResultSender = new TestResultSender();
    final DestroyHDFSStoreFunction function = createDestroyHDFSStoreFunction(mockCache, mockMember, xmlEntity);

    mockContext.checking(new Expectations() {
      {
        oneOf(mockMember).getId();
        will(returnValue(memberId));
        exactly(2).of(mockMember).getName();
        will(returnValue(memberName));
        oneOf(mockFunctionContext).getArguments();
        will(returnValue(hdfsStoreName));
        oneOf(mockCache).findHDFSStore(hdfsStoreName);
        will(throwException(new RuntimeException("expected")));
        oneOf(mockFunctionContext).getResultSender();
        will(returnValue(testResultSender));
      }
    });

    function.execute(mockFunctionContext);
    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertEquals(1, results.size());

    final CliFunctionResult result = (CliFunctionResult)results.get(0);
    assertEquals(memberName, result.getMemberIdOrName());
    assertEquals("expected", result.getThrowable().getMessage());

  }

  protected TestDestroyHDFSStoreFunction createDestroyHDFSStoreFunction(final Cache cache, DistributedMember member,
      XmlEntity xml) {
    return new TestDestroyHDFSStoreFunction(cache, member, xml);
  }

  protected static class TestDestroyHDFSStoreFunction extends DestroyHDFSStoreFunction {
    private static final long serialVersionUID = 1L;

    private final Cache cache;

    private final DistributedMember member;

    private final XmlEntity xml;

    public TestDestroyHDFSStoreFunction(final Cache cache, DistributedMember member, XmlEntity xml) {
      this.cache = cache;
      this.member = member;
      this.xml = xml;
    }

    @Override
    protected Cache getCache() {
      return this.cache;
    }

    @Override
    protected DistributedMember getDistributedMember(Cache cache) {
      return member;
    }

    @Override
    protected XmlEntity getXMLEntity(String storeName) {
      return xml;
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
