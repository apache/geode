/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.commands.HDFSStoreCommandsJUnitTest;
import com.gemstone.gemfire.management.internal.cli.functions.AlterHDFSStoreFunction.AlterHDFSStoreAttributes;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * The AlterHDFSStoreFunctionJUnitTest test suite class tests the contract and
 * functionality of the AlterHDFSStoreFunction class. </p>
 * 
 * @author Namrata Thanvi
 * @see com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl
 * @see com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder
 * @see com.gemstone.gemfire.management.internal.cli.functions.AlterHDFSStoreFunction
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 */
@SuppressWarnings( { "unused" })
@Category({IntegrationTest.class, HoplogTest.class})
public class AlterHDFSStoreFunctionJUnitTest {

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
    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");
    final XmlEntity xmlEntity = mockContext.mock(XmlEntity.class, "XmlEntity");

    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";

    final AlterHDFSStoreFunction function = createAlterHDFSStoreFunction(mockCache, mockMember, xmlEntity);
    final TestResultSender testResultSender = new TestResultSender();
    final HDFSStoreImpl mockHdfsStore = CreateHDFSStoreFunctionJUnitTest.createMockHDFSStoreImpl(mockContext,
        "hdfsStoreName", "hdfs://localhost:9000", "testDir", 1024, 20, .25f, null, 20, 20, null, false, 0, 1024, false,
        false, true, 20, 20, 10, 100);
	final AlterHDFSStoreAttributes alterHDFSStoreAttributes = new AlterHDFSStoreAttributes(
				"mockStore", 100, 100, false, false, 100, 100, 100, 100, 100,
				100);

    mockContext.checking(new Expectations() {
      {
        oneOf(mockMember).getId();
        will(returnValue(memberId));
        exactly(2).of(mockMember).getName();
        will(returnValue(memberName));        
        oneOf(mockFunctionContext).getArguments();
        will(returnValue(alterHDFSStoreAttributes));
        oneOf(mockCache).findHDFSStore(alterHDFSStoreAttributes.getHdfsUniqueName());
        will(returnValue(mockHdfsStore));
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

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");
    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class, "Cache");
    final XmlEntity xmlEntity = mockContext.mock(XmlEntity.class, "XmlEntity");

    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";

    final TestResultSender testResultSender = new TestResultSender();
    final AlterHDFSStoreFunction function = createAlterHDFSStoreFunction(mockCache, mockMember, xmlEntity);
	final AlterHDFSStoreAttributes alterHDFSStoreAttributes = new AlterHDFSStoreAttributes(
				"mockStore", 100, 100, false, false, 100, 100, 100, 100, 100,
				100);

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).findHDFSStore(alterHDFSStoreAttributes.getHdfsUniqueName());
        will(returnValue(null));       
        oneOf(mockMember).getId();
        will(returnValue(memberId));
        exactly(2).of(mockMember).getName();
        will(returnValue(memberName));
        oneOf(mockFunctionContext).getArguments();
        will(returnValue(alterHDFSStoreAttributes));
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
    final InternalCache mockCache = mockContext.mock(InternalCache.class, "Cache");
    final XmlEntity xmlEntity = mockContext.mock(XmlEntity.class, "XmlEntity");

    final TestResultSender testResultSender = new TestResultSender();
	final AlterHDFSStoreAttributes alterHDFSStoreAttributes = new AlterHDFSStoreAttributes(
				"mockStore", 100, 100, false, false, 100, 100, 100, 100, 100,
				100);

    final AlterHDFSStoreFunction function = new TestAlterHDFSStoreFunction(mockCache, mockMember, xmlEntity) {
      @Override
      protected Cache getCache() {
        throw new CacheClosedException("Expected");
      }
    };

    mockContext.checking(new Expectations() {
      {
        oneOf(mockFunctionContext).getArguments();
        will(returnValue(alterHDFSStoreAttributes));
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

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class, "Cache");
    final XmlEntity xmlEntity = mockContext.mock(XmlEntity.class, "XmlEntity");

    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";
    final TestResultSender testResultSender = new TestResultSender();
    final AlterHDFSStoreFunction function = createAlterHDFSStoreFunction(mockCache, mockMember, xmlEntity);

    final AlterHDFSStoreAttributes alterHDFSStoreAttributes = new AlterHDFSStoreAttributes(
				"mockStore", 100, 100, false, false, 100, 100, 100, 100, 100,
				100);
    mockContext.checking(new Expectations() {
      {
        oneOf(mockMember).getId();
        will(returnValue(memberId));
        exactly(2).of(mockMember).getName();
        will(returnValue(memberName));
        oneOf(mockFunctionContext).getArguments();
        will(returnValue(alterHDFSStoreAttributes));
        oneOf(mockCache).findHDFSStore(alterHDFSStoreAttributes.getHdfsUniqueName());
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

  protected TestAlterHDFSStoreFunction createAlterHDFSStoreFunction(final Cache cache, DistributedMember member,
      XmlEntity xml) {
    return new TestAlterHDFSStoreFunction(cache, member, xml);
  }

  protected static class TestAlterHDFSStoreFunction extends AlterHDFSStoreFunction {
    private static final long serialVersionUID = 1L;

    private final Cache cache;

    private final DistributedMember member;

    private final XmlEntity xml;

    public TestAlterHDFSStoreFunction(final Cache cache, DistributedMember member, XmlEntity xml) {
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

    @Override
    protected HDFSStore alterHdfsStore(HDFSStore hdfsStore, AlterHDFSStoreAttributes alterAttributes) {
      return hdfsStore;
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
