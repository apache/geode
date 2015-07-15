/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.management.internal.cli.functions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.util.HDFSStoreNotFoundException;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;

/**
 * The DescribeHDFSStoreFunctionJUnitTest test suite class tests the contract
 * and functionality of the DescribeHDFSStoreFunction class. </p>
 * 
 * @author Namrata Thanvi
 * @see com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl
 * @see com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder
 * @see com.gemstone.gemfire.management.internal.cli.functions.DescribeHDFSStoreFunction
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 */
@SuppressWarnings( { "unused" })
@Category({IntegrationTest.class, HoplogTest.class})
public class DescribeHDFSStoreFunctionJUnitTest {

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
    final String hdfsStoreName = "mockHdfsStore";
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";

    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class, "Cache");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final HDFSStoreImpl mockHdfsStore = createMockHDFSStore(hdfsStoreName, "hdfs://localhost:9000", "testDir", 1024, 20, .25f,
        null, 20, 20, null, false, 0, 1024, false, false, true, 20, 20, 10, 100);

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");
    final LogService mockLogService = mockContext.mock(LogService.class, "LogService");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {
      {
        oneOf(mockCache).findHDFSStore(hdfsStoreName);
        will(returnValue(mockHdfsStore));
        oneOf(mockMember).getName();
        will(returnValue(memberName));
        oneOf(mockFunctionContext).getArguments();
        will(returnValue(hdfsStoreName));
        oneOf(mockFunctionContext).getResultSender();
        will(returnValue(testResultSender));
      }
    });

    final DescribeHDFSStoreFunction function = createDescribeHDFSStoreFunction(mockCache, mockMember);

    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertEquals(1, results.size());

    final HDFSStoreConfigHolder hdfsStoreDetails = (HDFSStoreConfigHolder)results.get(0);

    assertNotNull(hdfsStoreDetails);
    assertEquals(hdfsStoreName, hdfsStoreDetails.getName());
    assertEquals("hdfs://localhost:9000", hdfsStoreDetails.getNameNodeURL());
    assertEquals("testDir", hdfsStoreDetails.getHomeDir());
    assertEquals(1024, hdfsStoreDetails.getWriteOnlyFileRolloverSize());
    assertEquals(20, hdfsStoreDetails.getWriteOnlyFileRolloverInterval());
    assertFalse(hdfsStoreDetails.getMinorCompaction());
    assertEquals("0.25", Float.toString(hdfsStoreDetails.getBlockCacheSize()));
    assertNull(hdfsStoreDetails.getHDFSClientConfigFile());
    assertTrue(hdfsStoreDetails.getMajorCompaction());
    assertEquals(20, hdfsStoreDetails.getMajorCompactionInterval());
    assertEquals(20, hdfsStoreDetails.getMajorCompactionThreads());
    assertEquals(10, hdfsStoreDetails.getMinorCompactionThreads());
    assertEquals(100, hdfsStoreDetails.getPurgeInterval());

    assertEquals(20, hdfsStoreDetails.getBatchSize());
    assertEquals(20, hdfsStoreDetails.getBatchInterval());
    assertNull(hdfsStoreDetails.getDiskStoreName());
    assertFalse(hdfsStoreDetails.getSynchronousDiskWrite());
    assertEquals(0, hdfsStoreDetails.getDispatcherThreads());
    assertEquals(1024, hdfsStoreDetails.getMaxMemory());
    assertFalse(hdfsStoreDetails.getBufferPersistent());
  }

  
  @Test
  public void testExecuteOnMemberHavingANonGemFireCache() throws Throwable {
    final Cache mockCache = mockContext.mock(Cache.class, "Cache");

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      exactly(0).of(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
      
    }});

    final DescribeHDFSStoreFunction function = createDescribeHDFSStoreFunction(mockCache , mockMember);

    function.execute(mockFunctionContext);

    final List<?> results = testResultSender.getResults();

    assertNotNull(results);
    assertTrue(results.isEmpty());
  }

  
  @Test(expected = HDFSStoreNotFoundException.class)
  public void testExecuteThrowingResourceNotFoundException() throws Throwable{    
    final String hdfsStoreName = "testHdfsStore";
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";

    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).findHDFSStore(hdfsStoreName);
      will(returnValue(null));
      oneOf(mockMember).getName();
      will(returnValue(memberName));
      oneOf(mockFunctionContext).getArguments();
      will(returnValue(hdfsStoreName));
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final DescribeHDFSStoreFunction function = createDescribeHDFSStoreFunction(mockCache,mockMember);

    function.execute(mockFunctionContext);

    try {
      testResultSender.getResults();
    }
    catch (HDFSStoreNotFoundException e) {
      assertEquals(String.format("A hdfs store with name (%1$s) was not found on member (%2$s).",
        hdfsStoreName, memberName), e.getMessage());
      throw e;
    }
  }
  
  
  @Test(expected = RuntimeException.class)
  public void testExecuteThrowingRuntimeException() throws Throwable {
    final String hdfsStoreName = "testHdfsStore";
    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";

    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class, "Cache");

    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "FunctionContext");

    final TestResultSender testResultSender = new TestResultSender();

    mockContext.checking(new Expectations() {{
      oneOf(mockCache).findHDFSStore(hdfsStoreName);
      will(throwException(new RuntimeException("ExpectedStrings")));
      oneOf(mockMember).getName();
      will(returnValue(memberName));
      oneOf(mockFunctionContext).getArguments();
      will(returnValue(hdfsStoreName));
      oneOf(mockFunctionContext).getResultSender();
      will(returnValue(testResultSender));
    }});

    final DescribeHDFSStoreFunction function = createDescribeHDFSStoreFunction(mockCache, mockMember);

    function.execute(mockFunctionContext);

    try {
      testResultSender.getResults();
    }
    catch (RuntimeException e) {
      assertEquals("ExpectedStrings", e.getMessage());
      throw e;
    }
  }
  
  
  protected HDFSStoreImpl createMockHDFSStore(final String storeName, final String namenode, final String homeDir,
      final int maxFileSize, final int fileRolloverInterval, final float blockCachesize, final String clientConfigFile,
      final int batchSize, final int batchInterval, final String diskStoreName, final boolean syncDiskwrite,
      final int dispatcherThreads, final int maxMemory, final boolean bufferPersistent, final boolean minorCompact,
      final boolean majorCompact, final int majorCompactionInterval, final int majorCompactionThreads,
      final int minorCompactionThreads, final int purgeInterval) {

    final HDFSStoreImpl mockHdfsStore = mockContext.mock(HDFSStoreImpl.class, storeName);

    mockContext.checking(new Expectations() {
      {
        oneOf(mockHdfsStore).getMajorCompaction();
        will(returnValue(majorCompact));
        oneOf(mockHdfsStore).getMajorCompactionInterval();
        will(returnValue(majorCompactionInterval));
        oneOf(mockHdfsStore).getMajorCompactionThreads();
        will(returnValue(majorCompactionThreads));
        oneOf(mockHdfsStore).getMinorCompactionThreads();
        will(returnValue(minorCompactionThreads));
        oneOf(mockHdfsStore).getPurgeInterval();
        will(returnValue(purgeInterval));
        oneOf(mockHdfsStore).getInputFileCountMax();
        will(returnValue(10));
        oneOf(mockHdfsStore).getInputFileSizeMax();
        will(returnValue(1024));
        oneOf(mockHdfsStore).getInputFileCountMin();
        will(returnValue(2));
        oneOf(mockHdfsStore).getBatchSize();
        will(returnValue(batchSize));
        oneOf(mockHdfsStore).getBatchInterval();
        will(returnValue(batchInterval));
        oneOf(mockHdfsStore).getDiskStoreName();
        will(returnValue(diskStoreName));
        oneOf(mockHdfsStore).getSynchronousDiskWrite();
        will(returnValue(syncDiskwrite));
        oneOf(mockHdfsStore).getBufferPersistent();
        will(returnValue(bufferPersistent));
        oneOf(mockHdfsStore).getDispatcherThreads();
        will(returnValue(dispatcherThreads));
        oneOf(mockHdfsStore).getMaxMemory();
        will(returnValue(maxMemory));
        oneOf(mockHdfsStore).getName();
        will(returnValue(storeName));
        oneOf(mockHdfsStore).getNameNodeURL();
        will(returnValue(namenode));
        oneOf(mockHdfsStore).getHomeDir();
        will(returnValue(homeDir));
        oneOf(mockHdfsStore).getWriteOnlyFileRolloverSize();
        will(returnValue(maxFileSize));
        oneOf(mockHdfsStore).getWriteOnlyFileRolloverInterval();
        will(returnValue(fileRolloverInterval));
        oneOf(mockHdfsStore).getMinorCompaction();
        will(returnValue(minorCompact));
        oneOf(mockHdfsStore).getBlockCacheSize();
        will(returnValue(blockCachesize));
        allowing(mockHdfsStore).getHDFSClientConfigFile();
        will(returnValue(clientConfigFile));
      }
    });
    return mockHdfsStore;
  }

  protected TestDescribeHDFSStoreFunction createDescribeHDFSStoreFunction(final Cache cache, DistributedMember member) {
    return new TestDescribeHDFSStoreFunction(cache, member);
  }

  protected static class TestDescribeHDFSStoreFunction extends DescribeHDFSStoreFunction {
    private static final long serialVersionUID = 1L;

    private final Cache cache;

    private final DistributedMember member;

    public TestDescribeHDFSStoreFunction(final Cache cache, DistributedMember member) {
      this.cache = cache;
      this.member = member;
    }

    @Override
    protected Cache getCache() {
      return this.cache;
    }

    @Override
    protected DistributedMember getDistributedMemberId(Cache cache) {
      return member;
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
