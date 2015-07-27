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
import java.util.Properties;

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
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.cli.commands.HDFSStoreCommandsJUnitTest;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;

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
public class CreateHDFSStoreFunctionJUnitTest {

  private static final Logger logger = LogService.getLogger();

  private Mockery mockContext;

  private static Properties props = new Properties();
  
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
    
    final TestResultSender testResultSender = new TestResultSender();
    
    final HDFSStoreImpl mockHdfsStore = createMockHDFSStoreImpl(mockContext, "hdfsStoreName", "hdfs://localhost:9000", "testDir",
        1024, 20, .25f, null, 20, 20, null, false, 0, 1024, false, false, true, 20, 20, 10, 100);
    
    final HDFSStoreConfigHolder mockHdfsStoreConfigHolder = HDFSStoreCommandsJUnitTest.createMockHDFSStoreConfigHolder(
        mockContext, "hdfsStoreName", "hdfs://localhost:9000", "testDir", 1024, 20, .25f, null, 40, 40, null, false, 0,
        2048, true, true, true, 40, 40, 40, 800);
    
    final CreateHDFSStoreFunction function = new TestCreateHDFSStoreFunction(mockCache, mockMember, xmlEntity , mockHdfsStore);

    mockContext.checking(new Expectations() {
      {
        oneOf(mockMember).getId();
        will(returnValue(memberId));
        exactly(2).of(mockMember).getName();
        will(returnValue(memberName));
        oneOf(mockFunctionContext).getArguments();
        will(returnValue(mockHdfsStoreConfigHolder));
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
  public void testExecuteOnMemberWithNoCache() throws Throwable {

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "MockFunctionContext");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class, "Cache");
    final XmlEntity xmlEntity = mockContext.mock(XmlEntity.class, "XmlEntity");

    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";
    
    final TestResultSender testResultSender = new TestResultSender();
    final HDFSStoreImpl mockHdfsStore = createMockHDFSStoreImpl(mockContext, "hdfsStoreName", "hdfs://localhost:9000", "testDir",
        1024, 20, .25f, null, 20, 20, null, false, 0, 1024, false, false, true, 20, 20, 10, 100);
    
    final HDFSStoreConfigHolder mockHdfsStoreConfigHolder = HDFSStoreCommandsJUnitTest.createMockHDFSStoreConfigHolder(mockContext, "hdfsStoreName",
        "hdfs://localhost:9000", "testDir", 1024, 20, .25f, null, 40, 40, null, false, 0, 2048, true, true, true, 40,
        40, 40, 800);
    
    final CreateHDFSStoreFunction function = new TestCreateHDFSStoreFunction(mockCache, mockMember, xmlEntity , mockHdfsStore) {
      @Override
      protected Cache getCache() {
        throw new CacheClosedException("Expected");
      }
    };

    mockContext.checking(new Expectations() {
      {
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

    final FunctionContext mockFunctionContext = mockContext.mock(FunctionContext.class, "MockFunctionContext");
    final DistributedMember mockMember = mockContext.mock(DistributedMember.class, "DistributedMember");
    final GemFireCacheImpl mockCache = mockContext.mock(GemFireCacheImpl.class, "Cache");
    final XmlEntity xmlEntity = mockContext.mock(XmlEntity.class, "XmlEntity");

    final String memberId = "mockMemberId";
    final String memberName = "mockMemberName";
    
    final TestResultSender testResultSender = new TestResultSender();
    final HDFSStoreImpl mockHdfsStore = createMockHDFSStoreImpl(mockContext, "hdfsStoreName", "hdfs://localhost:9000", "testDir",
        1024, 20, .25f, null, 20, 20, null, false, 0, 1024, false, false, true, 20, 20, 10, 100);
    
    final HDFSStoreConfigHolder mockHdfsStoreConfigHolder = HDFSStoreCommandsJUnitTest.createMockHDFSStoreConfigHolder(
        mockContext, "hdfsStoreName", "hdfs://localhost:9000", "testDir", 1024, 20, .25f, null, 40, 40, null, false, 0,
        2048, true, true, true, 40, 40, 40, 800);
    
    final CreateHDFSStoreFunction function = new TestCreateHDFSStoreFunction(mockCache, mockMember, xmlEntity , mockHdfsStore) {
      @Override
      protected Cache getCache() {
        throw new RuntimeException("expected");
      }
    };

    mockContext.checking(new Expectations() {
      {
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
    assertEquals("expected", result.getThrowable().getMessage());

  }

  public static HDFSStoreImpl createMockHDFSStoreImpl(Mockery mockContext, final String storeName, final String namenode, final String homeDir,
      final int maxFileSize, final int fileRolloverInterval, final float blockCachesize, final String clientConfigFile,
      final int batchSize, final int batchInterval, final String diskStoreName, final boolean syncDiskwrite,
      final int dispatcherThreads, final int maxMemory, final boolean bufferPersistent, final boolean minorCompact,
      final boolean majorCompact, final int majorCompactionInterval, final int majorCompactionThreads,
      final int minorCompactionThreads, final int purgeInterval) {

    HDFSStoreImpl mockHdfsStore = mockContext.mock(HDFSStoreImpl.class, "HDFSStoreImpl");

    HDFSStoreCommandsJUnitTest.createMockStore(mockContext, mockHdfsStore, storeName, namenode, homeDir, maxFileSize,
        fileRolloverInterval, minorCompact, minorCompactionThreads, majorCompact, majorCompactionThreads,
        majorCompactionInterval, purgeInterval, blockCachesize, clientConfigFile, batchSize, batchInterval,
        diskStoreName, syncDiskwrite, dispatcherThreads, maxMemory, bufferPersistent);
    
    return mockHdfsStore;
  }

  protected static class TestCreateHDFSStoreFunction extends CreateHDFSStoreFunction {
    private static final long serialVersionUID = 1L;

    private final Cache cache;

    private final DistributedMember member;

    private final XmlEntity xml;
    
    private final HDFSStoreImpl hdfsStore;

    public TestCreateHDFSStoreFunction(Cache cache, DistributedMember member, XmlEntity xml , HDFSStoreImpl hdfsStore) {
      this.cache = cache;
      this.member = member;
      this.xml = xml;
      this.hdfsStore = hdfsStore;
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
    protected HDFSStoreImpl createHdfsStore(Cache cache, HDFSStoreConfigHolder configHolder){
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
