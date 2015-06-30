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
import com.gemstone.gemfire.cache.hdfs.internal.HDFSEventQueueAttributesImpl;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreConfigHolder.AbstractHDFSCompactionConfigHolder;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.internal.logging.LogService;
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
    final HDFSStoreImpl mockHdfsStore = createMockHDFSStoreImpl("hdfsStoreName", "hdfs://localhost:9000", "testDir",
        1024, 20, .25f, null, 20, 20, null, false, 0, 1024, false, false, true, 20, 20, 10, 100);
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

  protected HDFSStoreImpl createMockHDFSStoreImpl(final String storeName, final String namenode, final String homeDir,
      final int maxFileSize, final int fileRolloverInterval, final float blockCachesize, final String clientConfigFile,
      final int batchSize, final int batchInterval, final String diskStoreName, final boolean syncDiskwrite,
      final int dispatcherThreads, final int maxMemory, final boolean bufferPersistent, final boolean minorCompact,
      final boolean majorCompact, final int majorCompactionInterval, final int majorCompactionThreads,
      final int minorCompactionThreads, final int purgeInterval) {

    HDFSStoreImpl mockHdfsStore = mockContext.mock(HDFSStoreImpl.class, "HDFSStoreImpl");

    HDFSEventQueueAttributesImpl mockEventQueue = createMockEventQueue("hdfsStore_" + storeName, batchSize,
        batchInterval, diskStoreName, syncDiskwrite, dispatcherThreads, maxMemory, bufferPersistent);

    AbstractHDFSCompactionConfigHolder mockCompactionConfig = createMockHDFSCompactionConfigHolder("hdfsStore_"
        + storeName, minorCompact, majorCompact, majorCompactionInterval, majorCompactionThreads,
        minorCompactionThreads, purgeInterval);

    mockHdfsStore = (HDFSStoreImpl)createMockStore(mockHdfsStore, storeName, namenode, homeDir, maxFileSize,
        fileRolloverInterval, minorCompact, blockCachesize, clientConfigFile, mockCompactionConfig, mockEventQueue);
    return mockHdfsStore;
  }

  protected HDFSStoreConfigHolder createMockHDFSStoreConfigHolder(final String storeName, final String namenode,
      final String homeDir, final int maxFileSize, final int fileRolloverInterval, final float blockCachesize,
      final String clientConfigFile, final int batchSize, final int batchInterval, final String diskStoreName,
      final boolean syncDiskwrite, final int dispatcherThreads, final int maxMemory, final boolean bufferPersistent,
      final boolean minorCompact, final boolean majorCompact, final int majorCompactionInterval,
      final int majorCompactionThreads, final int minorCompactionThreads, final int purgeInterval) {

    HDFSStoreConfigHolder mockHdfsStore = mockContext.mock(HDFSStoreConfigHolder.class, "HDFSStoreConfigHolder");
    HDFSEventQueueAttributesImpl mockEventQueue = createMockEventQueue("hdfsStoreConfig_" + storeName, batchSize,
        batchInterval, diskStoreName, syncDiskwrite, dispatcherThreads, maxMemory, bufferPersistent);

    AbstractHDFSCompactionConfigHolder mockCompactionConfig = createMockHDFSCompactionConfigHolder("hdfsStoreConfig_"
        + storeName, minorCompact, majorCompact, majorCompactionInterval, majorCompactionThreads,
        minorCompactionThreads, purgeInterval);

    mockHdfsStore = (HDFSStoreConfigHolder)createMockStore(mockHdfsStore, storeName, namenode, homeDir, maxFileSize,
        fileRolloverInterval, minorCompact, blockCachesize, clientConfigFile, mockCompactionConfig, mockEventQueue);
    return mockHdfsStore;

  }

  private HDFSEventQueueAttributesImpl createMockEventQueue(final String name, final int batchSize,
      final int batchInterval, final String diskStoreName, final boolean syncDiskwrite, final int dispatcherThreads,
      final int maxMemory, final boolean bufferPersistent) {

    final HDFSEventQueueAttributesImpl mockEventQueue = mockContext.mock(HDFSEventQueueAttributesImpl.class, name
        + "EventQueueImpl");

    mockContext.checking(new Expectations() {
      {
        allowing(mockEventQueue).getBatchSizeMB();
        will(returnValue(batchSize));
        allowing(mockEventQueue).getBatchTimeInterval();
        will(returnValue(batchInterval));
        allowing(mockEventQueue).getDiskStoreName();
        will(returnValue(diskStoreName));
        allowing(mockEventQueue).isDiskSynchronous();
        will(returnValue(syncDiskwrite));
        allowing(mockEventQueue).isPersistent();
        will(returnValue(bufferPersistent));
        allowing(mockEventQueue).getDispatcherThreads();
        will(returnValue(dispatcherThreads));
        allowing(mockEventQueue).getMaximumQueueMemory();
        will(returnValue(maxMemory));
      }
    });

    return mockEventQueue;
  }

  private AbstractHDFSCompactionConfigHolder createMockHDFSCompactionConfigHolder(final String name,
      final boolean minorCompact, final boolean majorCompact, final int majorCompactionInterval,
      final int majorCompactionThreads, final int minorCompactionThreads, final int purgeInterval) {

    final AbstractHDFSCompactionConfigHolder mockCompactionConfig = mockContext.mock(
        AbstractHDFSCompactionConfigHolder.class, name + "CompactionConfig");

    mockContext.checking(new Expectations() {
      {
        allowing(mockCompactionConfig).getAutoMajorCompaction();
        will(returnValue(majorCompact));
        allowing(mockCompactionConfig).getMajorCompactionIntervalMins();
        will(returnValue(majorCompactionInterval));
        allowing(mockCompactionConfig).getMajorCompactionMaxThreads();
        will(returnValue(majorCompactionThreads));
        allowing(mockCompactionConfig).getMaxThreads();
        will(returnValue(minorCompactionThreads));
        allowing(mockCompactionConfig).getOldFilesCleanupIntervalMins();
        will(returnValue(purgeInterval));
        allowing(mockCompactionConfig).getMaxInputFileCount();
        will(returnValue(10));
        allowing(mockCompactionConfig).getMaxInputFileSizeMB();
        will(returnValue(1024));
        allowing(mockCompactionConfig).getMinInputFileCount();
        will(returnValue(2));
        allowing(mockCompactionConfig).getCompactionStrategy();
        will(returnValue(null));
      }
    });

    return mockCompactionConfig;
  }

  private HDFSStore createMockStore(final HDFSStore mockStore, final String storeName, final String namenode,
      final String homeDir, final int maxFileSize, final int fileRolloverInterval, final boolean minorCompact, 
      final float blockCachesize,final String clientConfigFile, 
      final AbstractHDFSCompactionConfigHolder mockCompactionConfig,
      final HDFSEventQueueAttributesImpl mockEventQueue) {

    mockContext.checking(new Expectations() {
      {
        allowing(mockStore).getName();
        will(returnValue(storeName));
        allowing(mockStore).getNameNodeURL();
        will(returnValue(namenode));
        allowing(mockStore).getHomeDir();
        will(returnValue(homeDir));
        allowing(mockStore).getMaxFileSize();
        will(returnValue(maxFileSize));
        allowing(mockStore).getFileRolloverInterval();
        will(returnValue(fileRolloverInterval));
        allowing(mockStore).getMinorCompaction();
        will(returnValue(minorCompact));
        allowing(mockStore).getBlockCacheSize();
        will(returnValue(blockCachesize));
        allowing(mockStore).getHDFSClientConfigFile();
        will(returnValue(clientConfigFile));
        allowing(mockStore).getHDFSEventQueueAttributes();
        will(returnValue(mockEventQueue));
        allowing(mockStore).getHDFSCompactionConfig();
        will(returnValue(mockCompactionConfig));
      }
    });
    return mockStore;

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
