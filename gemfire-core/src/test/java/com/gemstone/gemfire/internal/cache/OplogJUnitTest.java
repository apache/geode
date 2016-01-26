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
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.Oplog.OPLOG_TYPE;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.jayway.awaitility.Awaitility;

/**
 * Testing Oplog API's
 *
 * @author Asif
 * @author Mitul
 */
@Category(IntegrationTest.class)
public class OplogJUnitTest extends DiskRegionTestingBase
{
  boolean proceed = false;

  private final DiskRegionProperties diskProps = new DiskRegionProperties();

  static final int OP_CREATE = 1;

  static final int OP_MODIFY = 2;

  static final int OP_DEL = 3;

  protected volatile static Random random = new Random();

  protected long expectedOplogSize = Oplog.OPLOG_NEW_ENTRY_BASE_REC_SIZE;

  volatile int totalSuccessfulOperations = 0;

  protected int numCreate = 0;

  protected int numModify = 0;

  protected int numDel = 0;

  protected long delta;

  protected boolean flushOccuredAtleastOnce = false;

  volatile protected boolean assertDone = false;

  boolean failure = false;

  /** The key for entry */
  static final String KEY = "KEY1";

  /** The initial value for key */
  static final String OLD_VALUE = "VAL1";

  /** The updated value for key */
  static final String NEW_VALUE = "VAL2";

  /** The value read from cache using LocalRegion.getValueOnDiskOrBuffer API */
  static volatile String valueRead = null;

  /** Boolean to indicate test to proceed for validation */
  static volatile boolean proceedForValidation = false;

  protected volatile Thread rollerThread = null;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    diskProps.setDiskDirs(dirs);
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
  }

  /**
   * Test method for 'com.gemstone.gemfire.internal.cache.Oplog.isBackup()'
   */
  @Test
  public void testIsBackup()
  {

    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        diskProps);
    if (!((LocalRegion)region).getDiskRegion().isBackup()) {
      fail("Test persist backup not being correctly set for overflow and persist");
    }
    closeDown();

    region = DiskRegionHelperFactory
        .getSyncOverFlowOnlyRegion(cache, diskProps);
    if (((LocalRegion)region).getDiskRegion().isBackup()) {
      fail("Test persist backup not being correctly set for overflow only mode");
    }
    closeDown();

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    if (!((LocalRegion)region).getDiskRegion().isBackup()) {
      fail("Test persist backup not being correctly set for persist only");
    }
    closeDown();
  }

  /*
   * Test method for 'com.gemstone.gemfire.internal.cache.Oplog.useSyncWrites()'
   */
  @Test
  public void testUseSyncWrites()
  {
    boolean result;
    diskProps.setSynchronous(true);
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        diskProps);
    result = ((LocalRegion)region).getAttributes().isDiskSynchronous();
    if (!result) {
      fail("Synchronous is false when it is supposed to be true");
    }
    closeDown();

    region = DiskRegionHelperFactory
        .getSyncOverFlowOnlyRegion(cache, diskProps);

    result = ((LocalRegion)region).getAttributes().isDiskSynchronous();
    if (!result) {
      fail("Synchronous is false when it is supposed to be true");
    }
    closeDown();

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    result = ((LocalRegion)region).getAttributes().isDiskSynchronous();
    if (!result) {
      fail("Synchronous is false when it is supposed to be true");
    }
    closeDown();

    diskProps.setSynchronous(false);
    region = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
        diskProps);

    result = ((LocalRegion)region).getAttributes().isDiskSynchronous();
    if (result) {
      fail("Synchronous is true when it is supposed to be false");
    }

    closeDown();
    region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache,
        diskProps);

    result = ((LocalRegion)region).getAttributes().isDiskSynchronous();
    if (result) {
      fail("Synchronous is true when it is supposed to be false");
    }
    closeDown();

    region = DiskRegionHelperFactory
        .getAsyncPersistOnlyRegion(cache, diskProps);

    result = ((LocalRegion)region).getAttributes().isDiskSynchronous();
    if (result) {
      fail("Synchronous is true when it is supposed to be false");
    }
    closeDown();
  }

  // @todo port testBufferOperations
  /**
   * Asif: Tests the correct behaviour of attributes like byte-threshhold,
   * asynch thread wait time,etc.
   * 'com.gemstone.gemfire.internal.cache.Oplog.bufferOperations()'
   */
//   @Test
// public void testBufferOperations()
//   {
//     boolean result;

//     diskProps.setBytesThreshold(0);
//     region = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
//         diskProps);
//     Oplog.WriterThread writer = ((LocalRegion)region).getDiskRegion()
//         .getChild().getAsynchWriter();
//     long waitTime = writer.getAsynchThreadWaitTime();
//     long buffSize = writer.getBufferSize();
//     result = waitTime == writer.getDefaultAsynchThreadWaitTime()
//         && buffSize == 0;

//     assertTrue("buffer operations is true when it is supposed to be false",
//         result);

//     closeDown();

//     region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache,
//         diskProps);
//     writer = ((LocalRegion)region).getDiskRegion().getChild().getAsynchWriter();
//     waitTime = writer.getAsynchThreadWaitTime();
//     buffSize = writer.getBufferSize();
//     result = waitTime == writer.getDefaultAsynchThreadWaitTime()
//         && buffSize == 0;

//     assertTrue("buffer operations is true when it is supposed to be false",
//         result);
//     closeDown();

//     region = DiskRegionHelperFactory
//         .getAsyncPersistOnlyRegion(cache, diskProps);
//     writer = ((LocalRegion)region).getDiskRegion().getChild().getAsynchWriter();
//     waitTime = writer.getAsynchThreadWaitTime();
//     buffSize = writer.getBufferSize();
//     result = waitTime == writer.getDefaultAsynchThreadWaitTime()
//         && buffSize == 0;

//     assertTrue("buffer operations is true when it is supposed to be false",
//         result);

//     closeDown();

//     diskProps.setBytesThreshold(100);

//     region = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
//         diskProps);

//     writer = ((LocalRegion)region).getDiskRegion().getChild().getAsynchWriter();
//     waitTime = writer.getAsynchThreadWaitTime();
//     buffSize = writer.getBufferSize();
//     result = waitTime <= 0 && buffSize > 0;
//     assertTrue("bufferoperations is false when it is supposed to be true",
//         result);

//     closeDown();

//     region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache,
//         diskProps);

//     writer = ((LocalRegion)region).getDiskRegion().getChild().getAsynchWriter();
//     waitTime = writer.getAsynchThreadWaitTime();
//     buffSize = writer.getBufferSize();
//     result = waitTime <= 0 && buffSize > 0;
//     assertTrue("baufferoperations is false when it is supposed to be true",
//         result);

//     closeDown();

//     region = DiskRegionHelperFactory
//         .getAsyncPersistOnlyRegion(cache, diskProps);

//     writer = ((LocalRegion)region).getDiskRegion().getChild().getAsynchWriter();
//     waitTime = writer.getAsynchThreadWaitTime();
//     buffSize = writer.getBufferSize();
//     result = waitTime <= 0 && buffSize > 0;
//     assertTrue("baufferoperations is false when it is supposed to be true",
//         result);

//     closeDown();
//   }

  /**
   * Test method for 'com.gemstone.gemfire.internal.cache.Oplog.clear(File)'
   */
  @Test
  public void testClear()
  {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        diskProps);
    putTillOverFlow(region);
    region.clear();
    region.close();
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        diskProps);
    assertTrue(" failed in get OverflowAndPersist ",
        region.get(new Integer(0)) == null);
    closeDown();

    region = DiskRegionHelperFactory
        .getSyncOverFlowOnlyRegion(cache, diskProps);
    putTillOverFlow(region);
    region.clear();
    region.close();
    region = DiskRegionHelperFactory
        .getSyncOverFlowOnlyRegion(cache, diskProps);
    assertTrue(" failed in get OverflowOnly ",
        region.get(new Integer(0)) == null);
    closeDown();

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    put100Int();
    region.clear();
    region.close();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    assertTrue(" failed in get PersistOnly ",
        region.get(new Integer(0)) == null);
    closeDown();
  }

  /**
   * Test method for 'com.gemstone.gemfire.internal.cache.Oplog.close()'
   */
  @Test
  public void testClose()
  {
    {
      deleteFiles();
      region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
          diskProps);
      DiskRegion dr = ((LocalRegion)region).getDiskRegion();
      Oplog oplog = dr.testHook_getChild();
      long id = oplog.getOplogId();
      oplog.close();
      // lk should still exist since it locks DiskStore not just one oplog
      //checkIfContainsFile(".lk");

      StatisticsFactory factory = region.getCache().getDistributedSystem();
      Oplog newOplog = new Oplog(id, dr.getOplogSet(), new DirectoryHolder(factory, dirs[0],
          1000, 0));
      dr.getOplogSet().setChild(newOplog);
      closeDown();
    }
    {
      deleteFiles();
      region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache,
          diskProps);
      DiskRegion dr = ((LocalRegion)region).getDiskRegion();
      dr.testHookCloseAllOverflowOplogs();
      // lk should still exist since it locks DiskStore not just one oplog
      //checkIfContainsFile(".lk");
      checkIfContainsFile("OVERFLOW");
      closeDown();
    }
    {
      deleteFiles();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskProps, Scope.LOCAL);
      DiskRegion dr = ((LocalRegion)region).getDiskRegion();
      Oplog oplog = dr.testHook_getChild();
      long id = oplog.getOplogId();
      oplog.close();
      // lk should still exist since it locks DiskStore not just one oplog
      //checkIfContainsFile(".lk");
      StatisticsFactory factory = region.getCache().getDistributedSystem();
      Oplog newOplog = new Oplog(id, dr.getOplogSet(), new DirectoryHolder(factory, dirs[0],
          1000, 2));
      dr.setChild(newOplog);
      closeDown();
    }

  }

  @Override
  protected void closeDown() {
    DiskRegion dr = null;
    if (region != null) {
      dr = ((LocalRegion)region).getDiskRegion();
    }
    super.closeDown();
    if (dr != null) {
      dr.getDiskStore().close();
      ((LocalRegion) region).getGemFireCache().removeDiskStore(dr.getDiskStore());
    }
  }
    
  

  void checkIfContainsFile(String fileExtension)
  {
    for (int i = 0; i < 4; i++) {
      File[] files = dirs[i].listFiles();
      for (int j = 0; j < files.length; j++) {
        if (files[j].getAbsolutePath().endsWith(fileExtension)) {
          fail("file "+ files[j] + " still exists after oplog.close()");
        }
      }
    }
  }

  /**
   * Test method for 'com.gemstone.gemfire.internal.cache.Oplog.destroy()'
   */
  @Test
  public void testDestroy()
  {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        diskProps);
    put100Int();
    putTillOverFlow(region);
    try {
      region.destroy(new Integer(0));
    }
    catch (EntryNotFoundException e1) {
      logWriter.error("Exception occured", e1);
      fail(" Entry not found when it was expected to be there");
    }
    region.close();
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        diskProps);
    assertTrue(" failed in get OverflowAndPersist ",
        region.get(new Integer(0)) == null);
    closeDown();

    region = DiskRegionHelperFactory
        .getSyncOverFlowOnlyRegion(cache, diskProps);
    put100Int();
    putTillOverFlow(region);
    try {
      region.destroy(new Integer(0));
    }
    catch (EntryNotFoundException e1) {
      logWriter.error("Exception occured", e1);
      fail(" Entry not found when it was expected to be there");
    }
    region.close();
    region = DiskRegionHelperFactory
        .getSyncOverFlowOnlyRegion(cache, diskProps);
    assertTrue(" failed in get OverflowOnly ",
        region.get(new Integer(0)) == null);

    closeDown();

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    put100Int();
    try {
      region.destroy(new Integer(0));
    }
    catch (EntryNotFoundException e1) {
      logWriter.error("Exception occured", e1);
      fail(" Entry not found when it was expected to be there");
    }
    region.close();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    assertTrue(" failed in get PersistOnly ",
        region.get(new Integer(0)) == null);
    closeDown();

  }

  /**
   * Test method for 'com.gemstone.gemfire.internal.cache.Oplog.remove(long)'
   */
  @Test
  public void testRemove()
  {
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        diskProps);
    putTillOverFlow(region);
    region.remove(new Integer(0));
    region.close();
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        diskProps);
    assertTrue(" failed in get OverflowAndPersist ",
        region.get(new Integer(0)) == null);
    closeDown();

    region = DiskRegionHelperFactory
        .getSyncOverFlowOnlyRegion(cache, diskProps);
    putTillOverFlow(region);
    region.remove(new Integer(0));
    assertTrue(" failed in get OverflowOnly ",
        region.get(new Integer(0)) == null);
    region.close();
    region = DiskRegionHelperFactory
        .getSyncOverFlowOnlyRegion(cache, diskProps);
    closeDown();

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    put100Int();
    region.remove(new Integer(0));
    assertTrue(" failed in get PersistOnly ",
        region.get(new Integer(0)) == null);
    region.close();
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    closeDown();

  }

  // @todo: port testByteBufferCreationForCreateModifyAndDeleteOperation
  /**
   * This tests the final ByteBuffer object that gets created for synch/Asynch
   * operation for a create / modify & Delete operation
   *
   * @author Asif
   */
//   @Test
//  public void testByteBufferCreationForCreateModifyAndDeleteOperation()
//   {
//     // Asif First create a persist only disk region which is of aysnch
//     // & switch of OplOg type
//     diskProps.setMaxOplogSize(1000);
//     diskProps.setBytesThreshold(500);
//     diskProps.setPersistBackup(true);
//     diskProps.setRolling(false);
//     diskProps.setSynchronous(false);
//     diskProps.setTimeInterval(-1);
//     diskProps.setOverflow(false);

//     region = DiskRegionHelperFactory
//         .getAsyncPersistOnlyRegion(cache, diskProps);
//     byte[] val = new byte[10];
//     for (int i = 0; i < 10; ++i) {
//       val[i] = (byte)i;
//     }
//     region.put(new Integer(1), val);
//     DiskEntry entry = ((DiskEntry)((LocalRegion)region)
//         .basicGetEntry(new Integer(1)));
//     long opKey = entry.getDiskId().getKeyId();
//     // The final position in the Byte Buffer created in Asynch Op should be
//     int createPos = 2 + 4 + val.length;
//     if (opKey > Integer.MAX_VALUE) {
//       createPos += 8;
//     }
//     else if (opKey > Short.MAX_VALUE) {
//       createPos += 4;
//     }
//     else {
//       createPos += 2;
//     }
//     createPos += 4;
//     createPos += EntryEventImpl.serialize(new Integer(1)).length;
//     DiskRegion dr = ((LocalRegion)region).getDiskRegion();
//     Oplog.WriterThread writer = dr.getChild().getAsynchWriter();
//     Oplog.AsyncOp asynchOp = writer
//         .getAsynchOpForEntryFromPendingFlushMap(entry.getDiskId());
//     ByteBuffer bb = asynchOp.getByteBuffer();
//     assertTrue(createPos == bb.position());
//     assertTrue(bb.limit() == bb.capacity());
//     byte val1[] = new byte[20];
//     for (int i = 0; i < 20; ++i) {
//       val1[i] = (byte)i;
//     }
//     region.put(new Integer(1), val1);
//     bb = writer.getAsynchOpForEntryFromPendingFlushMap(entry.getDiskId())
//         .getByteBuffer();
//     createPos += 10;
//     assertTrue(createPos == bb.position());
//     assertTrue(bb.limit() == bb.capacity());
//     byte val2[] = new byte[30];
//     for (int i = 0; i < 30; ++i) {
//       val2[i] = (byte)i;
//     }
//     region.put(new Integer(1), val2);
//     bb = writer.getAsynchOpForEntryFromPendingFlushMap(entry.getDiskId())
//         .getByteBuffer();
//     createPos += 10;
//     assertTrue(createPos == bb.position());
//     assertTrue(bb.limit() == bb.capacity());
//     long opSizeBeforeCreateRemove = dr.getChild().getOplogSize();
//     long pendingFlushSize = dr.getChild().getAsynchWriter()
//         .getCurrentBufferedBytesSize();
//     region.put(new Integer(2), val2);
//     DiskEntry entry2 = ((DiskEntry)((LocalRegion)region)
//         .basicGetEntry(new Integer(2)));
//     bb = writer.getAsynchOpForEntryFromPendingFlushMap(entry2.getDiskId())
//         .getByteBuffer();
//     assertNotNull(bb);
//     region.remove(new Integer(2));
//     assertNull(writer
//         .getAsynchOpForEntryFromPendingFlushMap(entry2.getDiskId()));
//     assertEquals(opSizeBeforeCreateRemove, dr.getChild().getOplogSize());
//     assertEquals(pendingFlushSize, dr.getChild().getAsynchWriter()
//         .getCurrentBufferedBytesSize());

//     closeDown();

//   }

  /**
   * Tests whether the data is written in the right format on the disk
   *
   * @author Asif
   */
  @Test
  public void testFaultInOfValuesFromDisk()
  {
    try {
      // Asif First create a persist only disk region which is of aysnch
      // & switch of OplOg type
      diskProps.setMaxOplogSize(1000);

      diskProps.setPersistBackup(true);
      diskProps.setRolling(false);
      diskProps.setSynchronous(true);
      diskProps.setTimeInterval(-1);
      diskProps.setOverflow(false);

      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskProps, Scope.LOCAL);
      byte[] val = new byte[10];
      for (int i = 0; i < 10; ++i) {
        val[i] = (byte)i;
      }
      region.put(new Integer(1), val);

      DiskEntry entry = ((DiskEntry)((LocalRegion)region)
          .basicGetEntry(new Integer(1)));
      DiskRegion dr = ((LocalRegion)region).getDiskRegion();

      val = (byte[])dr.getNoBuffer(entry.getDiskId());
      for (int i = 0; i < 10; ++i) {
        if (val[i] != (byte)i) {
          fail("Test for fault in from disk failed");
        }
      }
      val = (byte[])DiskStoreImpl.convertBytesAndBitsIntoObject(dr
          .getBytesAndBitsWithoutLock(entry.getDiskId(), true, false));
      for (int i = 0; i < 10; ++i) {
        if (val[i] != (byte)i) {
          fail("Test for fault in from disk failed");
        }
      }
      region.invalidate(new Integer(1));
      assertTrue(dr.getNoBuffer(entry.getDiskId()) == Token.INVALID);

    }
    catch (Exception e) {
      logWriter.error("Exception occured", e);
      fail(e.toString());
    }
    closeDown();
  }

  // @todo port testAsynchWriterTerminationOnSwitch
  /**
   * Tests the termination of asynch writer for an Oplog after the switch has
   * been made
   *
   * @author Asif
   */
//   @Test
//  public void testAsynchWriterTerminationOnSwitch()
//   {
//     // & switch of OplOg type
//     diskProps.setMaxOplogSize(23);
//     diskProps.setBytesThreshold(0);
//     diskProps.setPersistBackup(true);
//     diskProps.setRolling(false);
//     diskProps.setSynchronous(false);
//     diskProps.setTimeInterval(10000);
//     diskProps.setOverflow(false);
//     // diskProps.setDiskDirs(new File[]{new File("test1"), new
//     // File("test2"),
//     // new File("test3")});

//     region = DiskRegionHelperFactory
//         .getAsyncPersistOnlyRegion(cache, diskProps);
//     DiskRegion dr = ((LocalRegion)region).getDiskRegion();
//     Oplog.WriterThread writer = dr.getChild().getAsynchWriter();
//     // Populate data just below the switch over threshhold
//     byte[] val = new byte[5];
//     for (int i = 0; i < 5; ++i) {
//       val[i] = (byte)i;
//     }

//     region.put(new Integer(1), val);

//     DiskEntry entry = ((DiskEntry)((LocalRegion)region)
//         .basicGetEntry(new Integer(1)));
//     long opKey = entry.getDiskId().getKeyId();
//     // The final position in the Byte Buffer created in Asynch Op should be
//     int createPos = 2 + 4 + val.length;
//     if (opKey > Integer.MAX_VALUE) {
//       createPos += 8;
//     }
//     else if (opKey > Short.MAX_VALUE) {
//       createPos += 4;
//     }
//     else {
//       createPos += 2;
//     }
//     createPos += 4;
//     createPos += EntryEventImpl.serialize(new Integer(1)).length;
//     assertTrue(createPos == 22);
//     region.put(new Integer(2), val);
//     DistributedTestCase.join(writer.getThread(), 10 * 1000, null);
//     closeDown();
//   }

  /**
   * Tests the original ByteBufferPool gets transferred to the new Oplog for
   * synch mode
   *
   * @author Asif
   */
  @Test
  public void testByteBufferPoolTransferForSynchMode()
  {
    diskProps.setMaxOplogSize(1024);
    diskProps.setBytesThreshold(0);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setSynchronous(true);
    diskProps.setTimeInterval(10000);
    diskProps.setOverflow(false);
    // diskProps.setDiskDirs(new File[]{new File("test1"), new
    // File("test2"),
    // new File("test3")});

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    DiskRegion dr = ((LocalRegion)region).getDiskRegion();
    //assertNull(dr.getChild().getAsynchWriter());
    // Populate data just below the switch over threshhold
    byte[] val = new byte[5];
    for (int i = 0; i < 5; ++i) {
      val[i] = (byte)i;
    }

    region.put(new Integer(1), val);

    ((LocalRegion)region).basicGetEntry(new Integer(1));
    Oplog old = dr.testHook_getChild();
    ByteBuffer oldWriteBuf = old.getWriteBuf();
    region.forceRolling(); // start a new oplog
    region.put(new Integer(2), val);
    Oplog switched = dr.testHook_getChild();
    assertTrue(old != switched);
    assertEquals(dr.getDiskStore().persistentOplogs.getChild(2), switched);
    assertEquals(oldWriteBuf, switched.getWriteBuf());
    assertEquals(null, old.getWriteBuf());
    closeDown();

  }

  // @todo port this test if needed. ByteBufferPool code is going to change
  /**
   * Tests the ByteBufferPool usage during asynch mode operation & ensuring that
   * GetOperation does not get corrupted data due to returing of ByetBuffer to
   * the pool. There are 5 pre created pools in Oplog . Each pool has size of 1.
   * Out of 5 pools , only one pool is used by the test. Thus there are 4
   * bytebuffers which will always be free. Thus if the asynch writer had
   * initially 8 byte buffers only 4 will be released
   *
   * @author Asif
   */
//   @Test
//  public void testByteBufferPoolUsageForAsynchMode()
//   {
//     final int PRCREATED_POOL_NUM = 5;
//     try {
//       // Asif First create a persist only disk region which is of aysnch
//       // & switch of OplOg type
//       diskProps.setMaxOplogSize(1000);
//       diskProps.setPersistBackup(true);
//       diskProps.setRolling(false);
//       diskProps.setSynchronous(false);
//       diskProps.setTimeInterval(-1);
//       diskProps.setOverflow(false);
//       final int byte_threshold = 500;
//       diskProps.setBytesThreshold(byte_threshold);
//       byte[] val = new byte[50];
//       region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
//           diskProps);
//       for (int i = 0; i < 50; ++i) {
//         val[i] = (byte)i;
//       }
//       region.put(new Integer(1), val);
//       final int singleOpSize = evaluateSizeOfOperationForPersist(
//           new Integer(1), val, ((DiskEntry)((LocalRegion)region)
//               .basicGetEntry(new Integer(1))).getDiskId(), OP_CREATE);

//       final int loopCount = byte_threshold / singleOpSize + 1;
//       LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

//       final Thread th = new Thread(new Runnable() {
//         public void run()
//         {
//           takeRecursiveLockOnAllEntries(1);
//           DiskRegion dr = ((LocalRegion)region).getDiskRegion();
//           // Asif : Sleep for somemore time
//           try {
//             Thread.yield();
//             Thread.sleep(4000);
//           }
//           catch (InterruptedException ie) {
//             logWriter.error("Exception occured", ie);
//             failureCause = "No guarantee of vaildity of result hence failing. Exception = "
//                 + ie;
//             testFailed = true;
//             fail("No guarantee of vaildity of result hence failing. Exception = "
//                 + ie);
//           }

//           // There shoudl beatleast one Pool which has active counts
//           // as two
//           Oplog.ByteBufferPool bbp = null;
//           List pools = dr.getChild().getByteBufferPoolList();
//           Iterator itr = pools.iterator();
//           boolean found = false;
//           while (itr.hasNext()) {
//             bbp = (Oplog.ByteBufferPool)itr.next();
//             int len = bbp.getByteBufferHolderList().size();
//             if (len == (loopCount - (PRCREATED_POOL_NUM - 1))) {
//               found = true;
//               break;
//             }
//           }

//           if (!found) {
//             testFailed = true;
//             failureCause = "Test failed as the Asynch writer did not release ByetBuffer after  get operation";
//             fail("Test failed as the Asynch writer did not release ByetBuffer after  get operation");

//           }

//         }

//         private void takeRecursiveLockOnAllEntries(int key)
//         {
//           // Get the DisKID
//           DiskRegion dr = ((LocalRegion)region).getDiskRegion();
//           if (key > loopCount) {
//             // Interrupt the writer thread so as to start releasing
//             // bytebuffer to pool
//             //dr.getChild().getAsynchWriter().interrupt();
//             // Sleep for a while & check the active ByteBuffer
//             // count.
//             // It should be two
//             try {
//               Thread.yield();
//               Thread.sleep(5000);
//             }
//             catch (InterruptedException ie) {
//               logWriter.error("Exception occured", ie);
//               failureCause = "No guarantee of vaildity of result hence failing. Exception = "
//                   + ie;
//               testFailed = true;
//               fail("No guarantee of vaildity of result hence failing. Exception = "
//                   + ie);
//             }
//             // Check the number of ByteBuffers in the pool.
//             List pools = dr.getChild().getByteBufferPoolList();
//             // There shoudl beatleast one Pool which has active
//             // counts as two
//             Oplog.ByteBufferPool bbp = null;
//             Iterator itr = pools.iterator();
//             boolean found = true;
//             int len = -1;
//             while (itr.hasNext()) {
//               bbp = (Oplog.ByteBufferPool)itr.next();
//               len = bbp.getByteBufferHolderList().size();
//               if (len > 1) {
//                 found = false;
//                 break;
//               }
//             }
//             if (!found) {
//               failureCause = "Test failed as the Asynch writer released ByteBuffer before get operation. The length of byte buffer pool is found to be greater than 0. the length is"
//                   + len;
//               testFailed = true;
//               fail("Test failed as the Asynch writer released ByteBuffer before get operation");
//             }
//           }
//           else {
//             DiskEntry entry = ((DiskEntry)((LocalRegion)region)
//                 .basicGetEntry(new Integer(key)));
//             DiskId id = entry.getDiskId();

//             synchronized (id) {
//               takeRecursiveLockOnAllEntries(++key);

//             }
//           }
//         }

//       });

//       CacheObserver old = CacheObserverHolder
//           .setInstance(new CacheObserverAdapter() {
//             public void afterWritingBytes()
//             {
//               // Asif Start a Thread & do a get in the thread without
//               // releasing the
//               // lock on dik ID
//               th.start();
//               synchronized (OplogJUnitTest.this) {
//                 OplogJUnitTest.this.proceed = true;
//                 OplogJUnitTest.this.notify();
//               }
//               try {
//                 th.join(30 * 1000); // Yes, really use Thread#join here
//                 fail("never interrupted");
//               }
//               catch (InterruptedException ie) {
//                 // OK. Expected the interrupted Exception
//                 if (debug)
//                   System.out.println("Got the right exception");
//               }

//             }
//           });

//       int totalOpSize = singleOpSize;
//       for (int j = 1; j < loopCount; ++j) {
//         region.put(new Integer(j + 1), val);
//         totalOpSize += evaluateSizeOfOperationForPersist(new Integer(j + 1),
//             val, ((DiskEntry)((LocalRegion)region).basicGetEntry(new Integer(
//                 j + 1))).getDiskId(), OP_CREATE);
//       }
//       assertTrue(totalOpSize - byte_threshold <= singleOpSize);

//       if (!proceed) {
//         synchronized (this) {
//           if (!proceed) {
//             this.wait(25000);
//             if (!proceed) {
//               fail("Test failed as no callback recieved from asynch writer");
//             }
//           }
//         }
//       }
//       DistributedTestCase.join(th, 30 * 1000, null);
//       CacheObserverHolder.setInstance(old);
//     }
//     catch (Exception e) {
//       logWriter.error("Exception occured", e);
//       fail(e.toString());
//     }
//     assertFalse(failureCause, testFailed);
//     LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
//     closeDown();
//   }

  // give the new oplog record format it is too hard for the test to calculate
  // the expected size
//   /**
//    * @author Asif
//    */
//   @Test
//  public void testSynchModeConcurrentOperations()
//   {
//     final Map map = new HashMap();
//     diskProps.setMaxOplogSize(1024 * 1024 * 20);
//     diskProps.setPersistBackup(true);
//     diskProps.setRolling(false);
//     diskProps.setSynchronous(true);
//     diskProps.setOverflow(false);
//     final int THREAD_COUNT = 90;

//     final byte[] val = new byte[50];
//     region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps);
//     for (int i = 1; i < 101; ++i) {
//       map.put(new Integer(i), new Integer(i));
//     }
//     Thread[] threads = new Thread[THREAD_COUNT];
//     for (int i = 0; i < THREAD_COUNT; ++i) {
//       threads[i] = new Thread(new Runnable() {

//         public void run()
//         {
//           int sizeOfOp = 0;
//           DiskId id = null;
//           for (int j = 0; j < 50; ++j) {
//             int keyNum = random.nextInt(10) + 1;
//             Integer key = new Integer(keyNum);
//             Integer intgr = (Integer)map.get(key);
//             try {
//               synchronized (intgr) {

//                 region.create(key, val);
//                 DiskEntry entry = ((DiskEntry)((LocalRegion)region)
//                     .basicGetEntry(key));
//                 id = entry.getDiskId();

//               }
//               sizeOfOp = OplogJUnitTest.evaluateSizeOfOperationForPersist(key,
//                   val, id, OP_CREATE);
//               synchronized (OplogJUnitTest.this) {
//                 OplogJUnitTest.this.expectedOplogSize += sizeOfOp;
//                 ++OplogJUnitTest.this.totalSuccessfulOperations;
//                 ++OplogJUnitTest.this.numCreate;
//               }
//             }
//             catch (EntryExistsException eee) {
//               if (OplogJUnitTest.this.logWriter.finerEnabled()) {
//                 OplogJUnitTest.this.logWriter
//                     .finer("The entry already exists so this operation will not increase the size of oplog");
//               }
//             }
//             try {
//               boolean isUpdate = false;
//               synchronized (intgr) {
//                 isUpdate = region.containsKey(key);
//                 region.put(key, val);
//                 DiskEntry entry = ((DiskEntry)((LocalRegion)region)
//                     .basicGetEntry(key));
//                 id = entry.getDiskId();
//               }
//               sizeOfOp = OplogJUnitTest.evaluateSizeOfOperationForPersist(key,
//                   val, id, (isUpdate ? OP_MODIFY : OP_CREATE));
//               synchronized (OplogJUnitTest.this) {
//                 OplogJUnitTest.this.expectedOplogSize += sizeOfOp;
//                 ++OplogJUnitTest.this.totalSuccessfulOperations;
//                 if (!isUpdate) {
//                   ++OplogJUnitTest.this.numCreate;
//                 }
//                 else {
//                   ++OplogJUnitTest.this.numModify;
//                 }
//               }
//             }
//             catch (EntryDestroyedException ede) {
//               if (OplogJUnitTest.this.logWriter.finerEnabled()) {
//                 OplogJUnitTest.this.logWriter
//                     .finer("The entry already exists so this operation will not increase the size of oplog");
//               }
//             }

//             boolean deleted = false;
//             synchronized (intgr) {
//               if (region.containsKey(key)) {
//                 DiskEntry entry = ((DiskEntry)((LocalRegion)region)
//                     .basicGetEntry(key));
//                 id = entry.getDiskId();
//                 region.remove(key);
//                 deleted = true;
//               }

//             }
//             if (deleted) {
//               sizeOfOp = OplogJUnitTest.evaluateSizeOfOperationForPersist(key,
//                   null, id, OP_DEL);
//               synchronized (OplogJUnitTest.this) {
//                 OplogJUnitTest.this.expectedOplogSize += sizeOfOp;
//                 ++OplogJUnitTest.this.totalSuccessfulOperations;
//                 ++OplogJUnitTest.this.numDel;

//               }
//             }

//           }

//         }

//       });
//       threads[i].start();
//     }

//     for (int i = 0; i < THREAD_COUNT; ++i) {
//       DistributedTestCase.join(threads[i], 30 * 1000, null);
//     }
//     long inMemOplogSize = 0;
//     File opFile = null;
//     try {
//       opFile = ((LocalRegion)region).getDiskRegion().getChild().getOplogFile();
//     }
//     catch (Exception e) {
//       logWriter
//           .error(
//               "Exception in synching data present in the buffers of RandomAccessFile of Oplog, to the disk",
//               e);
//       fail("Test failed because synching of data present in buffer of RandomAccesFile ");
//     }
//     synchronized (opFile) {
//       inMemOplogSize = ((LocalRegion)region).getDiskRegion().getChild().getOplogSize();
//     }

//     long actFileSize = 0;
//     try {

//       actFileSize = ((LocalRegion)region).getDiskRegion().getChild().testGetOplogFileLength();
//     }
//     catch (IOException e) {

//       fail("exception not expected" + e);
//       fail("The test failed as the oplog could not eb synched to disk");
//     }
//     assertEquals((this.numCreate + this.numDel + this.numModify),
//         this.totalSuccessfulOperations);
//     assertTrue(" The expected oplog size =" + inMemOplogSize
//         + " Actual Oplog file size =" + actFileSize,
//         inMemOplogSize == actFileSize);
//     assertTrue(" The expected oplog size =" + this.expectedOplogSize
//         + " In memeory  Oplog size =" + inMemOplogSize,
//         this.expectedOplogSize == inMemOplogSize);
//     closeDown();

//   }

  static int evaluateSizeOfOperationForPersist(Object key, byte[] val,
      DiskId id, int OperationType)
  {
    int size = 1;
    long opKey = id.getKeyId();
    switch (OperationType) {
    case OP_CREATE:
      size += 4 + EntryEventImpl.serialize(key).length + 1 + 4 + val.length;
      break;

    case OP_MODIFY:
      // @todo how do a know if the key needed to be serialized?
      size += 1 + 4 + val.length + Oplog.bytesNeeded(Oplog.abs(opKey));
      break;
    case OP_DEL:
      size += Oplog.bytesNeeded(Oplog.abs(opKey));
      break;
    }
    return size;

  }

  // give the new oplog record format it is too hard for the test to calculate
  // the expected size
//   /**
//    * Tests whether the switching of Oplog happens correctly without size
//    * violation in case of concurrent region operations for synch mode.
//    */
//   @Test
//  public void testSwitchingForConcurrentSynchedOperations()
//   {
//     final Map map = new HashMap();
//     final int MAX_OPLOG_SIZE = 500;
//     diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
//     diskProps.setPersistBackup(true);
//     diskProps.setRolling(false);
//     diskProps.setSynchronous(true);
//     diskProps.setOverflow(false);
//     final int THREAD_COUNT = 5;
//     final byte[] val = new byte[50];
//     final byte[] uval = new byte[1];
//     region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps);
//     for (int i = 1; i < 101; ++i) {
//       map.put(new Integer(i), new Integer(i));
//     }
//     final AI uniqueCtr = CFactory.createAI();
//     Thread[] threads = new Thread[THREAD_COUNT];
//     for (int i = 0; i < THREAD_COUNT; ++i) {
//       threads[i] = new Thread(new Runnable() {
//         public void run()
//         {
//           int sizeOfOp = 0;
//           DiskId id = null;
//           for (int j = 0; j < 50; ++j) {
//             int keyNum = random.nextInt(10) + 1;
//             Integer key = new Integer(keyNum);
//             Integer intgr = (Integer)map.get(key);
//             try {
//               String uniqueKey = "UK" + uniqueCtr.incrementAndGet();
//               // since the files for "empty" oplogs now get cleaned up early
//               // create a unique key to keep this oplog alive.
//               region.create(uniqueKey, uval);
//               DiskEntry uentry = ((DiskEntry)((LocalRegion)region)
//                                  .basicGetEntry(uniqueKey));
//               sizeOfOp = OplogJUnitTest.evaluateSizeOfOperationForPersist(uniqueKey, uval, uentry.getDiskId(), OP_CREATE);
//               synchronized (OplogJUnitTest.this) {
//                 OplogJUnitTest.this.expectedOplogSize += sizeOfOp;
//                 ++OplogJUnitTest.this.totalSuccessfulOperations;
//                 ++OplogJUnitTest.this.numCreate;
//               }
              
//               synchronized (intgr) {

//                 region.create(key, val);
//                 DiskEntry entry = ((DiskEntry)((LocalRegion)region)
//                     .basicGetEntry(key));
//                 id = entry.getDiskId();

//               }
//               sizeOfOp = OplogJUnitTest.evaluateSizeOfOperationForPersist(key,
//                   val, id, OP_CREATE);
                                                                           
//               synchronized (OplogJUnitTest.this) {
//                 OplogJUnitTest.this.expectedOplogSize += sizeOfOp;
//                 ++OplogJUnitTest.this.totalSuccessfulOperations;
//                 ++OplogJUnitTest.this.numCreate;
//               }
//             }
//             catch (EntryExistsException eee) {
//               if (logWriter.finerEnabled()) {
//                 logWriter
//                     .finer("The entry already exists so this operation will not increase the size of oplog");
//               }
//             }
//             try {
//               boolean isUpdate = false;
//               synchronized (intgr) {
//                 isUpdate = region.containsKey(key) && region.get(key) != null
//                     && region.get(key) != Token.DESTROYED;
//                 region.put(key, val);
//                 DiskEntry entry = ((DiskEntry)((LocalRegion)region)
//                     .basicGetEntry(key));
//                 id = entry.getDiskId();
//               }
//               sizeOfOp = OplogJUnitTest.evaluateSizeOfOperationForPersist(key,
//                   val, id, (isUpdate ? OP_MODIFY : OP_CREATE));
//               synchronized (OplogJUnitTest.this) {
//                 OplogJUnitTest.this.expectedOplogSize += sizeOfOp;
//                 ++OplogJUnitTest.this.totalSuccessfulOperations;
//                 if (!isUpdate) {
//                   ++OplogJUnitTest.this.numCreate;
//                 }
//                 else {
//                   ++OplogJUnitTest.this.numModify;
//                 }
//               }
//             }
//             catch (EntryDestroyedException ede) {
//               if (logWriter.finerEnabled()) {
//                 logWriter
//                     .finer("The entry already exists so this operation will not increase the size of oplog");
//               }
//             }

//             boolean deleted = false;
//             synchronized (intgr) {

//               if (region.containsKey(key) && region.get(key) != null
//                   && region.get(key) != Token.DESTROYED) {
//                 DiskEntry entry = ((DiskEntry)((LocalRegion)region)
//                     .basicGetEntry(key));
//                 id = entry.getDiskId();
//                 region.remove(key);
//                 deleted = true;
//               }

//             }
//             if (deleted) {
//               sizeOfOp = OplogJUnitTest.evaluateSizeOfOperationForPersist(key,
//                   null, id, OP_DEL);
//               synchronized (OplogJUnitTest.this) {
//                 OplogJUnitTest.this.expectedOplogSize += sizeOfOp;
//                 ++OplogJUnitTest.this.totalSuccessfulOperations;
//                 ++OplogJUnitTest.this.numDel;

//               }
//             }

//           }

//         }

//       });
//       threads[i].start();
//     }

//     for (int i = 0; i < THREAD_COUNT; ++i) {
//       DistributedTestCase.join(threads[i], 30 * 1000, null);
//     }

//     long currentOplogID = ((LocalRegion)region).getDiskRegion().getChild()
//         .getOplogId();
//     assertTrue(
//         " Switching did not happen, increase the iterations to insert more data ",
//         currentOplogID > 1);
//     long inMemOplogSize = 0;

//     for (int j = 1; j <= currentOplogID; ++j) {

//       Oplog oplog = ((LocalRegion)region).getDiskRegion().getChild(j);
// //       if (j < currentOplogID) {
// //         // oplogs are now closed to save memory and file descriptors
// //         // once they are no longer needed
// //         assertEquals(null, oplog);
// //       } else {
//         inMemOplogSize += oplog.getOplogSize();
//         logWriter.info(" Oplog size="+ oplog.getOplogSize() + " Max Oplog size acceptable="+MAX_OPLOG_SIZE );
//         assertTrue(
//                    " The max Oplog Size limit is violated when taken the inmemory oplog size",
//                    oplog.getOplogSize() <= MAX_OPLOG_SIZE);

//         //      File opFile = null;
//         try {
//           oplog.getOplogFile();
//         }
//         catch (Exception e) {
//           logWriter
//             .error(
//                    "Exception in synching data present in the buffers of RandomAccessFile of Oplog, to the disk",
//                    e);
//           fail("Test failed because synching of data present in buffer of RandomAccesFile ");
//         }

//         assertTrue(
//                    " The max Oplog Size limit is violated when taken the actual file size",
//                    oplog.getActualFileLength() <= MAX_OPLOG_SIZE);
//         assertEquals(oplog.getOplogSize(), oplog.getActualFileLength());
// //       }
//     }

//     inMemOplogSize += ((LocalRegion)region).getDiskRegion().getDiskStore().undeletedOplogSize.get();
    
//     assertTrue(" The sum of all oplogs size as expected  ="
//         + this.expectedOplogSize + " Actual sizes of all oplogs ="
//         + inMemOplogSize, this.expectedOplogSize == inMemOplogSize);

//     assertEquals((this.numCreate + this.numDel + this.numModify),
//         this.totalSuccessfulOperations);
//     closeDown();

//   }

  // give the new oplog record format it is too hard for the test to calculate
  // the expected size
//   /**
//    * Tests whether the switching of Oplog happens correctly without size
//    * violation in case of concurrent region operations for asynch mode.
//    *
//    * @author Asif
//    */
//   @Test
//  public void testSwitchingForConcurrentASynchedOperations()
//   {
//     final int MAX_OPLOG_SIZE = 500;
//     diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
//     diskProps.setPersistBackup(true);
//     diskProps.setRolling(false);
//     diskProps.setSynchronous(false);
//     diskProps.setOverflow(false);
//     diskProps.setBytesThreshold(100);
//     final int THREAD_COUNT = 40;
//     final byte[] val = new byte[50];
//     region = DiskRegionHelperFactory
//         .getAsyncPersistOnlyRegion(cache, diskProps);

//     Thread[] threads = new Thread[THREAD_COUNT];
//     for (int i = 0; i < THREAD_COUNT; ++i) {
//       final int threadNum = (i + 1);
//       threads[i] = new Thread(new Runnable() {
//         public void run()
//         {
//           int sizeOfOp = 0;
//           DiskId id = null;
//           try {
//             region.create(new Integer(threadNum), val);
//           }

//           catch (EntryExistsException e) {
//             e.printStackTrace();
//             testFailed = true;
//             failureCause = "Entry existed with key =" + threadNum;
//             fail("Entry existed with key =" + threadNum);
//           }
//           DiskEntry entry = ((DiskEntry)((LocalRegion)region)
//               .basicGetEntry(new Integer(threadNum)));
//           id = entry.getDiskId();

//           sizeOfOp = OplogJUnitTest.evaluateSizeOfOperationForPersist(
//               new Integer(threadNum), val, id, OP_CREATE);
//           synchronized (OplogJUnitTest.this) {
//             OplogJUnitTest.this.expectedOplogSize += sizeOfOp;
//             ++OplogJUnitTest.this.totalSuccessfulOperations;
//             ++OplogJUnitTest.this.numCreate;
//           }

//         }

//       });

//       threads[i].start();
//     }

//     for (int i = 0; i < THREAD_COUNT; ++i) {
//       DistributedTestCase.join(threads[i], 30 * 1000, null);
//     }

//     long currentOplogID = ((LocalRegion)region).getDiskRegion().getChild()
//         .getOplogId();
//     assertTrue(
//         " Switching did not happen, increase the iterations to insert more data ",
//         currentOplogID > 1);
//     if (debug)
//       System.out.print("Total number of oplogs created = " + currentOplogID);
//     long inMemOplogSize = 0;

//     for (int j = 1; j <= currentOplogID; ++j) {
//       Oplog oplog = ((LocalRegion)region).getDiskRegion().getChild(j);
// //       if (j < currentOplogID) {
// //         // oplogs are now closed to save memory and file descriptors
// //         // once they are no longer needed
// //         assertEquals(null, oplog);
// //       } else {
//         inMemOplogSize += oplog.getOplogSize();
//         //oplog.forceFlush();
//         assertTrue(
//                    " The max Oplog Size limit is violated when taken the inmemory oplog size",
//                    oplog.getOplogSize() <= MAX_OPLOG_SIZE);
//         //      File opFile = null;
//         try {
//           oplog.getOplogFile();
//         }
//         catch (Exception e) {
//           logWriter
//             .error(
//                    "Exception in synching data present in the buffers of RandomAccessFile of Oplog, to the disk",
//                    e);
//           fail("Test failed because synching of data present in buffer of RandomAccesFile ");
//         }
//         assertTrue(
//                    " The max Oplog Size limit is violated when taken the actual file size",
//                    oplog.getActualFileLength() <= MAX_OPLOG_SIZE);
//         assertEquals(oplog.getOplogSize(), oplog.getActualFileLength());
// //       }
//     }

//     inMemOplogSize += ((LocalRegion)region).getDiskRegion().getDiskStore().undeletedOplogSize.get();

//     assertTrue(" The sum of all oplogs size as expected  ="
//         + this.expectedOplogSize + " Actual sizes of all oplogs ="
//         + inMemOplogSize, this.expectedOplogSize == inMemOplogSize);
//     assertEquals((this.numCreate + this.numDel + this.numModify),
//         this.totalSuccessfulOperations);
//     assertFalse(failureCause, testFailed);
//     closeDown();

//   }

//   /**
//    * @author Asif
//    */
//   @Test
//  public void testAsyncWriterTerminationAfterSwitch()
//   {
//     final int MAX_OPLOG_SIZE = 500;
//     diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
//     diskProps.setPersistBackup(true);
//     diskProps.setRolling(false);
//     diskProps.setSynchronous(false);
//     diskProps.setOverflow(false);
//     diskProps.setBytesThreshold(100);
//     final int THREAD_COUNT = 40;
//     final byte[] val = new byte[50];
//     region = DiskRegionHelperFactory
//         .getAsyncPersistOnlyRegion(cache, diskProps);

//     Thread[] threads = new Thread[THREAD_COUNT];
//     for (int i = 0; i < THREAD_COUNT; ++i) {
//       final int threadNum = (i + 1);
//       threads[i] = new Thread(new Runnable() {
//         public void run()
//         {
//           int sizeOfOp = 0;
//           DiskId id = null;
//           try {
//             region.create(new Integer(threadNum), val);
//           }

//           catch (EntryExistsException e) {
//             testFailed = true;
//             failureCause = "Entry existed with key =" + threadNum;
//             fail("Entry existed with key =" + threadNum);
//           }
//           DiskEntry entry = ((DiskEntry)((LocalRegion)region)
//               .basicGetEntry(new Integer(threadNum)));
//           id = entry.getDiskId();

//           sizeOfOp = OplogJUnitTest.evaluateSizeOfOperationForPersist(
//               new Integer(threadNum), val, id, OP_CREATE);
//           synchronized (OplogJUnitTest.this) {
//             OplogJUnitTest.this.expectedOplogSize += sizeOfOp;
//             ++OplogJUnitTest.this.totalSuccessfulOperations;
//             ++OplogJUnitTest.this.numCreate;
//           }

//         }

//       });

//       threads[i].start();
//     }

//     for (int i = 0; i < THREAD_COUNT; ++i) {
//       DistributedTestCase.join(threads[i], 30 * 1000, null);
//     }

//     long currentOplogID = ((LocalRegion)region).getDiskRegion().getChild()
//         .getOplogId();
//     assertTrue(
//         " Switching did not happen, increase the iterations to insert more data ",
//         currentOplogID > 1);

//     for (int j = 1; j < currentOplogID; ++j) {
//       Oplog oplog = ((LocalRegion)region).getDiskRegion().getChild(j);
// //       if (oplog != null) {
// //         DistributedTestCase.join(oplog.getAsynchWriter().getThread(), 10 * 1000, null);
// //       }
//     }
//     assertFalse(failureCause, testFailed);
//     closeDown();

//   }

//   /**
//    * @author Asif
//    */
//   @Test
//  public void testMultipleByteBuffersASynchOperations()
//   {
//     final int MAX_OPLOG_SIZE = 100000;
//     diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
//     diskProps.setPersistBackup(true);
//     diskProps.setRolling(false);
//     diskProps.setSynchronous(false);
//     diskProps.setOverflow(false);
//     diskProps.setBytesThreshold(1000);
//     Oplog.testSetMaxByteBufferSize(100);
//     LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
//     final int OP_COUNT = 40;
//     final byte[] val = new byte[50];
//     region = DiskRegionHelperFactory
//         .getAsyncPersistOnlyRegion(cache, diskProps);
//     CacheObserver old = CacheObserverHolder
//         .setInstance(new CacheObserverAdapter() {
//           public void afterWritingBytes()
//           {

//             synchronized (OplogJUnitTest.this) {
//               flushOccuredAtleastOnce = true;
//               OplogJUnitTest.this.notify();
//             }

//           }
//         });

//     int sizeOfOp = 0;
//     DiskId id = null;
//     for (int i = 0; i < OP_COUNT; ++i) {
//       try {
//         region.create(new Integer(i), val);
//         DiskEntry entry = ((DiskEntry)((LocalRegion)region)
//             .basicGetEntry(new Integer(i)));
//         id = entry.getDiskId();
//         sizeOfOp += evaluateSizeOfOperationForPersist(new Integer(i), val, id,
//             OP_CREATE);

//       }

//       catch (EntryExistsException e) {
//         fail("Entry existed with key =" + i);
//       }
//     }
//     Oplog currOplog = ((LocalRegion)region).getDiskRegion().getChild();
//     long currentOplogID = currOplog.getOplogId();
//     long expectedSize = currOplog.getOplogSize();
//     // Ensure that now switching has happned during the operations
//     assertEquals(1, currentOplogID);
//     assertTrue(
//         "The number of operations did not cause asynch writer to run atleast once , the expected file size = "
//             + expectedSize, expectedSize > 1000);
//     if (!flushOccuredAtleastOnce) {
//       synchronized (this) {
//         if (!flushOccuredAtleastOnce) {
//           try {
//             this.wait(20000);
//           }
//           catch (InterruptedException e) {
//             fail("No guarantee as flushed occure deven once.Exception=" + e);
//           }
//         }
//       }
//     }
//     if (!flushOccuredAtleastOnce) {
//       fail("In the wait duration , flush did not occur even once. Try increasing the wait time");
//     }
//     long actualFileSize = 0L;

//     try {
//       actualFileSize = currOplog.getFileChannel().position();
//     }
//     catch (IOException e) {
//       fail(e.toString());
//     }

//     assertTrue(
//         "The number of operations did not cause asynch writer to run atleast once  as the  actual file size = "
//             + actualFileSize, actualFileSize >= 1000);
//     //currOplog.forceFlush();
// //    File opFile = null;
//     try {
//       currOplog.getOplogFile();
//     }
//     catch (Exception e) {
//       logWriter
//           .error(
//               "Exception in synching data present in the buffers of RandomAccessFile of Oplog, to the disk",
//               e);
//       fail("Test failed because synching of data present in buffer of RandomAccesFile ");
//     }
//     actualFileSize = currOplog.getActualFileLength();
//     assertTrue(
//         " The expected  Oplog Size not equal to the actual file size. Expected size="
//             + expectedSize + " actual size = " + actualFileSize,
//         expectedSize == actualFileSize);
//     Oplog.testSetMaxByteBufferSize(Integer.MAX_VALUE);
//     LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
//     CacheObserverHolder.setInstance(old);
//     closeDown();

//   }

  /**
   * Tests the bug which arises in case of asynch mode during oplog switching
   * caused by conflation of create/destroy operation.The bug occurs if a create
   * operation is followed by destroy but before destroy proceeds some other
   * operation causes oplog switching
   *
   * @author Asif
   */
  @Test
  public void testBug34615()
  {
    final int MAX_OPLOG_SIZE = 100;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setSynchronous(false);
    diskProps.setOverflow(false);
    diskProps.setBytesThreshold(150);

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    final byte[] val = new byte[50];
    region = DiskRegionHelperFactory
        .getAsyncPersistOnlyRegion(cache, diskProps);
    final CacheObserver old = CacheObserverHolder
        .setInstance(new CacheObserverAdapter() {
          @Override
          public void afterConflation(ByteBuffer orig, ByteBuffer conflated)
          {
            Thread th = new Thread(new Runnable() {
              public void run()
              {
                region.put("2", new byte[75]);
              }
            });
            assertNull(conflated);
            th.start();
            DistributedTestCase.join(th, 30 * 1000, null);
            LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;

          }
        });

    region.put("1", val);
    region.remove("1");
    assertFalse(failureCause, testFailed);
    CacheObserverHolder.setInstance(old);
    closeDown();

  }

  /**
   * @author Asif
   */
  @Test
  public void testConflation() throws Exception {

    final int MAX_OPLOG_SIZE = 1000;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setSynchronous(false);
    diskProps.setOverflow(false);
    diskProps.setBytesThreshold(1500);

    final byte[] val = new byte[50];
    final byte[][] bb = new byte[2][];
    bb[0] = new byte[5];
    region = DiskRegionHelperFactory
        .getAsyncPersistOnlyRegion(cache, diskProps);
    try {
      region.put("1", val);
      region.put("1", new byte[10]);
      region.put("2", val);
      region.put("2", new byte[100]);
      region.create("3", null);
      region.put("3", new byte[10]);
      region.create("4", null);
      region.put("4", new byte[0]);

      // tests for byte[][]
      region.create("5", bb);
      region.put("6", val);
      region.put("6", bb);
      region.create("7", null);
      region.put("7", bb);

      region.create("8", new byte[9]);
      region.invalidate("8");
      region.create("9", new byte[0]);
      region.invalidate("9");

      region.create("10", new byte[9]);
      region.localInvalidate("10");
      region.create("11", new byte[0]);
      region.localInvalidate("11");

      DiskRegion dr = ((LocalRegion)region).getDiskRegion();
      dr.flushForTesting();
      byte[] val_1 = ((byte[])((LocalRegion)region).getValueOnDisk("1"));
      assertEquals(val_1.length, 10);
      byte[] val_2 = ((byte[])((LocalRegion)region).getValueOnDisk("2"));
      assertEquals(val_2.length, 100);
      byte[] val_3 = ((byte[])((LocalRegion)region).getValueOnDisk("3"));
      assertEquals(val_3.length, 10);
      byte[] val_4 = ((byte[])((LocalRegion)region).getValueOnDisk("4"));
      assertEquals(val_4.length, 0);
      byte[][] val_5 = (byte[][])((LocalRegion)region).getValueOnDisk("5");
      assertEquals(val_5.length, 2);
      assertEquals(val_5[0].length, 5);
      assertNull(val_5[1]);
      byte[][] val_6 = (byte[][])((LocalRegion)region).getValueOnDisk("6");
      assertEquals(val_6.length, 2);
      assertEquals(val_6[0].length, 5);
      assertNull(val_6[1]);
      byte[][] val_7 = (byte[][])((LocalRegion)region).getValueOnDisk("7");
      assertEquals(val_7.length, 2);
      assertEquals(val_7[0].length, 5);
      assertNull(val_7[1]);
      Object val_8 = ((LocalRegion)region).getValueOnDisk("8");
      assertEquals(val_8, Token.INVALID);
      Object val_9 = ((LocalRegion)region).getValueOnDisk("9");
      assertEquals(val_9, Token.INVALID);
      Object val_10 = ((LocalRegion)region).getValueOnDisk("10");
      assertEquals(val_10, Token.LOCAL_INVALID);
      Object val_11 = ((LocalRegion)region).getValueOnDisk("11");
      assertEquals(val_11, Token.LOCAL_INVALID);

    } catch (Exception e) {
      logWriter.error("Exception occured", e);
      //fail("The test failed due to exception = " + e);
      throw e;
    } finally {
      closeDown();
    }
  }

  /**
   * This tests the retrieval of empty byte array when present in asynch buffers
   *
   * @author Asif
   */
  @Test
  public void testGetEmptyByteArrayInAsynchBuffer()
  {
    final int MAX_OPLOG_SIZE = 1000;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setSynchronous(false);
    diskProps.setOverflow(false);
    diskProps.setBytesThreshold(1500);

    final byte[] val = new byte[50];
    region = DiskRegionHelperFactory
        .getAsyncPersistOnlyRegion(cache, diskProps);
    try {
      region.put("1", val);
      region.put("1", new byte[0]);
      byte[] val_1 = ((byte[])((LocalRegion)region).getValueOnDiskOrBuffer("1"));
      assertEquals(val_1.length, 0);
    }
    catch (Exception e) {
      logWriter.error("Exception occured", e);
      fail("The test failed due to exception = " + e);
    }
    closeDown();
  }

  /**
   * This tests the retrieval of empty byte array in synch mode
   *
   * @author Asif
   */
  @Test
  public void testGetEmptyByteArrayInSynchMode()
  {
    final int MAX_OPLOG_SIZE = 1000;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    diskProps.setBytesThreshold(1500);

    final byte[] val = new byte[50];
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    try {
      region.put("1", val);
      region.put("1", new byte[0]);
      byte[] val_1 = ((byte[])((LocalRegion)region).getValueOnDiskOrBuffer("1"));
      assertEquals(val_1.length, 0);
    }
    catch (Exception e) {
      logWriter.error("Exception occured", e);
      fail("The test failed due to exception = " + e);
    }
    closeDown();
  }

  /**
   * This tests the bug which caused the oplogRoller to attempt to roll a
   * removed entry whose value is Token.Removed This bug can occur if a remove
   * operation causes oplog switching & hence roller thread gets notified, & the
   * roller thread obtains the iterator of the concurrent region map before the
   * remove
   *
   * @author Asif
   */
  @Test
  public void testBug34702()
  {
    final int MAX_OPLOG_SIZE = 500*2;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    final byte[] val = new byte[200];
    proceed = false;

    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    region.put("key1", val);
    region.put("key2", val);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    final CacheObserver old = CacheObserverHolder
        .setInstance(new CacheObserverAdapter() {

          @Override
          public void afterSettingOplogOffSet(long offset)
          {
            ((LocalRegion)region).getDiskRegion().forceRolling();
            // Let the operation thread yield to the Roller so that
            // it is able to obtain the iterator of the concurrrent region map
            // & thus get the reference to the entry which will contain
            // value as Token.Removed as the entry though removed from
            // concurrent
            // map still will be available to the roller
            Thread.yield();
            // Sleep for some time
            try {
              Thread.sleep(5000);
            }
            catch (InterruptedException e) {
              testFailed = true;
              failureCause = "No guarantee that test is succesful";
              fail("No guarantee that test is succesful");
            }
          }

          @Override
          public void afterHavingCompacted()
          {
            proceed = true;
            synchronized (OplogJUnitTest.this) {
              OplogJUnitTest.this.notify();
            }
          }
        });
    try {
      region.destroy("key1");
      region.destroy("key2");
    }
    catch (Exception e1) {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(old);
      fail("Test failed as entry deletion threw exception. Exception = " + e1);
    }
    // Wait for some time & check if the after having rolled callabck
    // is issued sucessfully or not.
    if (!proceed) {
      synchronized (this) {
        if (!proceed) {
          try {
            this.wait(20000);
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // The test will automatically fail due to proceed flag
          }
        }
      }
    }
    assertFalse(failureCause, testFailed);
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    CacheObserverHolder.setInstance(old);

    if (!proceed) {
      fail("Test failed as afterHavingCompacted callabck not issued even after sufficient wait");
    }
    closeDown();

  }

  /**
   * tests a potential deadlock situation if the operation causing a swithcing
   * of Oplog is waiting for roller to free space. The problem can arise if the
   * operation causing Oplog switching is going on an Entry , which already has
   * its oplog ID referring to the Oplog being switched. In such case, when the
   * roller will try to roll the entries referencing the current oplog , it will
   * not be able to acquire the lock on the entry as the switching thread has
   * already taken a lock on it.
   *
   * @author Asif
   */
  @Test
  public void testRollingDeadlockSituation()
  {
    final int MAX_OPLOG_SIZE = 2000;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    diskProps.setDiskDirsAndSizes(new File[] { dirs[0] }, new int[] { 1400 });
    final byte[] val = new byte[500];
    proceed = false;
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);
    region.put("key1", val);
    region.put("key1", val);
    try {
      region.put("key1", val);
    }
    catch (DiskAccessException dae) {
      logWriter.error("Exception occured", dae);
      fail("Test failed as DiskAccessException was encountered where as the operation should ideally have proceeded without issue . exception = "
          + dae);
    }
  }

  /**
   * This tests whether an empty byte array is correctly writtem to the disk as
   * a zero value length operation & hence the 4 bytes field for recording the
   * value length is absent & also since the value length is zero no byte for it
   * should also get added. Similary during recover from HTree as well as Oplog ,
   * the empty byte array should be read correctly
   *
   * @author Asif
   */
  @Test
  public void testEmptyByteArrayPutAndRecovery()
  {
    CacheObserver old = CacheObserverHolder
        .setInstance(new CacheObserverAdapter() {
          @Override
          public void afterConflation(ByteBuffer origBB, ByteBuffer conflatedBB)
          {
            if ((2 + 4 + 1 + EntryEventImpl.serialize("key1").length) != origBB
                .capacity()) {
              failureCause = "For a backup region, addition of an empty array should result in an offset of 6 bytes where as actual offset is ="
                  + origBB.capacity();
              testFailed = true;
            }
            Assert
                .assertTrue(
                    "For a backup region, addition of an empty array should result in an offset of 6 bytes where as actual offset is ="
                        + origBB.capacity(), (2 + 4 + 1 + EntryEventImpl
                        .serialize("key1").length) == origBB.capacity());

          }
        });
    try {
      final int MAX_OPLOG_SIZE = 2000;
      diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
      diskProps.setPersistBackup(true);
      // diskProps.setRolling(true);
      diskProps.setSynchronous(true);
      diskProps.setOverflow(false);
      diskProps.setDiskDirsAndSizes(new File[] { dirs[0] }, new int[] { 1400 });
      final byte[] val = new byte[0];
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskProps, Scope.LOCAL);
      region.put("key1", val);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskProps, Scope.LOCAL);
      byte[] _val = (byte[])region.get("key1");
      assertTrue(
          "value of key1 after restarting the region is not an empty byte array. This may indicate problem in reading from Oplog",
          _val.length == 0);
      if (this.logWriter.infoEnabled()) {
        this.logWriter
            .info("After first region close & opening again no problems encountered & hence Oplog has been read successfully.");
        this.logWriter
            .info("Closing the region again without any operation done, would indicate that next time data will be loaded from HTree .");
      }
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskProps, Scope.LOCAL);
      _val = (byte[])region.get("key1");
      assertTrue(
          "value of key1 after restarting the region is not an empty byte array. This may indicate problem in reading from HTRee",
          _val.length == 0);
      assertFalse(failureCause, testFailed);
      // region.close();

    }
    finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(old);

    }
  }

  /**
   * This is used to test bug 35012 where a remove operation on a key gets
   * unrecorded due to switching of Oplog if it happens just after the remove
   * operation has destroyed the in memory entry & is about to acquire the
   * readlock in DiskRegion to record the same. If the Oplog has switched during
   * that duration , the bug would appear
   *
   * @author Asif
   */

  @Test
  public void testBug35012()
  {
    final int MAX_OPLOG_SIZE = 500;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    final byte[] val = new byte[200];
    try {

      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskProps, Scope.LOCAL);

      region.put("key1", val);
      region.put("key2", val);
      region.put("key3", val);
      final Thread th = new Thread(
          new Runnable() {
            public void run() {
              region.remove("key1");
            }
          }
        );
      // main thread acquires the write lock
      ((LocalRegion)region).getDiskRegion().acquireWriteLock();
      try {
            th.start();
            Thread.yield();
            DiskRegion dr = ((LocalRegion)region).getDiskRegion();
            dr.testHook_getChild().forceRolling(dr);
      }
      finally {
        ((LocalRegion)region).getDiskRegion().releaseWriteLock();
      }
      DistributedTestCase.join(th, 30 * 1000, null);
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskProps, Scope.LOCAL);
      assertEquals(region.size(), 2);
    }
    catch (Exception e) {
      this.logWriter.error("Exception occurred ", e);
      fail("The test could not be completed because of exception .Exception="
          + e);
    }
    closeDown();

  }

  /**
   * Tests the various configurable parameters used by the ByteBufferPool . The
   * behaviour of parameters is based on the mode of DiskRegion ( synch or
   * asynch) . Pls refer to the class documentation ( Oplog.ByteBufferPool) for
   * the exact behaviour of the class
   *
   * @author Asif
   */
//   @Test
//  public void testByteBufferPoolParameters()
//   {
//     // If the mode is asynch , the ByteBuffer obtained should e non direct else
//     // direct
//     final int MAX_OPLOG_SIZE = 500;
//     diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
//     diskProps.setPersistBackup(true);
//     diskProps.setRolling(false);
//     diskProps.setSynchronous(true);
//     diskProps.setOverflow(false);
//     region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps);
//     List bbPools = ((LocalRegion)region).getDiskRegion().getChild()
//         .getByteBufferPoolList();
//     ByteBuffer bb = ((Oplog.ByteBufferPool)bbPools.get(1)).getBufferFromPool();
//     assertTrue(" ByteBuffer is not of type direct", bb.isDirect());
//     region.destroyRegion();
//     diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
//     diskProps.setPersistBackup(true);
//     diskProps.setRolling(false);
//     diskProps.setSynchronous(false);
//     diskProps.setOverflow(false);
//     region = DiskRegionHelperFactory
//         .getAsyncPersistOnlyRegion(cache, diskProps);
//     bbPools = ((LocalRegion)region).getDiskRegion().getChild()
//         .getByteBufferPoolList();
//     bb = ((Oplog.ByteBufferPool)bbPools.get(1)).getBufferFromPool();
//     assertTrue(" ByteBuffer is not of type direct", bb.isDirect());
//     region.close();
//     // Test max pool limit & wait time ( valid only in synch mode).
//     diskProps.setSynchronous(true);
//     diskProps.setRegionName("testRegion");
//     System.setProperty("/testRegion_MAX_POOL_SIZE", "1");

//     System.setProperty("/testRegion_WAIT_TIME", "4000");
//     region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps);
//     bbPools = ((LocalRegion)region).getDiskRegion().getChild()
//         .getByteBufferPoolList();
//     bb = ((Oplog.ByteBufferPool)bbPools.get(1)).getBufferFromPool();
//     assertTrue("Since the Pool has one  Entry , it should be direct", bb
//         .isDirect());

//     long t1 = System.currentTimeMillis();
//     bb = ((Oplog.ByteBufferPool)bbPools.get(1)).getBufferFromPool();
//     long t2 = System.currentTimeMillis();
//     assertTrue(
//         "Since the Pool should have been exhausted hence non direct byte buffer should have been returned",
//         !bb.isDirect());
//     assertTrue("The wait time for ByteBuffer pool was not respected ",
//         (t2 - t1) > 3000);
//     region.close();
// //     // In case of asynch mode , the upper limit should not have been imposed
// //     System.setProperty("/testRegion_MAX_POOL_SIZE", "1");
// //     System.setProperty("/testRegion_WAIT_TIME", "5000");
// //     diskProps.setSynchronous(false);
// //     diskProps.setRegionName("testRegion");
// //     region = DiskRegionHelperFactory
// //         .getAsyncPersistOnlyRegion(cache, diskProps);
// //     bbPools = ((LocalRegion)region).getDiskRegion().getChild()
// //         .getByteBufferPoolList();
// //     bb = ((Oplog.ByteBufferPool)bbPools.get(1)).getBufferFromPool();
// //     t1 = System.currentTimeMillis();
// //     bb = ((Oplog.ByteBufferPool)bbPools.get(1)).getBufferFromPool();
// //     t2 = System.currentTimeMillis();
// //     assertTrue(
// //                "There should not have been any  wait time " + (t2-t1) + " for ByteBuffer pool ",
// //         (t2 - t1) / 1000 < 3);
// //     region.close();
//     System.setProperty("/testRegion_MAX_POOL_SIZE", "2");
//     System.setProperty("/testRegion_WAIT_TIME", "5000");
//     diskProps.setSynchronous(true);
//     diskProps.setRegionName("testRegion");
//     region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps);
//     bbPools = ((LocalRegion)region).getDiskRegion().getChild()
//         .getByteBufferPoolList();
//     Oplog.ByteBufferPool pool = (Oplog.ByteBufferPool)bbPools.get(1);
//     ByteBuffer bb1 = pool.getBufferFromPool();
//     ByteBuffer bb2 = pool.getBufferFromPool();
//     assertEquals(2, pool.getTotalBuffers());
//     assertEquals(2, pool.getBuffersInUse());
//     ((LocalRegion)region).getDiskRegion().getChild().releaseBuffer(bb1);
//     ((LocalRegion)region).getDiskRegion().getChild().releaseBuffer(bb2);
//     assertEquals(0, pool.getBuffersInUse());
//     region.close();

//     System.setProperty("/testRegion_MAX_POOL_SIZE", "1");
//     System.setProperty("/testRegion_WAIT_TIME", "1000");
//     diskProps.setSynchronous(true);
//     diskProps.setRegionName("testRegion");
//     region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps);
//     bbPools = ((LocalRegion)region).getDiskRegion().getChild()
//         .getByteBufferPoolList();
//     pool = (Oplog.ByteBufferPool)bbPools.get(1);
//     bb1 = pool.getBufferFromPool();
//     bb2 = pool.getBufferFromPool();
//     assertEquals(1, pool.getTotalBuffers());
//     assertEquals(1, pool.getBuffersInUse());
//     ((LocalRegion)region).getDiskRegion().getChild().releaseBuffer(bb1);
//     ((LocalRegion)region).getDiskRegion().getChild().releaseBuffer(bb2);
//     assertEquals(0, pool.getBuffersInUse());
//     closeDown();

//   }

  /**
   * Tests the ByteBuffer Pool operations for release of ByteBuffers in case the
   * objects being put vary in size & hence use ByteBuffer Pools present at
   * different indexes
   *
   * @author Asif
   */
//   @Test
//  public void testByteBufferPoolReleaseBugTest()
//   {

//     diskProps.setPersistBackup(true);
//     diskProps.setRolling(false);
//     diskProps.setSynchronous(true);
//     diskProps.setOverflow(false);
//     System.setProperty("/testRegion_UNIT_BUFF_SIZE", "100");
//     System.setProperty("/testRegion_UNIT_BUFF_SIZE", "100");
//     System.setProperty("gemfire.log-level", getGemFireLogLevel());
//     region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps);
//     region.put("key1", new byte[900]);
//     region.put("key1", new byte[700]);
//     closeDown();

//   }

  /**
   * Tests if buffer size & time are not set , the asynch writer gets awakened
   * on time basis of default 1 second
   *
   * @author Asif
   */
  @Test
  public void testAsynchWriterAttribBehaviour1()
  {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    ((DiskStoreFactoryImpl)dsf).setMaxOplogSizeInBytes(10000);
    File dir = new File("testingDirectoryDefault");
    dir.mkdir();
    dir.deleteOnExit();
    File[] dirs = { dir };
    dsf.setDiskDirs(dirs);
    AttributesFactory factory = new AttributesFactory();
    final long t1 = System.currentTimeMillis();
    DiskStore ds = dsf.create("test");
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(ds.getName());
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setScope(Scope.LOCAL);
    try {
      region = cache.createVMRegion("test", factory.createRegionAttributes());
    }
    catch (Exception e1) {
      logWriter.error("Test failed due to exception", e1);
      fail("Test failed due to exception " + e1);

    }

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    CacheObserver old = CacheObserverHolder.setInstance(

    new CacheObserverAdapter() {
      private long t2;
      @Override
      public void goingToFlush()
      {
        t2 = System.currentTimeMillis();
        delta = t2 - t1;
        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
        synchronized (OplogJUnitTest.this) {
          OplogJUnitTest.this.notify();
        }
      }
    });

    region.put("key1", "111111111111");
    synchronized (this) {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        try {
          this.wait(10000);
        }
        catch (InterruptedException e) {
          logWriter.error("Test failed due to exception", e);
          fail("Test failed due to exception " + e);

        }
        assertFalse(LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER);
      }
    }
    CacheObserverHolder.setInstance(old);
    // Windows clock has an accuracy of 15 ms. Accounting for the same.
    assertTrue(
        "delta is in miilliseconds=" + delta,
        delta >= 985);
    closeDown();
  }

  /**
   * Tests if buffer size is set but time is not set , the asynch writer gets
   * awakened on buffer size basis
   *
   * @author Asif
   */
  public void DARREL_DISABLE_testAsynchWriterAttribBehaviour2()
  {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    ((DiskStoreFactoryImpl)dsf).setMaxOplogSizeInBytes(10000);
    dsf.setQueueSize(2);
    File dir = new File("testingDirectoryDefault");
    dir.mkdir();
    dir.deleteOnExit();
    File[] dirs = { dir };
    dsf.setDiskDirs(dirs);
    AttributesFactory factory = new AttributesFactory();
    DiskStore ds = dsf.create("test");
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(ds.getName());
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setScope(Scope.LOCAL);
    try {
      region = cache.createVMRegion("test", factory.createRegionAttributes());
    }
    catch (Exception e1) {
      logWriter.error("Test failed due to exception", e1);
      fail("Test failed due to exception " + e1);

    }

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    CacheObserver old = CacheObserverHolder.setInstance(

    new CacheObserverAdapter() {

      @Override
      public void goingToFlush()
      {
        synchronized (OplogJUnitTest.this) {
          LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
          OplogJUnitTest.this.notify();
        }
      }
    });

    region.put("key1", new byte[25]);
    try {
      Thread.sleep(1000);
    }
    catch (InterruptedException e) {
      logWriter.error("Test failed due to exception", e);
      fail("Test failed due to exception " + e);
    }
    assertTrue(LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER);
    region.put("key2", new byte[25]);
    synchronized (this) {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        try {
          OplogJUnitTest.this.wait(10000);
          assertFalse(LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER);
        }
        catch (InterruptedException e2) {
          logWriter.error("Test failed due to exception", e2);
          fail("Test failed due to exception " + e2);
        }
      }
    }
    CacheObserverHolder.setInstance(old);
    closeDown();
  }

  /**
   * Tests if buffer size & time interval are explicitly set to zero then the
   * flush will occur due to asynchForceFlush or due to switching of Oplog
   *
   * @author Asif
   */
  @Test
  public void testAsynchWriterAttribBehaviour3()
  {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    ((DiskStoreFactoryImpl)dsf).setMaxOplogSizeInBytes(500);
    dsf.setQueueSize(0);
    dsf.setTimeInterval(0);
    File dir = new File("testingDirectoryDefault");
    dir.mkdir();
    dir.deleteOnExit();
    File[] dirs = { dir };
    dsf.setDiskDirs(dirs);
    AttributesFactory factory = new AttributesFactory();
    DiskStore ds = dsf.create("test");
    factory.setDiskSynchronous(false);
    factory.setDiskStoreName(ds.getName());
    factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    factory.setScope(Scope.LOCAL);
    try {
      region = cache.createVMRegion("test", factory.createRegionAttributes());
    }
    catch (Exception e1) {
      logWriter.error("Test failed due to exception", e1);
      fail("Test failed due to exception " + e1);

    }

    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
    CacheObserver old = CacheObserverHolder.setInstance(

    new CacheObserverAdapter() {

      @Override
      public void goingToFlush()
      {
        LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
        synchronized (OplogJUnitTest.this) {
          OplogJUnitTest.this.notify();
        }
      }
    });
    try {
      region.put("key1", new byte[100]);
      region.put("key2", new byte[100]);
      region.put("key3", new byte[100]);
      region.put("key4", new byte[100]);
      region.put("key5", new byte[100]);
      Thread.sleep(1000);
      assertTrue(LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER);
    }
    catch (Exception e) {
      logWriter.error("Test failed due to exception", e);
      fail("Test failed due to exception " + e);
    }
    region.forceRolling();
    synchronized (this) {
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        try {
          OplogJUnitTest.this.wait(10000);
        }
        catch (InterruptedException e2) {
          logWriter.error("Test failed due to exception", e2);
          fail("Test failed due to exception " + e2);
        }
      }
    }
    assertFalse(LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER);
    CacheObserverHolder.setInstance(old);
    closeDown();
  }

  /**
   * Tests if the preblowing of a file with size greater than the disk space
   * available so that preblowing results in IOException , is able to recover
   * without problem
   *
   * @author Asif
   */
  //Now we preallocate spaces for if files and also crfs and drfs. So the below test is not valid
  // any more. See revision: r42359 and r42320. So disabling this test.
  public void _testPreblowErrorCondition()
  {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    ((DiskStoreFactoryImpl)dsf).setMaxOplogSizeInBytes(100000000L * 1024L * 1024L * 1024L);
    dsf.setAutoCompact(false);
    File dir = new File("testingDirectoryDefault");
    dir.mkdir();
    dir.deleteOnExit();
    File[] dirs = { dir };
    int size[] = new int[] { Integer.MAX_VALUE };
    dsf.setDiskDirsAndSizes(dirs, size);
    AttributesFactory factory = new AttributesFactory();
    logWriter.info("<ExpectedException action=add>"
                   + "Could not pregrow"
                   + "</ExpectedException>");
    try {
      DiskStore ds = dsf.create("test");
      factory.setDiskStoreName(ds.getName());
      factory.setDiskSynchronous(true);
      factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
      factory.setScope(Scope.LOCAL);
      try {
        region = cache.createVMRegion("test", factory.createRegionAttributes());
      }
      catch (Exception e1) {
        logWriter.error("Test failed due to exception", e1);
        fail("Test failed due to exception " + e1);

      }
      region.put("key1", new byte[900]);
      byte[] val = null;
      try {
        val = (byte[])((LocalRegion)region).getValueOnDisk("key1");
      }
      catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        fail(e.toString());
      }
      assertTrue(val.length == 900);
    } finally {
      logWriter.info("<ExpectedException action=remove>"
                     + "Could not pregrow"
                     + "</ExpectedException>");
    }
    closeDown();
  }

  /**
   * Tests if the byte buffer pool in asynch mode tries to contain the pool size
   *
   * @author Asif
   */
  @Test
  public void testByteBufferPoolContainment()
  {

    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1024 * 1024);
    diskProps.setSynchronous(false);
    diskProps.setOverflow(false);
    diskProps.setBytesThreshold(10); // this is now item count
    diskProps.setTimeInterval(0);
    region = DiskRegionHelperFactory
        .getAsyncPersistOnlyRegion(cache, diskProps);
    final byte[] val = new byte[1000];
    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

    CacheObserverHolder
        .setInstance(new CacheObserverAdapter() {

          @Override
          public void goingToFlush()
          { // Delay flushing

            assertEquals(10, region.size());
            for (int i = 10; i < 20; ++i) {
              region.put("" + i, val);
            }
            synchronized (OplogJUnitTest.this) {
              OplogJUnitTest.this.notify();
              LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
            }

          }
        });
    for (int i = 0; i < 10; ++i) {
      region.put("" + i, val);
    }
    try {
      synchronized (OplogJUnitTest.this) {
        if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
          OplogJUnitTest.this.wait(9000);
          assertEquals(false, LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER);
        }
      }

    }
    catch (InterruptedException ie) {
      fail("interrupted");
    }
    //((LocalRegion)region).getDiskRegion().getChild().forceFlush();
//     int x = ((LocalRegion)region).getDiskRegion().getChild().getAsynchWriter()
//         .getApproxFreeBuffers();
//     assertEquals(10, x);
  }

  // we no longer have a pendingFlushMap
//   /**
//    * This test does the following: <br>
//    * 1)Create a diskRegion with async mode and byte-threshold as 25 bytes. <br>
//    * 2)Put an entry into the region such that the async-buffer is just over 25
//    * bytes and the writer-thread is invoked. <br>
//    * 3)Using CacheObserver.afterSwitchingWriteAndFlushMaps callback, perform a
//    * put on the same key just after the async writer thread swaps the
//    * pendingFlushMap and pendingWriteMap for flushing. <br>
//    * 4)Using CacheObserver.afterWritingBytes, read the value for key
//    * (LocalRegion.getValueOnDiskOrBuffer) just after the async writer thread has
//    * flushed to the disk. <br>
//    * 5) Verify that the value read in step3 is same as the latest value. This
//    * will ensure that the flushBufferToggle flag is functioning as expected ( It
//    * prevents the writer thread from setting the oplog-offset in diskId if that
//    * particular entry has been updated by a put-thread while the
//    * async-writer-thread is flushing that entry.)
//    *
//    * @throws Exception
//    */
//   @Test
//  public void testFlushBufferToggleFlag() throws Exception
//   {
//     final int MAX_OPLOG_SIZE = 100000;
//     diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
//     diskProps.setPersistBackup(true);
//     diskProps.setRolling(false);
//     diskProps.setSynchronous(false);
//     diskProps.setOverflow(false);
//     diskProps.setBytesThreshold(25);
//     LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;

//     region = DiskRegionHelperFactory
//         .getAsyncPersistOnlyRegion(cache, diskProps);
//     CacheObserver old = CacheObserverHolder
//         .setInstance(new CacheObserverAdapter() {
//           public void afterWritingBytes()
//           {
//             LocalRegion localregion = (LocalRegion)region;
//             try {
//               valueRead = (String)localregion.getValueOnDiskOrBuffer(KEY);
//               synchronized (OplogJUnitTest.class) {
//                 proceedForValidation = true;
//                 OplogJUnitTest.class.notify();
//               }
//             }
//             catch (EntryNotFoundException e) {
//               e.printStackTrace();
//             }
//           }

//           public void afterSwitchingWriteAndFlushMaps()
//           {
//             region.put(KEY, NEW_VALUE);
//           }

//         });

//     region.put(KEY, OLD_VALUE);

//     if (!proceedForValidation) {
//       synchronized (OplogJUnitTest.class) {
//         if (!proceedForValidation) {
//           try {
//             OplogJUnitTest.class.wait(9000);
//             assertEquals(true, proceedForValidation);
//           }
//           catch (InterruptedException e) {
//             fail("interrupted");
//           }
//         }
//       }
//     }

//     cache.getLogger().info("valueRead : " + valueRead);
//     assertEquals("valueRead is stale, doesnt match with latest PUT", NEW_VALUE,
//         valueRead);
//     LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
//     CacheObserverHolder.setInstance(old);
//     closeDown();
//   }

  /**
   * tests async stats are correctly updated
   */
  @Test
  public void testAsyncStats() throws InterruptedException
  {
    diskProps.setBytesThreshold(101);
    diskProps.setTimeInterval(1000000);
    region = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
        diskProps);
    final DiskStoreStats dss = ((LocalRegion)region).getDiskRegion().getDiskStore().getStats();
    WaitCriterion evFull = new WaitCriterion() {
      public boolean done() {
        return dss.getQueueSize() == 100;
      }
      public String description() {
        return null;
      }
    };
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return dss.getQueueSize() == 0;
      }
      public String description() {
        return null;
      }
    };
    WaitCriterion ev2 = new WaitCriterion() {
      public boolean done() {
        return dss.getFlushes() == 100;
      }
      public String description() {
        return null;
      }
    };
    WaitCriterion ev3 = new WaitCriterion() {
      public boolean done() {
        return dss.getFlushes() == 200;
      }
      public String description() {
        return null;
      }
    };

    assertEquals(0, dss.getQueueSize());
    put100Int();
    DistributedTestCase.waitForCriterion(evFull, 2 * 1000, 200, true);
    assertEquals(0, dss.getFlushes());
    region.writeToDisk();
    DistributedTestCase.waitForCriterion(ev, 2 * 1000, 200, true);
    DistributedTestCase.waitForCriterion(ev2, 1000, 200, true);
    put100Int();
    DistributedTestCase.waitForCriterion(evFull, 2 * 1000, 200, true);
    region.writeToDisk();
    DistributedTestCase.waitForCriterion(ev, 2 * 1000, 200, true);
    DistributedTestCase.waitForCriterion(ev3, 1000, 200, true);
    closeDown();
  }

  /**
   * Tests delayed creation of DiskID in overflow only mode
   *
   * @author Asif
   */
  @Test
  public void testDelayedDiskIdCreationInOverflowOnlyMode()
  {
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1024 * 1024);
    diskProps.setSynchronous(false);
    diskProps.setOverflow(true);
    diskProps.setBytesThreshold(10000);
    diskProps.setTimeInterval(0);
    diskProps.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getAsyncOverFlowOnlyRegion(cache,
        diskProps);
    final byte[] val = new byte[1000];
    region.put("1", val);
    DiskEntry entry = (DiskEntry)((LocalRegion)region).basicGetEntry("1");
    assertTrue(entry instanceof AbstractDiskLRURegionEntry);
    assertNull(entry.getDiskId());
    region.put("2", val);
    assertNotNull(entry.getDiskId());
    entry = (DiskEntry)((LocalRegion)region).basicGetEntry("2");
    assertTrue(entry instanceof AbstractDiskLRURegionEntry);
    assertNull(entry.getDiskId());
  }

  /**
   * Tests immediate creation of DiskID in overflow With Persistence mode
   *
   * @author Asif
   */
  @Test
  public void testImmediateDiskIdCreationInOverflowWithPersistMode()
  {
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1024 * 1024);
    diskProps.setSynchronous(false);
    diskProps.setOverflow(true);
    diskProps.setBytesThreshold(10000);
    diskProps.setTimeInterval(0);
    diskProps.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getAsyncOverFlowAndPersistRegion(cache,
        diskProps);
    final byte[] val = new byte[1000];
    region.put("1", val);
    DiskEntry entry = (DiskEntry)((LocalRegion)region).basicGetEntry("1");
    assertTrue(entry instanceof AbstractDiskLRURegionEntry);
    assertNotNull(entry.getDiskId());
    region.put("2", val);
    assertNotNull(entry.getDiskId());
    entry = (DiskEntry)((LocalRegion)region).basicGetEntry("2");
    assertTrue(entry instanceof AbstractDiskLRURegionEntry);
    assertNotNull(entry.getDiskId());
  }

  /**
   * An entry which is evicted to disk will have the flag already written to
   * disk, appropriately set
   *
   * @author Asif
   */
  @Test
  public void testEntryAlreadyWrittenIsCorrectlyUnmarkedForOverflowOnly()
    throws Exception
  {
    try {
      diskProps.setPersistBackup(false);
      diskProps.setRolling(false);
      diskProps.setMaxOplogSize(1024 * 1024);
      diskProps.setSynchronous(true);
      diskProps.setOverflow(true);
      diskProps.setBytesThreshold(10000);
      diskProps.setTimeInterval(0);
      diskProps.setOverFlowCapacity(1);
      region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache,
          diskProps);
      final byte[] val = new byte[1000];
      region.put("1", val);
      region.put("2", val);
      // "1" should now be on disk
      region.get("1");
      // "2" should now be on disk
      DiskEntry entry1 = (DiskEntry)((LocalRegion)region).basicGetEntry("1");
      DiskId did1 = entry1.getDiskId();
      DiskId.isInstanceofOverflowIntOplogOffsetDiskId(did1);
      assertTrue(!did1.needsToBeWritten());
      region.put("1", "3");
      assertTrue(did1.needsToBeWritten());
      region.put("2", val);
      DiskEntry entry2 = (DiskEntry)((LocalRegion)region).basicGetEntry("2");
      DiskId did2 = entry2.getDiskId();
      assertTrue(!did2.needsToBeWritten() || !did1.needsToBeWritten());
      tearDown();
      setUp();
      diskProps.setPersistBackup(false);
      diskProps.setRolling(false);
      long opsize = Integer.MAX_VALUE;
      opsize += 100L;
      diskProps.setMaxOplogSize(opsize);
      diskProps.setSynchronous(true);
      diskProps.setOverflow(true);
      diskProps.setBytesThreshold(10000);
      diskProps.setTimeInterval(0);
      diskProps.setOverFlowCapacity(1);
      region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache,
          diskProps);
      region.put("1", val);
      region.put("2", val);
      region.get("1");
      entry1 = (DiskEntry)((LocalRegion)region).basicGetEntry("1");
      did1 = entry1.getDiskId();
      DiskId.isInstanceofOverflowOnlyWithLongOffset(did1);
      assertTrue(!did1.needsToBeWritten());
      region.put("1", "3");
      assertTrue(did1.needsToBeWritten());
      region.put("2", "3");
      did2 = entry2.getDiskId();
      assertTrue(!did2.needsToBeWritten() || !did1.needsToBeWritten());
    }
    catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }

  /**
   * An persistent or overflow with persistence entry which is evicted to disk,
   * will have the flag already written to disk, appropriately set
   *
   * @author Asif
   */
  @Test
  public void testEntryAlreadyWrittenIsCorrectlyUnmarkedForOverflowWithPersistence()
  {
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1024 * 1024);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(true);
    diskProps.setBytesThreshold(10000);
    diskProps.setTimeInterval(0);
    diskProps.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        diskProps);
    final byte[] val = new byte[1000];
    region.put("1", val);
    DiskEntry entry1 = (DiskEntry)((LocalRegion)region).basicGetEntry("1");
    DiskId did1 = entry1.getDiskId();
    DiskId.isInstanceofPersistIntOplogOffsetDiskId(did1);
    assertTrue(!did1.needsToBeWritten());
    region.put("2", val);
    assertTrue(!did1.needsToBeWritten());
  }

  /**
   * Tests the various DiskEntry.Helper APIs for correctness as there is now
   * delayed creation of DiskId and accessing OplogkeyId will throw
   * UnsupportedException
   */
  @Test
  public void testHelperAPIsForOverflowOnlyRegion()
  {
    diskProps.setPersistBackup(false);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1024 * 1024);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(true);
    diskProps.setBytesThreshold(10000);
    diskProps.setTimeInterval(0);
    diskProps.setOverFlowCapacity(2);
    region = DiskRegionHelperFactory
        .getSyncOverFlowOnlyRegion(cache, diskProps);
    final byte[] val = new byte[1000];
    DiskRegion dr = ((LocalRegion)region).getDiskRegion();
    region.put("1", val);
    // region.get("1");
    region.put("2", val);
    // region.get("2");
    region.put("3", val);
    // region.get("3");
    DiskEntry entry1 = (DiskEntry)((LocalRegion)region).basicGetEntry("1");
//    DiskId did1 = entry1.getDiskId();
    DiskEntry entry2 = (DiskEntry)((LocalRegion)region).basicGetEntry("2");
//    DiskId did2 = entry2.getDiskId();
    DiskEntry entry3 = (DiskEntry)((LocalRegion)region).basicGetEntry("3");
//    DiskId did3 = entry3.getDiskId();
    assertNull(entry2.getDiskId());
    assertNull(entry3.getDiskId());
    assertNotNull(entry1.getDiskId());
    assertNull(DiskEntry.Helper.getValueOnDisk(entry3, dr));
    assertNull(DiskEntry.Helper.getValueOnDisk(entry2, dr));
    assertNotNull(DiskEntry.Helper.getValueOnDisk(entry1, dr));
    assertNull(DiskEntry.Helper.getValueOnDisk(entry3, dr));
    assertNull(DiskEntry.Helper.getValueOnDisk(entry2, dr));

    assertNull(entry2.getDiskId());
    assertNull(entry3.getDiskId());
    assertNotNull(entry1.getDiskId());
    assertNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry3, dr, (LocalRegion) region));
    assertNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry2, dr, (LocalRegion) region));
    assertNotNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry1, dr, (LocalRegion) region));
    assertNull(DiskEntry.Helper.getValueOnDisk(entry3, dr));
    assertNull(DiskEntry.Helper.getValueOnDisk(entry2, dr));
  }

  /**
   * Tests the various DiskEntry.Helper APIs for correctness as there is now
   * delayed creation of DiskId and accessing OplogkeyId will throw
   * UnsupportedException
   */
  @Test
  public void testHelperAPIsForOverflowWithPersistenceRegion()
  {
    helperAPIsForPersistenceWithOrWithoutOverflowRegion(true /* should overflow */);
  }

  /**
   * Tests the various DiskEntry.Helper APIs for correctness as there is now
   * delayed creation of DiskId and accessing OplogkeyId will throw
   * UnsupportedException
   */
  @Test
  public void testHelperAPIsForPersistenceRegion()
  {
    helperAPIsForPersistenceWithOrWithoutOverflowRegion(false /* should overflow */);
  }

  /**
   * Tests the various DiskEntry.Helper APIs for correctness as there is now
   * delayed creation of DiskId and accessing OplogkeyId will throw
   * UnsupportedException
   */
  private void helperAPIsForPersistenceWithOrWithoutOverflowRegion(
      boolean overflow)
  {
    diskProps.setPersistBackup(true);
    diskProps.setRolling(false);
    diskProps.setMaxOplogSize(1024 * 1024);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(overflow);
    diskProps.setBytesThreshold(10000);
    diskProps.setTimeInterval(0);
    diskProps.setOverFlowCapacity(2);
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        diskProps);
    final byte[] val = new byte[1000];
    DiskRegion dr = ((LocalRegion)region).getDiskRegion();
    region.put("1", val);
    // region.get("1");
    region.put("2", val);
    // region.get("2");
    region.put("3", val);
    // region.get("3");
    DiskEntry entry1 = (DiskEntry)((LocalRegion)region).basicGetEntry("1");
//    DiskId did1 = entry1.getDiskId();
    DiskEntry entry2 = (DiskEntry)((LocalRegion)region).basicGetEntry("2");
//    DiskId did2 = entry2.getDiskId();
    DiskEntry entry3 = (DiskEntry)((LocalRegion)region).basicGetEntry("3");
//    DiskId did3 = entry3.getDiskId();
    assertNotNull(entry2.getDiskId());
    assertNotNull(entry3.getDiskId());
    assertNotNull(entry1.getDiskId());
    assertNotNull(DiskEntry.Helper.getValueOnDisk(entry3, dr));
    assertNotNull(DiskEntry.Helper.getValueOnDisk(entry2, dr));
    assertNotNull(DiskEntry.Helper.getValueOnDisk(entry1, dr));

    assertNotNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry3, dr, (LocalRegion) region));
    assertNotNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry2, dr, (LocalRegion) region));
    assertNotNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry1, dr, (LocalRegion) region));

    region.close();
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,
        diskProps);
    dr = ((LocalRegion)region).getDiskRegion();
    entry1 = (DiskEntry)((LocalRegion)region).basicGetEntry("1");
//    did1 = entry1.getDiskId();
    entry2 = (DiskEntry)((LocalRegion)region).basicGetEntry("2");
//    did2 = entry2.getDiskId();
    entry3 = (DiskEntry)((LocalRegion)region).basicGetEntry("3");
//    did3 = entry3.getDiskId();

    assertNotNull(entry2.getDiskId());
    assertNotNull(entry3.getDiskId());
    assertNotNull(entry1.getDiskId());

    assertNotNull(DiskEntry.Helper.getValueOnDisk(entry3, dr));
    assertNotNull(DiskEntry.Helper.getValueOnDisk(entry2, dr));
    assertNotNull(DiskEntry.Helper.getValueOnDisk(entry1, dr));

    assertNotNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry3, dr, (LocalRegion) region));
    assertNotNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry2, dr, (LocalRegion) region));
    assertNotNull(DiskEntry.Helper.getValueOnDiskOrBuffer(entry1, dr, (LocalRegion) region));

  }

  // @todo this test is failing for some reason. Does it need to be fixed?
  /**
   * Bug test to reproduce the bug 37261. The scenario which this test depicts
   * is not actually the cause of Bug 37261. This test validates the case where
   * a synch persist only entry1 is created in Oplog1. A put operation on entry2
   * causes the switch , but before Oplog1 is rolled , the entry1 is modified so
   * that it references Oplog2. Thus in effect roller will skip rolling entry1
   * when rolling Oplog1.Now entry1 is deleted in Oplog2 and then a rolling
   * happens. There should not be any error
   */
//   @Test
//  public void testBug37261_1()
//   {
//     CacheObserver old = CacheObserverHolder.getInstance();
//     try {
//       // Create a persist only region with rolling true
//       diskProps.setPersistBackup(true);
//       diskProps.setRolling(true);
//       diskProps.setCompactionThreshold(100);
//       diskProps.setMaxOplogSize(1024);
//       diskProps.setSynchronous(true);
//       this.proceed = false;
//       region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
//           diskProps);
//       // create an entry 1 in oplog1,
//       region.put("key1", new byte[800]);

//       // Asif the second put will cause a switch to oplog 2 & also cause the
//       // oplog1
//       // to be submitted to the roller
//       LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
//       CacheObserverHolder.setInstance(new CacheObserverAdapter() {
//         public void beforeGoingToCompact()
//         {
//           // modify entry 1 so that it points to the new switched oplog
//           Thread th = new Thread(new Runnable() {
//             public void run()
//             {
//               region.put("key1", new byte[400]);
//             }
//           });
//           th.start();
//           try {
//             DistributedTestCase.join(th, 30 * 1000, null);
//           }
//           catch (Exception e) {
//             e.printStackTrace();
//             failureCause = e.toString();
//             failure = true;
//           }
//         }

//         public void afterHavingCompacted()
//         {
//           synchronized (OplogJUnitTest.this) {
//             rollerThread = Thread.currentThread();
//             OplogJUnitTest.this.notify();
//             OplogJUnitTest.this.proceed = true;
//           }
//         }
//       });
//       region.put("key2", new byte[300]);
//       synchronized (this) {
//         if (!this.proceed) {
//           this.wait(15000);
//           assertTrue(this.proceed);
//         }
//       }
//       this.proceed = false;
//       // Asif Delete the 1st entry
//       region.destroy("key1");

//       CacheObserverHolder.setInstance(new CacheObserverAdapter() {
//         public void afterHavingCompacted()
//         {
//           synchronized (OplogJUnitTest.this) {
//             OplogJUnitTest.this.notify();
//             OplogJUnitTest.this.proceed = true;
//           }
//         }
//       });
//       // Coz another switch and wait till rolling done
//       region.put("key2", new byte[900]);

//       synchronized (this) {
//         if (!this.proceed) {
//           this.wait(15000);
//           assertFalse(this.proceed);
//         }
//       }
//       // Check if the roller is stil alive
//       assertTrue(rollerThread.isAlive());
//     }
//     catch (Exception e) {
//       e.printStackTrace();
//       fail("Test failed du toe xception" + e);
//     }
//     finally {
//       CacheObserverHolder.setInstance(old);
//       LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
//     }

//   }

  /**
   * Tests the condition when a 'put' is in progress and concurrent 'clear' and
   * 'put'(on the same key) occur. Thus if after Htree  ref was set (in
   * 'put'), the region got cleared (and same key re-'put'),
   * the entry will get recorded in the new Oplog without a corresponding
   * create ( because the Oplogs containing create have already been deleted
   * due to the clear operation). This put should not proceed. Also, Region
   * creation after closing should not give an exception.
   */
  @Test
  public void testPutClearPut()
  {
    try {
      // Create a persist only region with rolling true
      diskProps.setPersistBackup(true);
      diskProps.setRolling(true);
      diskProps.setMaxOplogSize(1024);
      diskProps.setSynchronous(true);
      this.proceed = false;
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskProps, Scope.LOCAL);
      final Thread clearOp = new Thread(new Runnable() {
        public void run()
        {
          try {
            LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
            region.clear();
            region.put("key1", "value3");
          }
          catch (Exception e) {
            testFailed = true;
            failureCause = "Encountered Exception=" + e;
          }
        }
      });
      region.getAttributesMutator().setCacheWriter(new CacheWriterAdapter() {
        @Override
        public void beforeUpdate(EntryEvent event) throws CacheWriterException {
          clearOp.start();
        }
      });
      try {
        DistributedTestCase.join(clearOp, 30 * 1000, null);
      }
      catch (Exception e) {
        testFailed = true;
        failureCause = "Encountered Exception=" + e;
        e.printStackTrace();
      }
      region.create("key1", "value1");
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      region.put("key1", "value2");
      if (!testFailed) {
        region.close();
        region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskProps, Scope.LOCAL);
      }else {
        fail(failureCause);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to exception" + e);
    }
    finally {
      testFailed = false;
      proceed = false;
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    }
  }

  /**
   * Tests the condition when a 'put' on an alreay created entry
   *  and concurrent 'clear' are happening.
   *  Thus if after HTree ref was set (in
   * 'put'), the region got cleared (and same key re-'put'),
   * the entry will actually become a create in the VM
   * The new Oplog should record it as a create even though the
   * Htree ref in ThreadLocal will not match with the
   * current Htree Ref. But the operation is valid & should get recorded
   * in Oplog
   *
   */
  @Test
  public void testPutClearCreate()
  {
    failure = false;
    try {
      // Create a persist only region with rolling true
      diskProps.setPersistBackup(true);
      diskProps.setRolling(true);
      diskProps.setMaxOplogSize(1024);
      diskProps.setSynchronous(true);

      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskProps, Scope.LOCAL);

      region.create("key1", "value1");
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      CacheObserverHolder.setInstance(new CacheObserverAdapter(){
        @Override
        public void afterSettingDiskRef() {
          Thread clearTh = new Thread( new Runnable() {
            public void run() {
              region.clear();
            }
          });
          clearTh.start();
          try {
            DistributedTestCase.join(clearTh, 120 * 1000, null);
            failure = clearTh.isAlive();
            failureCause = "Clear Thread still running !";
          } catch(Exception e) {
            failure = true;
            failureCause = e.toString();
          }
        }
      });
      region.put("key1", "value2");
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      assertFalse(failureCause,failure);
      assertEquals(1,region.size());
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskProps, Scope.LOCAL);
      assertEquals(1,region.size());
      assertEquals("value2",(String)region.get("key1"));
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to exception" + e);
    }
    finally {
      testFailed = false;
      proceed = false;
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(new CacheObserverAdapter());
      failure = false;
    }
  }

  /**
   * Tests if 'destroy' transaction is working correctly
   * for sync-overflow-only disk region entry
   */
  @Test
  public void testOverFlowOnlySyncDestroyTx()
  {
    diskProps.setMaxOplogSize(20480);
    diskProps.setOverFlowCapacity(1);
    diskProps.setDiskDirs(dirs);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache,
    		diskProps);
    assertNotNull(region);
    region.put("key", "createValue");
    region.put("key1", "createValue1");
    try {
      cache.getCacheTransactionManager().begin();
      region.destroy("key");
      cache.getCacheTransactionManager().commit();
      assertNull("The deleted entry should have been null",((LocalRegion)region).entries.getEntry("key"));
    }
    catch (CommitConflictException e) {
      testFailed = true;
      fail("CommitConflitException encountered");
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to exception" + e);
    }
  }

  /**
   * Test to force a recovery to follow the path of switchOutFilesForRecovery
   * and ensuring that IOExceptions do not come as a result. This is also a bug test for
   * bug 37682
   * @throws Exception
   */
  @Test
  public void testSwitchFilesForRecovery() throws Exception
  {
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, null, Scope.LOCAL);
    put100Int();
    region.forceRolling();
    Thread.sleep(2000);
    put100Int();
    int sizeOfRegion = region.size();
    region.close();
    //this variable will set to false in the src code itself
    //NewLBHTreeDiskRegion.setJdbmexceptionOccuredToTrueForTesting = true;
    try {
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, null, Scope.LOCAL);
    }
    catch (Exception e) {
      fail("failed in recreating region due to"+e);
    } finally {
      //NewLBHTreeDiskRegion.setJdbmexceptionOccuredToTrueForTesting = false;
    }
    if (sizeOfRegion != region.size()) {
      fail(" Expected region size to be " + sizeOfRegion
          + " after recovery but it is " + region.size());
    }
  }
  
  /**
   * tests directory stats are correctly updated in case of single directory
   * (for bug 37531)
   */
  @Test
  public void testPersist1DirStats()
  {
    final AtomicBoolean freezeRoller = new AtomicBoolean();
    CacheObserver old = CacheObserverHolder
          .setInstance(new CacheObserverAdapter() {
              private volatile boolean didBeforeCall = false;
      @Override
      public void beforeGoingToCompact() {
        this.didBeforeCall = true;
        synchronized (freezeRoller) {
          if (!assertDone) {
          	try {
          	  // Here, we are not allowing the Roller thread to roll the old oplog into htree
                  while (!freezeRoller.get()) {
                    freezeRoller.wait();
                  }
                  freezeRoller.set(false);
          	}
          	catch (InterruptedException e) {
                  fail("interrupted");
          	}
          }
        }
      }
      @Override
      public void afterHavingCompacted() {
        if (this.didBeforeCall) {
          this.didBeforeCall = false;
          LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
          //assertTrue("Assert failure for DSpaceUsage in afterHavingCompacted ", diskSpaceUsageStats() == calculatedDiskSpaceUsageStats());
          // what is the point of this assert?
          checkDiskStats();
        }
      }
    });
    try {
      final int MAX_OPLOG_SIZE = 500;
      diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
      diskProps.setPersistBackup(true);
      diskProps.setRolling(true);
      diskProps.setSynchronous(true);
      diskProps.setOverflow(false);
      diskProps.setDiskDirsAndSizes(new File[] { dirs[0] }, new int[] { 4000 });
      final byte[] val = new byte[200];
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskProps, Scope.LOCAL);
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      region.put("key1", val);
      // Disk space should have changed due to 1 put
      //assertTrue("stats did not increase after put 1 ", diskSpaceUsageStats() == calculatedDiskSpaceUsageStats());
      checkDiskStats();
      region.put("key2", val);
      //assertTrue("stats did not increase after put 2", diskSpaceUsageStats() == calculatedDiskSpaceUsageStats());
      checkDiskStats();
      // This put will cause a switch as max-oplog size (500) will be exceeded (600)
      region.put("key3", val);
      synchronized (freezeRoller) {
        //assertTrue("current disk space usage with Roller thread in wait and put key3 done is incorrect " +  diskSpaceUsageStats() + " " + calculatedDiskSpaceUsageStats(), diskSpaceUsageStats()== calculatedDiskSpaceUsageStats());
        checkDiskStats();
        assertDone = true;
        freezeRoller.set(true);
        freezeRoller.notifyAll();
      }

      region.close();
      closeDown();
//    Stop rolling to get accurate estimates:
      diskProps.setRolling(false);
      
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
    	        diskProps, Scope.LOCAL);
      
      // On recreating the region after closing, old Oplog file gets rolled into htree
      // "Disk space usage zero when region recreated"
      checkDiskStats();
      region.put("key4", val);
      //assertTrue("stats did not increase after put 4", diskSpaceUsageStats() == calculatedDiskSpaceUsageStats());
      checkDiskStats();
      region.put("key5", val);
      //assertTrue("stats did not increase after put 5", diskSpaceUsageStats() == calculatedDiskSpaceUsageStats());
      checkDiskStats();
      assertDone = false;
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      region.put("key6", val);
      // again we expect a switch in oplog here
      synchronized (freezeRoller) {
        //assertTrue("current disk space usage with Roller thread in wait and put key6 done is incorrect", diskSpaceUsageStats()== calculatedDiskSpaceUsageStats());
        checkDiskStats();
        assertDone = true;
        freezeRoller.set(true);
        freezeRoller.notifyAll();
      }
      region.close();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to exception" + e);
    }
    finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(old);
      synchronized (freezeRoller) {
        assertDone = true;
        freezeRoller.set(true);
        freezeRoller.notifyAll();
      }
    }
  }
  
  /**
   * Tests reduction in size of disk stats 
   * when the oplog is rolled.
   */
  @Test
  public void testStatsSizeReductionOnRolling() throws Exception 
  {
    final int MAX_OPLOG_SIZE = 500*2;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setCompactionThreshold(100);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    diskProps.setDiskDirsAndSizes(new File[] { dirs[0] }, new int[] { 4000 });
    final byte[] val = new byte[333];
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
      diskProps, Scope.LOCAL);
    final DiskRegion dr = ((LocalRegion)region).getDiskRegion(); 
    final Object lock = new Object();
    final boolean [] exceptionOccured = new boolean[] {true};
    final boolean [] okToExit = new boolean[] {false};
    final boolean [] switchExpected = new boolean[] {false};
    
    // calculate sizes
    final int extra_byte_num_per_entry = InternalDataSerializer.calculateBytesForTSandDSID(getDSID((LocalRegion)region));
    final int key3_size = DiskOfflineCompactionJUnitTest.getSize4Create(extra_byte_num_per_entry, "key3", val);
    final int tombstone_key1 = DiskOfflineCompactionJUnitTest.getSize4TombstoneWithKey(extra_byte_num_per_entry, "key1");
    final int tombstone_key2 = DiskOfflineCompactionJUnitTest.getSize4TombstoneWithKey(extra_byte_num_per_entry, "key2");

    CacheObserver old = CacheObserverHolder
          .setInstance(new CacheObserverAdapter() {
              private long before = -1;
              private DirectoryHolder dh = null;
              private long oplogsSize = 0;

              @Override
              public void beforeSwitchingOplog() {
                cache.getLogger().info("beforeSwitchingOplog");
                if (!switchExpected[0]) {
                  fail("unexpected oplog switch");
                }
                if (before == -1) {
                  // only want to call this once; before the 1st oplog destroy
                  this.dh = dr.getNextDir();
                  this.before = this.dh.getDirStatsDiskSpaceUsage();
                }
              }
              @Override
              public void beforeDeletingCompactedOplog(Oplog oplog)
              {
                cache.getLogger().info("beforeDeletingCompactedOplog");
                oplogsSize += oplog.getOplogSize();
              }
              @Override
              public void afterHavingCompacted() {
                cache.getLogger().info("afterHavingCompacted");
                if(before > -1) {
                  synchronized(lock) {            
                    okToExit[0] = true;
                    long after = this.dh.getDirStatsDiskSpaceUsage();
                    // after compaction, in _2.crf, key3 is an create-entry, 
                    // key1 and key2 are tombstones. 
                    // _2.drf contained a rvvgc with drMap.size()==1
                    int expected_drf_size = 
                      Oplog.OPLOG_DISK_STORE_REC_SIZE +
                      Oplog.OPLOG_MAGIC_SEQ_REC_SIZE +
                      Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE +
                      DiskOfflineCompactionJUnitTest.getRVVSize(1, new int[] {0}, true);
                    int expected_crf_size = 
                      Oplog.OPLOG_DISK_STORE_REC_SIZE + 
                      Oplog.OPLOG_MAGIC_SEQ_REC_SIZE + 
                      Oplog.OPLOG_GEMFIRE_VERSION_REC_SIZE + 
                      DiskOfflineCompactionJUnitTest.getRVVSize(1, new int[] {1}, false) + 
                      Oplog.OPLOG_NEW_ENTRY_BASE_REC_SIZE + 
                      key3_size + tombstone_key1 + tombstone_key2;
                    int oplog_2_size = expected_drf_size + expected_crf_size; 
                    if (after != oplog_2_size) {
                      cache.getLogger().info("test failed before=" + before
                                             + " after=" + after
                                             + " oplogsSize=" + oplogsSize);
                      exceptionOccured[0] = true;
                    }else {
                      exceptionOccured[0] = false;
                    }
                    LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
                    lock.notify();
                  }
                }
              }
            });
    try {
      
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      cache.getLogger().info("putting key1");
      region.put("key1", val);
      // Disk space should have changed due to 1 put
      //assertTrue("stats did not increase after put 1 ", diskSpaceUsageStats() == calculatedDiskSpaceUsageStats());
      checkDiskStats();
      cache.getLogger().info("putting key2");
      region.put("key2", val);
      //assertTrue("stats did not increase after put 2", diskSpaceUsageStats() == calculatedDiskSpaceUsageStats());
      checkDiskStats();

      cache.getLogger().info("removing key1");
      region.remove("key1");
      cache.getLogger().info("removing key2");
      region.remove("key2");

      // This put will cause a switch as max-oplog size (900) will be exceeded (999)
      switchExpected[0] = true;
      cache.getLogger().info("putting key3");
      region.put("key3", val);
      cache.getLogger().info("waiting for compaction");
      synchronized(lock) {
        if (!okToExit[0]) {
          lock.wait(9000);
          assertTrue(okToExit[0]);
        }
        assertFalse(exceptionOccured[0]);
      }

      region.close();
    }
    finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
      CacheObserverHolder.setInstance(old);      
    }
  }

  // @todo this test is broken; size1 can keep changing since the roller will
  // keep copying forward forever. Need to change it so copy forward oplogs
  // will not be compacted so that size1 reaches a steady state
  /**
   * Tests stats verification with rolling enabled
   */
//   @Test
//  public void testSizeStatsAfterRecreationWithRollingEnabled() throws Exception 
//   {
//     final int MAX_OPLOG_SIZE = 500;
//     diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
//     diskProps.setPersistBackup(true);
//     diskProps.setRolling(true);
//     diskProps.setCompactionThreshold(100);
//     diskProps.setSynchronous(true);
//     diskProps.setOverflow(false);
//     diskProps.setDiskDirsAndSizes(new File[] { dirs[0] }, new int[] { 4000 });
//     final byte[] val = new byte[200];
//     region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
//       diskProps);
//     final DiskRegion dr = ((LocalRegion)region).getDiskRegion(); 
//     final Object lock = new Object();
//     final boolean [] exceptionOccured = new boolean[] {true};
//     final boolean [] okToExit = new boolean[] {false}; 
    
//     CacheObserver old = CacheObserverHolder
//           .setInstance(new CacheObserverAdapter() {
//               private long before = -1;             
//               public void beforeDeletingCompactedOplog(Oplog rolledOplog)
//               {
//                 if (before == -1) {
//                   // only want to call this once; before the 1st oplog destroy
//                   before = dr.getNextDir().getDirStatsDiskSpaceUsage();
//                 }
//               }
//               public void afterHavingCompacted() {
//                 if(before > -1) {
//                   synchronized(lock) {            
//                     okToExit[0] = true;
//                     long after = dr.getNextDir().getDirStatsDiskSpaceUsage();;
//                     exceptionOccured[0] = false;            
//                     LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
//                     lock.notify();
//                   }
//                 }
//               }
//             });
//     try {
      
//       LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
//       region.put("key1", val);     
//       region.put("key2", val);      
//       // This put will cause a switch as max-oplog size (500) will be exceeded (600)
//       region.put("key3", val);
//       synchronized(lock) {
//         if (!okToExit[0]) {
//           lock.wait(9000);
//           assertTrue(okToExit[0]);
//         }
//         assertFalse(exceptionOccured[0]);
//       }
//       while (region.forceCompaction() != null) {
//         // wait until no more oplogs to compact
//         Thread.sleep(50);
//       }
//       long size1 =0;
//       for(DirectoryHolder dh:dr.getDirectories()) {
//         cache.getLogger().info(" dir=" + dh.getDir()
//                                + " size1=" + dh.getDirStatsDiskSpaceUsage());
//         size1 += dh.getDirStatsDiskSpaceUsage();
//       }     
//       System.out.println("Size before closing= "+ size1);
//       region.close();
//       diskProps.setRolling(false);
//       region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
//           diskProps);
      
//       long size2 =0;
//       for(DirectoryHolder dh:((LocalRegion)region).getDiskRegion().getDirectories()) {
//         cache.getLogger().info(" dir=" + dh.getDir()
//                                + " size2=" + dh.getDirStatsDiskSpaceUsage());
//         size2 += dh.getDirStatsDiskSpaceUsage();
//       }
//       System.out.println("Size after recreation= "+ size2);
//       assertEquals(size1, size2);
//       region.close();
      
//     }
//     finally {
//       LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
//       CacheObserverHolder.setInstance(old);      
//     }
//   }

  // This test is not valid. When listenForDataSerializeChanges is called
  // it ALWAYS does vrecman writes and a commit. Look at saveInstantiators
  // and saveDataSerializers to see these commit calls.
  // These calls can cause the size of the files to change.
   /**
    * Tests if without rolling the region size before close is same as after 
    * recreation
    */
  @Test
  public void testSizeStatsAfterRecreation() throws Exception 
   {
     final int MAX_OPLOG_SIZE = 500;
     diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
     diskProps.setPersistBackup(true);
     diskProps.setRolling(false);
     diskProps.setSynchronous(true);
     diskProps.setOverflow(false);
     diskProps.setDiskDirsAndSizes(new File[] { dirs[0],dirs[1] }, new int[] { 4000,4000 });
     final byte[] val = new byte[200];
     region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
       diskProps, Scope.LOCAL);
     DiskRegion dr = ((LocalRegion)region).getDiskRegion(); 
    
     try {      
       LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
       for(int i = 0; i < 8;++i) {
         region.put("key"+i, val);
       }
       long size1 =0;
       for(DirectoryHolder dh:dr.getDirectories()) {
         size1 += dh.getDirStatsDiskSpaceUsage();
       }
       System.out.println("Size before close = "+ size1);
       region.close();
       region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
           diskProps, Scope.LOCAL);
       dr = ((LocalRegion)region).getDiskRegion();
       long size2 =0;
       for(DirectoryHolder dh:dr.getDirectories()) {
         size2 += dh.getDirStatsDiskSpaceUsage();
       }
       System.out.println("Size after recreation= "+ size2);
       assertEquals(size1, size2);
       region.close();
     }
     finally {
       LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;          
     }
   }
   
  @Test
  public void testUnPreblowOnRegionCreate() throws Exception {
    final int MAX_OPLOG_SIZE = 20000;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    diskProps.setDiskDirsAndSizes(new File[] { dirs[0] }, new int[] { 40000 });
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);

    try {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      for (int i = 0; i < 10; ++i) {
        region.put("key-" + i, "value-");
      }

      assertEquals(18000, getOplogFileSizeSum(dirs[0], ".crf"));
      assertEquals(2000, getOplogFileSizeSum(dirs[0], ".drf"));

      // make a copy of inflated crf. use this to replace compacted crf to
      // simulate incomplete diskStore close
      File[] files = dirs[0].listFiles();
      for (File file : files) {
        if (file.getName().endsWith(".crf") || file.getName().endsWith(".drf")) {
          File inflated = new File(file.getAbsolutePath() + "_inflated");
          FileUtils.copyFile(file, inflated);
        }
      }

      cache.close();
      assertTrue(500 > getOplogFileSizeSum(dirs[0], ".crf"));
      assertTrue(100 > getOplogFileSizeSum(dirs[0], ".drf"));
      
      // replace compacted crf with inflated crf and remove krf
      files = dirs[0].listFiles();
      for (File file : files) {
        String name = file.getName();
        if (name.endsWith(".krf") || name.endsWith(".crf") || name.endsWith(".drf")) {
          file.delete();
        }        
      }
      for (File file : files) {
        String name = file.getName();
        if (name.endsWith("_inflated")) {
          assertTrue(file.renameTo(new File(file.getAbsolutePath().replace("_inflated", ""))));
        }        
      }
      assertEquals(18000, getOplogFileSizeSum(dirs[0], ".crf"));
      assertEquals(2000, getOplogFileSizeSum(dirs[0], ".drf"));

      createCache();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskProps, Scope.LOCAL);
      for (int i = 10; i < 20; ++i) {
        region.put("key-" + i, "value-");
      }

      int sizeCrf = getOplogFileSizeSum(dirs[0], ".crf");
      assertTrue("crf too big:" + sizeCrf, sizeCrf < 18000 + 500);
      assertTrue("crf too small:" + sizeCrf, sizeCrf > 18000);
      int sizeDrf = getOplogFileSizeSum(dirs[0], ".drf");
      assertTrue("drf too big:" + sizeDrf, sizeDrf < 2000 + 100);
      assertTrue("drf too small:" + sizeDrf, sizeDrf > 2000);

      // test that region recovery does not cause unpreblow
      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
          diskProps, Scope.LOCAL);

      assertEquals(sizeCrf, getOplogFileSizeSum(dirs[0], ".crf"));
      assertEquals(sizeDrf, getOplogFileSizeSum(dirs[0], ".drf"));
    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    }
  }

  private int getOplogFileSizeSum(File dir, String type) {
    int sum = 0;
    File[] files = dir.listFiles();
    for (File file : files) {
      String name = file.getName();
      if (name.endsWith(type)) {
        sum += file.length();
      }
    }
    return sum;
  }
   
  @Test
  public void testMagicSeqPresence() throws Exception {
    final int MAX_OPLOG_SIZE = 200;
    diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
    diskProps.setPersistBackup(true);
    diskProps.setRolling(true);
    diskProps.setSynchronous(true);
    diskProps.setOverflow(false);
    diskProps.setDiskDirsAndSizes(new File[] { dirs[0] }, new int[] { 4000 });
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

    // 3 types of oplog files will be verified
    verifyOplogHeader(dirs[0], ".if", ".crf", ".drf");

    try {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
      IntStream.range(0, 20).forEach(i -> region.put("key-" + i, "value-" + i));
      // krf is created, so 4 types of oplog files will be verified
      verifyOplogHeader(dirs[0], ".if", ".crf", ".drf", ".krf");

      region.close();
      region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps, Scope.LOCAL);

      verifyOplogHeader(dirs[0], ".if", ".crf", ".drf", ".krf");
      region.close();
    } finally {
      LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;
    }
  }

  private void verifyOplogHeader(File dir, String ... oplogTypes) throws IOException {
    
    Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
      List<String> types = new ArrayList<>(Arrays.asList(oplogTypes));
      Arrays.stream(dir.listFiles()).map(File::getName).map(f -> f.substring(f.indexOf("."))).forEach(types::remove);
      return types.isEmpty();
    });
    
    File[] files = dir.listFiles();
     HashSet<String> verified = new HashSet<String>();
     for (File file : files) {
       String name = file.getName();
       byte[] expect = new byte[Oplog.OPLOG_MAGIC_SEQ_REC_SIZE];
       if (name.endsWith(".crf")) {
         expect[0] = Oplog.OPLOG_MAGIC_SEQ_ID;
         System.arraycopy(OPLOG_TYPE.CRF.getBytes(), 0, expect, 1, OPLOG_TYPE.getLen());
         verified.add(".crf");
       } else if (name.endsWith(".drf")) {
         expect[0] = Oplog.OPLOG_MAGIC_SEQ_ID;
         System.arraycopy(OPLOG_TYPE.DRF.getBytes(), 0, expect, 1, OPLOG_TYPE.getLen());
         verified.add(".drf");
       } else if (name.endsWith(".krf")) {
         expect[0] = Oplog.OPLOG_MAGIC_SEQ_ID;
         System.arraycopy(OPLOG_TYPE.KRF.getBytes(), 0, expect, 1, OPLOG_TYPE.getLen());
         verified.add(".krf");
       } else if (name.endsWith(".if")) {
         expect[0] = DiskInitFile.OPLOG_MAGIC_SEQ_ID;
         System.arraycopy(OPLOG_TYPE.IF.getBytes(), 0, expect, 1, OPLOG_TYPE.getLen());
         verified.add(".if");
       } else {
         System.out.println("Ignored: " + file);
         continue;
       }
       expect[expect.length-1] = 21; // EndOfRecord
       
       byte[] buf = new byte[Oplog.OPLOG_MAGIC_SEQ_REC_SIZE];
       
       FileInputStream fis = new FileInputStream(file);
       int count = fis.read(buf, 0, 8);
       fis.close();
       
       System.out.println("Verifying: " + file);
       assertEquals("expected a read to return 8 but it returned " + count + " for file " + file, 8, count);
       assertTrue(Arrays.equals(expect, buf));
     }
     
     assertEquals(oplogTypes.length, verified.size());
  }
  
   /**
    * Tests if without rolling the region size before close is same as after 
    * recreation
    */
  @Test
  public void testSizeStatsAfterRecreationInAsynchMode() throws Exception 
   {
     final int MAX_OPLOG_SIZE = 1000;
     diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
     diskProps.setPersistBackup(true);
     diskProps.setRolling(false);
     diskProps.setSynchronous(false);
     diskProps.setBytesThreshold(800);
     diskProps.setOverflow(false);
     diskProps.setDiskDirsAndSizes(new File[] { dirs[0],dirs[1] }, new int[] { 4000,4000 });
     final byte[] val = new byte[25];
     region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
       diskProps);
     DiskRegion dr = ((LocalRegion)region).getDiskRegion(); 
  
     try {      
       LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
       for(int i = 0; i < 42;++i) {
         region.put("key"+i, val);
       }
       // need to wait for writes to happen before getting size
       dr.flushForTesting();
       long size1 =0;
       for(DirectoryHolder dh:dr.getDirectories()) {
         size1 += dh.getDirStatsDiskSpaceUsage();
       }
       System.out.println("Size before close = "+ size1);
       region.close();
       diskProps.setSynchronous(true);
       region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
           diskProps, Scope.LOCAL);
       dr = ((LocalRegion)region).getDiskRegion();
       long size2 =0;
       for(DirectoryHolder dh:dr.getDirectories()) {
         size2 += dh.getDirStatsDiskSpaceUsage();
       }
       System.out.println("Size after recreation= "+ size2);
       assertEquals(size1, size2);
       region.close();
     }
     finally {
       LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;          
     }
   }
  
  

  @Test
  public void testAsynchModeStatsBehaviour() throws Exception 
   {
     final int MAX_OPLOG_SIZE = 1000;
     diskProps.setMaxOplogSize(MAX_OPLOG_SIZE);
     diskProps.setPersistBackup(true);
     diskProps.setRolling(false);
     diskProps.setSynchronous(false);
     diskProps.setBytesThreshold(800);
     diskProps.setTimeInterval(Long.MAX_VALUE);
     diskProps.setOverflow(false);
     diskProps.setDiskDirsAndSizes(new File[] { dirs[0] }, new int[] { 4000});
     final byte[] val = new byte[25];
     region = DiskRegionHelperFactory.getAsyncPersistOnlyRegion(cache,
       diskProps);
     DiskRegion dr = ((LocalRegion)region).getDiskRegion(); 
    
     try {      
       LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = true;
       for(int i = 0; i < 4;++i) {
         region.put("key"+i, val);
       }
       // This test now has a race condition in it since
       // async puts no longer increment disk space.
       // It is not until a everything is flushed that we will know the disk size.
       dr.flushForTesting();
       
       checkDiskStats();
       long size1 =0;
       for(DirectoryHolder dh:dr.getDirectories()) {
         size1 += dh.getDirStatsDiskSpaceUsage();
       }
       System.out.println("Size before close = "+ size1);
       region.close();
       diskProps.setSynchronous(true);
       region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
           diskProps, Scope.LOCAL);
       dr = ((LocalRegion)region).getDiskRegion();
       long size2 =0;
       for(DirectoryHolder dh:dr.getDirectories()) {
         size2 += dh.getDirStatsDiskSpaceUsage();
       }
       System.out.println("Size after recreation= "+ size2);
       assertEquals(size1, size2);
       region.close();
     }
     finally {
       LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER = false;          
     }
   }

  protected long diskSpaceUsageStats() {
  	return ((LocalRegion)region).getDiskRegion().getInfoFileDir().getDirStatsDiskSpaceUsage();
  }

  protected long calculatedDiskSpaceUsageStats() {
    long oplogSize = oplogSize();
//     cache.getLogger().info(" oplogSize=" + oplogSize
//                            + " statSize=" + diskSpaceUsageStats());
    return oplogSize;
  }
  private void checkDiskStats() {
    long actualDiskStats = diskSpaceUsageStats();
    long computedDiskStats = calculatedDiskSpaceUsageStats();
    int tries=0;
    while (actualDiskStats != computedDiskStats && tries++ <= 100) {
      // race conditions exist in which the stats change
      try { Thread.sleep(100); } catch (InterruptedException ignore) {}
      actualDiskStats = diskSpaceUsageStats();
      computedDiskStats = calculatedDiskSpaceUsageStats();
    }
    assertEquals(computedDiskStats, actualDiskStats);
  }

  private long oplogSize() {
    long size = ((LocalRegion)region).getDiskRegion().getDiskStore().undeletedOplogSize.get();
//     cache.getLogger().info("undeletedOplogSize=" + size);
    Oplog [] opArray = ((LocalRegion)region).getDiskRegion().getDiskStore().persistentOplogs.getAllOplogs();
    if((opArray != null) && (opArray.length != 0)){
      for (int j = 0; j < opArray.length; ++j) {
    	  size += opArray[j].getOplogSize();
//           cache.getLogger().info("oplog#" + opArray[j].getOplogId()
//                                  + ".size=" + opArray[j].getOplogSize());
      }
    }
    return size;
  }
  private int getDSID(LocalRegion lr) {
    return lr.getDistributionManager().getDistributedSystemId();    
  }
}
