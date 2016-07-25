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

import java.io.File;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.cache.DiskInitFile.DiskRegionFlag;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl.OplogEntryIdSet;
import com.gemstone.gemfire.internal.cache.persistence.DiskRecoveryStore;
import com.gemstone.gemfire.internal.cache.persistence.DiskStoreID;
import com.gemstone.gemfire.internal.cache.versions.DiskRegionVersionVector;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class OplogRVVJUnitTest {

  private File testDirectory;
  private Mockery context = new Mockery() {{
    setImposteriser(ClassImposteriser.INSTANCE);
  }};

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    testDirectory = temporaryFolder.newFolder("_" + getClass().getSimpleName());
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = true;
  }

  @After
  public void tearDown() throws Exception {
    DiskStoreImpl.SET_IGNORE_PREALLOCATE = false;
  }

  @Test
  public void testRecoverRVV() throws UnknownHostException {
    final DiskInitFile df = context.mock(DiskInitFile.class);
    final LogWriterI18n logger = context.mock(LogWriterI18n.class);
    final GemFireCacheImpl cache = context.mock(GemFireCacheImpl.class);
    //Create a mock disk store impl. 
    final DiskStoreImpl parent = context.mock(DiskStoreImpl.class);
    final StatisticsFactory sf = context.mock(StatisticsFactory.class);
    final DiskStoreID ownerId = DiskStoreID.random();
    final DiskStoreID m1 = DiskStoreID.random();
    final DiskStoreID m2 = DiskStoreID.random();
    final DiskRecoveryStore drs = context.mock(DiskRecoveryStore.class);

    context.checking(new Expectations() {{
      ignoring(sf);
      allowing(df).getOrCreateCanonicalId(m1);
      will(returnValue(1));
      allowing(df).getOrCreateCanonicalId(m2);
      will(returnValue(2));
      allowing(df).getOrCreateCanonicalId(ownerId);
      will(returnValue(3));
      allowing(df).getCanonicalObject(1);
      will(returnValue(m1));
      allowing(df).getCanonicalObject(2);
      will(returnValue(m2));
      allowing(df).getCanonicalObject(3);
      will(returnValue(ownerId));
      ignoring(df);
    }});
    DirectoryHolder dirHolder = new DirectoryHolder(sf, testDirectory, 0, 0);

    context.checking(new Expectations() {{
      ignoring(logger);
      allowing(cache).getLoggerI18n();
      will(returnValue(logger));
      allowing(cache).cacheTimeMillis();
      will(returnValue(System.currentTimeMillis()));
      allowing(parent).getCache();
      will(returnValue(cache));
      allowing(parent).getMaxOplogSizeInBytes();
      will(returnValue(10000L));
      allowing(parent).getName();
      will(returnValue("test"));
      allowing(parent).getStats();
      will(returnValue(new DiskStoreStats(sf, "stats")));
      allowing(parent).getDiskInitFile();
      will(returnValue(df));
      allowing(parent).getDiskStoreID();
      will(returnValue(DiskStoreID.random()));
    }});
    
    final DiskRegionVersionVector rvv = new DiskRegionVersionVector(ownerId);
    rvv.recordVersion(m1, 0);
    rvv.recordVersion(m1, 1);
    rvv.recordVersion(m1, 2);
    rvv.recordVersion(m1, 10);
    rvv.recordVersion(m1, 7);
    rvv.recordVersion(m2, 0);
    rvv.recordVersion(m2, 1);
    rvv.recordVersion(m2, 2);
    rvv.recordGCVersion(m1, 1);
    rvv.recordGCVersion(m2, 0);
    
    //create the oplog

    final AbstractDiskRegion diskRegion = context.mock(AbstractDiskRegion.class);
    final PersistentOplogSet oplogSet = context.mock(PersistentOplogSet.class);
    final Map<Long, AbstractDiskRegion> map = new HashMap<Long, AbstractDiskRegion>();
    map.put(5L, diskRegion);
    context.checking(new Expectations() {{
      allowing(diskRegion).getRegionVersionVector();
      will(returnValue(rvv));
      allowing(diskRegion).getRVVTrusted();
      will(returnValue(true));
      allowing(parent).getAllDiskRegions();
      will(returnValue(map));
      allowing(oplogSet).getCurrentlyRecovering(5L);
      will(returnValue(drs));
      allowing(oplogSet).getParent();
      will(returnValue(parent));
      ignoring(oplogSet);
      ignoring(parent);
      allowing(diskRegion).getFlags();
      will(returnValue(EnumSet.of(DiskRegionFlag.IS_WITH_VERSIONING)));
    }});
    
    Map<Long, AbstractDiskRegion> regions = parent.getAllDiskRegions();
    
    Oplog oplog = new Oplog(1, oplogSet, dirHolder);
    oplog.close();
    
    context.checking(new Expectations() {{
      one(drs).recordRecoveredGCVersion(m1, 1);
      one(drs).recordRecoveredGCVersion(m2, 0);
      one(drs).recordRecoveredVersonHolder(ownerId, rvv.getMemberToVersion().get(ownerId), true);
      one(drs).recordRecoveredVersonHolder(m1, rvv.getMemberToVersion().get(m1), true);
      one(drs).recordRecoveredVersonHolder(m2, rvv.getMemberToVersion().get(m2), true);
      one(drs).setRVVTrusted(true);
    }});

    oplog = new Oplog(1, oplogSet);
    File drfFile = FileUtil.find(testDirectory, ".*.drf");
    File crfFile = FileUtil.find(testDirectory, ".*.crf");
    oplog.addRecoveredFile(drfFile, dirHolder);
    oplog.addRecoveredFile(crfFile, dirHolder);
    OplogEntryIdSet deletedIds = new OplogEntryIdSet();
    oplog.recoverDrf(deletedIds, false, true);
    oplog.recoverCrf(deletedIds, true, true, false, Collections.singleton(oplog), true);
    context.assertIsSatisfied();
  }

}
