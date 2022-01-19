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
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Test;

import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.cache.persistence.PRPersistentConfig;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;

/**
 * Tests DiskRegion's IF file.
 *
 */
public class DiskIFJUnitTest extends DiskRegionTestingBase {
  DiskRegionProperties diskProps = new DiskRegionProperties();

  @Test
  public void testEmptyIF() throws Exception {
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("testEmptyIF");

    LocalRegion lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskProps, Scope.LOCAL);
    DiskRegion dr = lr.getDiskRegion();
    assertEquals(null, dr.getMyInitializingID());
    PersistentMemberID myId = dr.getMyPersistentID();
    assertNotNull(myId);
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertTrue(dr.getOfflineAndEqualMembers().isEmpty());

    // do recovery
    close(lr);
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(null, dr.getMyInitializingID());
    assertEquals(myId, dr.getMyPersistentID());
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertTrue(dr.getOfflineAndEqualMembers().isEmpty());

    dr.forceIFCompaction();
    assertEquals(null, dr.getMyInitializingID());
    assertEquals(myId, dr.getMyPersistentID());
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertTrue(dr.getOfflineAndEqualMembers().isEmpty());

    // do recovery
    close(lr);
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(null, dr.getMyInitializingID());
    assertEquals(myId, dr.getMyPersistentID());
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertTrue(dr.getOfflineAndEqualMembers().isEmpty());
  }

  @Test
  public void testMyPMID() throws Exception {
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("testMyPMID");
    LocalRegion lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskProps, Scope.LOCAL);
    DiskRegion dr = lr.getDiskRegion();
    PersistentMemberID myId = createNewPMID();
    assertEquals(null, dr.getMyInitializingID());
    PersistentMemberID oldId = dr.getMyPersistentID();
    dr.setInitializing(myId);
    assertEquals(myId, dr.getMyInitializingID());
    assertEquals(oldId, dr.getMyPersistentID());
    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(myId, dr.getMyInitializingID());
    assertEquals(oldId, dr.getMyPersistentID());

    dr.forceIFCompaction();
    assertEquals(myId, dr.getMyInitializingID());
    assertEquals(oldId, dr.getMyPersistentID());
    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(myId, dr.getMyInitializingID());
    assertEquals(oldId, dr.getMyPersistentID());

    dr.setInitialized();
    assertEquals(null, dr.getMyInitializingID());
    assertEquals(myId, dr.getMyPersistentID());
    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(null, dr.getMyInitializingID());
    assertEquals(myId, dr.getMyPersistentID());

    dr.forceIFCompaction();
    assertEquals(null, dr.getMyInitializingID());
    assertEquals(myId, dr.getMyPersistentID());
    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(null, dr.getMyInitializingID());
    assertEquals(myId, dr.getMyPersistentID());
  }

  @Test
  public void testMemberIdSets() throws Exception {
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("testMemberIdSets");
    LocalRegion lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskProps, Scope.LOCAL);
    DiskRegion dr = lr.getDiskRegion();
    PersistentMemberID myOnId = createNewPMID();
    PersistentMemberID myOnId2 = createNewPMID();
    PersistentMemberID myOffId = createNewPMID();
    PersistentMemberID myEqualsId = createNewPMID();
    dr.memberOnline(myOnId);
    dr.memberOnline(myOnId2);
    dr.memberOffline(myOffId);
    dr.memberOfflineAndEqual(myEqualsId);
    assertEquals(TestUtils.asSet(myOnId, myOnId2), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualsId), dr.getOfflineAndEqualMembers());
    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(TestUtils.asSet(myOnId, myOnId2), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualsId), dr.getOfflineAndEqualMembers());
    dr.forceIFCompaction();
    assertEquals(TestUtils.asSet(myOnId, myOnId2), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualsId), dr.getOfflineAndEqualMembers());
    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(TestUtils.asSet(myOnId, myOnId2), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualsId), dr.getOfflineAndEqualMembers());

    dr.memberOffline(myOnId);
    dr.memberOfflineAndEqual(myOnId2);
    HashSet<PersistentMemberID> expSet = new HashSet<>();
    expSet.add(myOffId);
    expSet.add(myOnId);
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertEquals(expSet, dr.getOfflineMembers());
    HashSet<PersistentMemberID> expSet2 = new HashSet<>();
    expSet2.add(myEqualsId);
    expSet2.add(myOnId2);
    assertEquals(expSet2, dr.getOfflineAndEqualMembers());
    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertEquals(expSet, dr.getOfflineMembers());
    assertEquals(expSet2, dr.getOfflineAndEqualMembers());
    dr.forceIFCompaction();
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertEquals(expSet, dr.getOfflineMembers());
    assertEquals(expSet2, dr.getOfflineAndEqualMembers());
    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertEquals(expSet, dr.getOfflineMembers());
    assertEquals(expSet2, dr.getOfflineAndEqualMembers());

    dr.memberOnline(myOnId);
    dr.memberOnline(myOnId2);
    dr.memberOnline(myOffId);
    dr.memberOnline(myEqualsId);
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertTrue(dr.getOfflineAndEqualMembers().isEmpty());
    expSet.add(myEqualsId);
    expSet.add(myOnId2);
    assertEquals(expSet, dr.getOnlineMembers());
    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertEquals(expSet, dr.getOnlineMembers());
    dr.forceIFCompaction();
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertEquals(expSet, dr.getOnlineMembers());
    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertEquals(expSet, dr.getOnlineMembers());

    dr.memberOffline(myOffId);
    dr.memberOfflineAndEqual(myEqualsId);
    assertEquals(TestUtils.asSet(myOnId, myOnId2), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualsId), dr.getOfflineAndEqualMembers());
    dr.memberRemoved(myOffId);
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertEquals(TestUtils.asSet(myOnId, myOnId2), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myEqualsId), dr.getOfflineAndEqualMembers());
    dr.memberRemoved(myOnId);
    dr.memberRemoved(myOnId2);
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertEquals(Collections.singleton(myEqualsId), dr.getOfflineAndEqualMembers());
    dr.memberRemoved(myEqualsId);
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertTrue(dr.getOfflineAndEqualMembers().isEmpty());
    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertTrue(dr.getOfflineAndEqualMembers().isEmpty());
    dr.forceIFCompaction();
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertTrue(dr.getOfflineAndEqualMembers().isEmpty());
    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertTrue(dr.getOfflineAndEqualMembers().isEmpty());
  }

  @Test
  public void testAboutToDestroy() throws Exception {
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("testAboutToDestroy");
    LocalRegion lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskProps, Scope.LOCAL);
    DiskRegion dr = lr.getDiskRegion();
    PersistentMemberID myId = createNewPMID();
    PersistentMemberID myOnId = createNewPMID();
    PersistentMemberID myOffId = createNewPMID();
    PersistentMemberID myEqualId = createNewPMID();
    dr.setInitializing(myId);
    dr.setInitialized();
    dr.memberOnline(myOnId);
    dr.memberOffline(myOffId);
    dr.memberOfflineAndEqual(myEqualId);
    assertEquals(Collections.singleton(myOnId), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualId), dr.getOfflineAndEqualMembers());
    assertEquals(null, dr.getMyInitializingID());
    assertEquals(myId, dr.getMyPersistentID());
    assertEquals(false, dr.wasAboutToDestroy());

    dr.beginDestroy(lr);
    assertEquals(Collections.singleton(myOnId), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualId), dr.getOfflineAndEqualMembers());
    assertEquals(myId, dr.getMyPersistentID());
    assertEquals(true, dr.wasAboutToDestroy());
    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(Collections.singleton(myOnId), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualId), dr.getOfflineAndEqualMembers());
    assertEquals(myId, dr.getMyPersistentID());
    assertEquals(true, dr.wasAboutToDestroy());

    dr.forceIFCompaction();
    assertEquals(Collections.singleton(myOnId), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualId), dr.getOfflineAndEqualMembers());
    assertEquals(myId, dr.getMyPersistentID());
    assertEquals(true, dr.wasAboutToDestroy());
    close(lr);

    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(Collections.singleton(myOnId), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualId), dr.getOfflineAndEqualMembers());
    assertEquals(myId, dr.getMyPersistentID());
    assertEquals(true, dr.wasAboutToDestroy());

    dr.endDestroy(lr);
    assertEquals(Collections.emptySet(), dr.getOnlineMembers());
    assertEquals(Collections.emptySet(), dr.getOfflineMembers());
    assertEquals(Collections.emptySet(), dr.getOfflineAndEqualMembers());
    assertEquals(false, dr.wasAboutToDestroy());

    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(Collections.emptySet(), dr.getOnlineMembers());
    assertEquals(Collections.emptySet(), dr.getOfflineMembers());
    assertEquals(Collections.emptySet(), dr.getOfflineAndEqualMembers());
    assertEquals(false, dr.wasAboutToDestroy());

    dr.forceIFCompaction();
    assertEquals(Collections.emptySet(), dr.getOnlineMembers());
    assertEquals(Collections.emptySet(), dr.getOfflineMembers());
    assertEquals(Collections.emptySet(), dr.getOfflineAndEqualMembers());
    assertEquals(false, dr.wasAboutToDestroy());
    close(lr);

    assertEquals(Collections.emptySet(), dr.getOnlineMembers());
    assertEquals(Collections.emptySet(), dr.getOfflineMembers());
    assertEquals(Collections.emptySet(), dr.getOfflineAndEqualMembers());
    assertEquals(false, dr.wasAboutToDestroy());
  }

  @Test
  public void testAboutToPartialDestroy() throws Exception {
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("testAboutToPartialDestroy");
    LocalRegion lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskProps, Scope.LOCAL);
    DiskRegion dr = lr.getDiskRegion();
    PersistentMemberID myId = createNewPMID();
    PersistentMemberID myOnId = createNewPMID();
    PersistentMemberID myOffId = createNewPMID();
    PersistentMemberID myEqualId = createNewPMID();
    dr.setInitializing(myId);
    dr.setInitialized();
    dr.memberOnline(myOnId);
    dr.memberOffline(myOffId);
    dr.memberOfflineAndEqual(myEqualId);
    assertEquals(Collections.singleton(myOnId), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualId), dr.getOfflineAndEqualMembers());
    assertEquals(null, dr.getMyInitializingID());
    assertEquals(myId, dr.getMyPersistentID());
    assertEquals(false, dr.wasAboutToDestroy());
    assertEquals(false, dr.wasAboutToDestroyDataStorage());

    dr.beginDestroyDataStorage();
    assertEquals(Collections.singleton(myOnId), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualId), dr.getOfflineAndEqualMembers());
    assertEquals(myId, dr.getMyPersistentID());
    assertEquals(false, dr.wasAboutToDestroy());
    assertEquals(true, dr.wasAboutToDestroyDataStorage());
    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(Collections.singleton(myOnId), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualId), dr.getOfflineAndEqualMembers());
    assertEquals(myId, dr.getMyPersistentID());
    assertEquals(false, dr.wasAboutToDestroy());
    assertEquals(true, dr.wasAboutToDestroyDataStorage());

    dr.forceIFCompaction();
    assertEquals(Collections.singleton(myOnId), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualId), dr.getOfflineAndEqualMembers());
    assertEquals(myId, dr.getMyPersistentID());
    assertEquals(false, dr.wasAboutToDestroy());
    assertEquals(true, dr.wasAboutToDestroyDataStorage());
    close(lr);

    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(Collections.singleton(myOnId), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualId), dr.getOfflineAndEqualMembers());
    assertEquals(myId, dr.getMyPersistentID());
    assertEquals(false, dr.wasAboutToDestroy());
    assertEquals(true, dr.wasAboutToDestroyDataStorage());

    dr.endDestroy(lr);
    assertEquals(Collections.singleton(myOnId), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualId), dr.getOfflineAndEqualMembers());
    assertEquals(null, dr.getMyPersistentID());
    assertEquals(false, dr.wasAboutToDestroy());
    assertEquals(false, dr.wasAboutToDestroyDataStorage());
    close(lr);

    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(Collections.singleton(myOnId), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualId), dr.getOfflineAndEqualMembers());
    PersistentMemberID newId = dr.getMyPersistentID();
    if (myId.equals(newId)) {
      fail("expected a new id but was: " + newId);
    }
    assertNotNull(newId);
    assertEquals(false, dr.wasAboutToDestroy());
    assertEquals(false, dr.wasAboutToDestroyDataStorage());

    dr.forceIFCompaction();
    assertEquals(Collections.singleton(myOnId), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualId), dr.getOfflineAndEqualMembers());
    assertEquals(newId, dr.getMyPersistentID());
    assertEquals(false, dr.wasAboutToDestroy());
    assertEquals(false, dr.wasAboutToDestroyDataStorage());
    close(lr);

    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(Collections.singleton(myOnId), dr.getOnlineMembers());
    assertEquals(Collections.singleton(myOffId), dr.getOfflineMembers());
    assertEquals(Collections.singleton(myEqualId), dr.getOfflineAndEqualMembers());
    assertEquals(newId, dr.getMyPersistentID());
    assertEquals(false, dr.wasAboutToDestroy());
    assertEquals(false, dr.wasAboutToDestroyDataStorage());
  }

  @Test
  public void testRegionConfigDefaults() throws Exception {
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("testRegionConfigDefaults");
    LocalRegion lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskProps, Scope.LOCAL);
    DiskRegion dr = lr.getDiskRegion();
    assertEquals(false, dr.hasConfigChanged());
    assertEquals(0, dr.getLruAlgorithm());
    assertEquals(0, dr.getLruAction());
    assertEquals(0, dr.getLruLimit());
    assertEquals(16, dr.getConcurrencyLevel());
    assertEquals(16, dr.getInitialCapacity());
    assertEquals(0.75f, dr.getLoadFactor(), 0.01);
    assertEquals(false, dr.getStatisticsEnabled());
    assertEquals(false, dr.isBucket());

    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(false, dr.hasConfigChanged());
    assertEquals(0, dr.getLruAlgorithm());
    assertEquals(0, dr.getLruAction());
    assertEquals(0, dr.getLruLimit());
    assertEquals(16, dr.getConcurrencyLevel());
    assertEquals(16, dr.getInitialCapacity());
    assertEquals(0.75f, dr.getLoadFactor(), 0.01);
    assertEquals(false, dr.getStatisticsEnabled());
    assertEquals(false, dr.isBucket());

    dr.forceIFCompaction();
    assertEquals(false, dr.hasConfigChanged());
    assertEquals(0, dr.getLruAlgorithm());
    assertEquals(0, dr.getLruAction());
    assertEquals(0, dr.getLruLimit());
    assertEquals(16, dr.getConcurrencyLevel());
    assertEquals(16, dr.getInitialCapacity());
    assertEquals(0.75f, dr.getLoadFactor(), 0.01);
    assertEquals(false, dr.getStatisticsEnabled());
    assertEquals(false, dr.isBucket());
    close(lr);

    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(false, dr.hasConfigChanged());
    assertEquals(0, dr.getLruAlgorithm());
    assertEquals(0, dr.getLruAction());
    assertEquals(0, dr.getLruLimit());
    assertEquals(16, dr.getConcurrencyLevel());
    assertEquals(16, dr.getInitialCapacity());
    assertEquals(0.75f, dr.getLoadFactor(), 0.01);
    assertEquals(false, dr.getStatisticsEnabled());
    assertEquals(false, dr.isBucket());
  }

  @Test
  public void testRegionConfigNonDefaults() throws Exception {
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("testRegionConfigNonDefaults");
    diskProps.setOverFlowCapacity(42);
    diskProps.setConcurrencyLevel(53);
    diskProps.setInitialCapacity(64);
    diskProps.setLoadFactor(0.31f);
    diskProps.setStatisticsEnabled(true);
    LocalRegion lr =
        (LocalRegion) DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    DiskRegion dr = lr.getDiskRegion();
    assertEquals(false, dr.hasConfigChanged());
    assertEquals(1, dr.getLruAlgorithm());
    assertEquals(2, dr.getLruAction());
    assertEquals(42, dr.getLruLimit());
    assertEquals(53, dr.getConcurrencyLevel());
    assertEquals(64, dr.getInitialCapacity());
    assertEquals(0.31f, dr.getLoadFactor(), 0.01);
    assertEquals(true, dr.getStatisticsEnabled());

    close(lr);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    dr = lr.getDiskRegion();
    assertEquals(false, dr.hasConfigChanged());
    assertEquals(1, dr.getLruAlgorithm());
    assertEquals(2, dr.getLruAction());
    assertEquals(42, dr.getLruLimit());
    assertEquals(53, dr.getConcurrencyLevel());
    assertEquals(64, dr.getInitialCapacity());
    assertEquals(0.31f, dr.getLoadFactor(), 0.01);
    assertEquals(true, dr.getStatisticsEnabled());

    dr.forceIFCompaction();
    assertEquals(false, dr.hasConfigChanged());
    assertEquals(1, dr.getLruAlgorithm());
    assertEquals(2, dr.getLruAction());
    assertEquals(42, dr.getLruLimit());
    assertEquals(53, dr.getConcurrencyLevel());
    assertEquals(64, dr.getInitialCapacity());
    assertEquals(0.31f, dr.getLoadFactor(), 0.01);
    assertEquals(true, dr.getStatisticsEnabled());
    close(lr);

    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    dr = lr.getDiskRegion();
    assertEquals(false, dr.hasConfigChanged());
    assertEquals(1, dr.getLruAlgorithm());
    assertEquals(2, dr.getLruAction());
    assertEquals(42, dr.getLruLimit());
    assertEquals(53, dr.getConcurrencyLevel());
    assertEquals(64, dr.getInitialCapacity());
    assertEquals(0.31f, dr.getLoadFactor(), 0.01);
    assertEquals(true, dr.getStatisticsEnabled());

    // @todo test isBucket == true
  }

  @Test
  public void testRegionConfigChange() throws Exception {
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("testRegionConfigChange");
    diskProps.setOverFlowCapacity(42);
    diskProps.setConcurrencyLevel(53);
    diskProps.setInitialCapacity(64);
    diskProps.setLoadFactor(0.31f);
    diskProps.setStatisticsEnabled(true);
    LocalRegion lr =
        (LocalRegion) DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    DiskRegion dr = lr.getDiskRegion();
    assertEquals(false, dr.hasConfigChanged());
    assertEquals(1, dr.getLruAlgorithm());
    assertEquals(2, dr.getLruAction());
    assertEquals(42, dr.getLruLimit());
    assertEquals(53, dr.getConcurrencyLevel());
    assertEquals(64, dr.getInitialCapacity());
    assertEquals(0.31f, dr.getLoadFactor(), 0.01);
    assertEquals(true, dr.getStatisticsEnabled());

    close(lr);

    diskProps.setOverFlowCapacity(999);
    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    dr = lr.getDiskRegion();
    assertEquals(false, dr.hasConfigChanged());
    assertEquals(1, dr.getLruAlgorithm());
    assertEquals(2, dr.getLruAction());
    assertEquals(999, dr.getLruLimit());
    assertEquals(53, dr.getConcurrencyLevel());
    assertEquals(64, dr.getInitialCapacity());
    assertEquals(0.31f, dr.getLoadFactor(), 0.01);
    assertEquals(true, dr.getStatisticsEnabled());
    close(lr);

    // do recovery
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache, diskProps);
    dr = lr.getDiskRegion();
    assertEquals(false, dr.hasConfigChanged());
    assertEquals(1, dr.getLruAlgorithm());
    assertEquals(2, dr.getLruAction());
    assertEquals(999, dr.getLruLimit());
    assertEquals(53, dr.getConcurrencyLevel());
    assertEquals(64, dr.getInitialCapacity());
    assertEquals(0.31f, dr.getLoadFactor(), 0.01);
    assertEquals(true, dr.getStatisticsEnabled());
  }

  /**
   * Make sure if we have multiple init file with the same ds name that ds creation fails. See bug
   * 41883.
   */
  @Test
  public void testTwoIFFiles() throws Exception {
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("testTwoIFFiles");
    diskProps.setDiskDirs(dirs);

    LocalRegion lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskProps, Scope.LOCAL);
    DiskRegion dr = lr.getDiskRegion();
    assertEquals(null, dr.getMyInitializingID());
    PersistentMemberID myId = dr.getMyPersistentID();
    assertNotNull(myId);
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertTrue(dr.getOfflineAndEqualMembers().isEmpty());

    // do recovery
    close(lr);
    assertEquals(true, (new File(dirs[0], "BACKUPtestTwoIFFiles.if")).exists());
    File extraIF = new File(dirs[1], "BACKUPtestTwoIFFiles.if");
    extraIF.createNewFile();
    try {
      lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
          Scope.LOCAL);
      fail("expected IllegalStateException");
    } catch (IllegalStateException ignored) {
    } finally {
      extraIF.delete();
    }
  }

  /**
   * See if we can find the init file if it is not in the first directory. See bug 41883.
   */
  @Test
  public void testIFFIleInSecondDir() throws Exception {
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("testIFFIleInSecondDir");
    diskProps.setDiskDirs(dirs);

    LocalRegion lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskProps, Scope.LOCAL);
    DiskRegion dr = lr.getDiskRegion();
    assertEquals(null, dr.getMyInitializingID());
    PersistentMemberID myId = dr.getMyPersistentID();
    assertNotNull(myId);
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertTrue(dr.getOfflineAndEqualMembers().isEmpty());

    // do recovery
    close(lr);
    assertEquals(true, (new File(dirs[0], "BACKUPtestIFFIleInSecondDir.if")).exists());
    // switch dir[]0 and dir[1]
    File[] myDirs = new File[2];
    myDirs[0] = dirs[1];
    myDirs[1] = dirs[0];
    diskProps.setDiskDirs(myDirs);
    assertEquals(true, (new File(myDirs[1], "BACKUPtestIFFIleInSecondDir.if")).exists());
    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    dr = lr.getDiskRegion();
    assertEquals(null, dr.getMyInitializingID());
    assertEquals(myId, dr.getMyPersistentID());
    assertTrue(dr.getOnlineMembers().isEmpty());
    assertTrue(dr.getOfflineMembers().isEmpty());
    assertTrue(dr.getOfflineAndEqualMembers().isEmpty());
  }

  /**
   * Make sure that disk store oplog files from another instance of a disk store are not allowed
   * when loading the other disk store
   */
  @Test
  public void testTwoDiskStores() {
    diskProps.setPersistBackup(true);
    diskProps.setRegionName("testTwoDiskStores");
    // create first instance in dir 0
    File[] myDirs = new File[1];
    myDirs[0] = dirs[0];
    diskProps.setDiskDirs(myDirs);

    LocalRegion lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,
        diskProps, Scope.LOCAL);
    lr.put("key1", "value1");
    close(lr);
    assertEquals(true, (new File(dirs[0], "BACKUPtestTwoDiskStores.if")).exists());
    assertEquals(true, (new File(dirs[0], "BACKUPtestTwoDiskStores_1.crf")).exists());
    assertEquals(true, (new File(dirs[0], "BACKUPtestTwoDiskStores_1.drf")).exists());

    // now create another instance in dir 1
    myDirs = new File[1];
    myDirs[0] = dirs[1];
    diskProps.setDiskDirs(myDirs);

    lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
        Scope.LOCAL);
    lr.put("key2", "value2");
    close(lr);
    assertEquals(true, (new File(dirs[1], "BACKUPtestTwoDiskStores.if")).exists());
    assertEquals(true, (new File(dirs[1], "BACKUPtestTwoDiskStores_1.crf")).exists());
    assertEquals(true, (new File(dirs[1], "BACKUPtestTwoDiskStores_1.drf")).exists());

    // remove the second if file; we want to make sure that duplicate oplogs will
    // cause recovery to fail
    (new File(dirs[1], "BACKUPtestTwoDiskStores.if")).delete();

    // now try to recover from with both dirs added. This should fail
    myDirs = new File[2];
    myDirs[0] = dirs[0];
    myDirs[1] = dirs[1];
    diskProps.setDiskDirs(myDirs);
    try {
      lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
          Scope.LOCAL);
      fail("expected DiskAccessException");
    } catch (org.apache.geode.cache.DiskAccessException expected) {
      if (!expected.getMessage().contains("two different directories")) {
        fail("did not expect: " + expected
            + ". It should have contained: \"two different directories\"");
      }
    }

    // now try to recover with just the wrong oplog files
    (new File(dirs[0], "BACKUPtestTwoDiskStores_1.crf")).delete();
    (new File(dirs[0], "BACKUPtestTwoDiskStores_1.drf")).delete();
    try {
      lr = (LocalRegion) DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache, diskProps,
          Scope.LOCAL);
      fail("expected DiskAccessException");
    } catch (org.apache.geode.cache.DiskAccessException expected) {
      if (!expected.getMessage().contains("does not belong")) {
        fail("did not expect: " + expected + ". It should have contained: \"does not belong\"");
      }
    }
  }

  @Test
  public void testPRConfig() throws Exception {
    DiskStoreImpl store = (DiskStoreImpl) cache.createDiskStoreFactory().create("testStore");

    PRPersistentConfig config = new PRPersistentConfig(5, "j");
    store.addPersistentPR("pr1", config);
    store.addPersistentPR("pr2", config);
    store.removePersistentPR("pr1");
    assertEquals(config, store.getPersistentPRConfig("pr2"));
    assertEquals(null, store.getPersistentPRConfig("pr1"));


    // recover the store
    cache.close();
    cache = createCache();
    store = (DiskStoreImpl) cache.createDiskStoreFactory().create("testStore");

    // Make sure the config is still the same.
    assertEquals(config, store.getPersistentPRConfig("pr2"));
    assertEquals(null, store.getPersistentPRConfig("pr1"));
    store.forceIFCompaction();

    assertEquals(config, store.getPersistentPRConfig("pr2"));
    assertEquals(null, store.getPersistentPRConfig("pr1"));

    // recover the store again
    cache.close();
    cache = createCache();
    store = (DiskStoreImpl) cache.createDiskStoreFactory().create("testStore");

    assertEquals(config, store.getPersistentPRConfig("pr2"));
    assertEquals(null, store.getPersistentPRConfig("pr1"));
  }

  private void close(LocalRegion lr) {
    lr.close();
    lr.getDiskStore().close();
    lr.getGemFireCache().removeDiskStore(lr.getDiskStore());
  }

  static long pmidCtr = 0;

  static PersistentMemberID createNewPMID() {
    return new PersistentMemberID(DiskStoreID.random(), null, null, ++pmidCtr, (short) 0);
  }
}
