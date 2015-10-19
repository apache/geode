/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.internal.cache.LocalRegion;

import dunit.AsyncInvocation;
import dunit.SerializableCallable;
import dunit.VM;

/**
 * A class for testing the basic HDFS functionality
 * 
 * @author Hemant Bhanawat
 */
@SuppressWarnings({"serial", "rawtypes", "unchecked", "deprecation"})
public class ColocatedRegionWithHDFSDUnitTest extends RegionWithHDFSTestBase {

  public ColocatedRegionWithHDFSDUnitTest(String name) {
    super(name);
  }

  @Override
  protected SerializableCallable getCreateRegionCallable(
      final int totalnumOfBuckets, final int batchSizeMB,
      final int maximumEntries, final String folderPath,
      final String uniqueName, final int batchInterval,
      final boolean queuePersistent, final boolean writeonly,
      final long timeForRollover, final long maxFileSize) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        HDFSStoreFactory hsf = getCache().createHDFSStoreFactory();
        hsf.setBatchSize(batchSizeMB);
        hsf.setBufferPersistent(queuePersistent);
        hsf.setMaxMemory(3);
        hsf.setBatchInterval(batchInterval);
        hsf.setHomeDir(tmpDir + "/" + folderPath);
        homeDir = new File(tmpDir + "/" + folderPath).getCanonicalPath();
        hsf.setHomeDir(homeDir);
        hsf.create(uniqueName);

        AttributesFactory af = new AttributesFactory();
        af.setDataPolicy(DataPolicy.PARTITION);
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setTotalNumBuckets(totalnumOfBuckets);
        paf.setRedundantCopies(1);

        af.setHDFSStoreName(uniqueName);
        af.setPartitionAttributes(paf.create());
        af.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
            maximumEntries, EvictionAction.LOCAL_DESTROY));

        af.setHDFSWriteOnly(writeonly);
        Region r1 = createRootRegion(uniqueName + "-r1", af.create());

        paf.setColocatedWith(uniqueName + "-r1");
        af.setPartitionAttributes(paf.create());
        af.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
            maximumEntries, EvictionAction.LOCAL_DESTROY));
        Region r2 = createRootRegion(uniqueName + "-r2", af.create());

        ((LocalRegion) r1).setIsTest();
        ((LocalRegion) r2).setIsTest();

        return 0;
      }
    };
    return createRegion;
  }

  @Override
  protected void doPuts(String uniqueName, int start, int end) {
    Region r1 = getRootRegion(uniqueName + "-r1");
    Region r2 = getRootRegion(uniqueName + "-r2");

    for (int i = start; i < end; i++) {
      r1.put("K" + i, "V" + i);
      r2.put("K" + i, "V" + i);
    }
  }

  protected AsyncInvocation doAsyncPuts(VM vm, final String regionName,
      final int start, final int end, final String suffix) throws Exception {
    return vm.invokeAsync(new SerializableCallable() {
      public Object call() throws Exception {
        Region r1 = getRootRegion(regionName + "-r1");
        Region r2 = getRootRegion(regionName + "-r2");

        getCache().getLogger().info("Putting entries ");
        for (int i = start; i < end; i++) {
          r1.put("K" + i, "V" + i + suffix);
          r2.put("K" + i, "V" + i + suffix);
        }
        return null;
      }

    });
  }

  protected void doPutAll(final String uniqueName, Map map) {
    Region r1 = getRootRegion(uniqueName + "-r1");
    Region r2 = getRootRegion(uniqueName + "-r2");
    r1.putAll(map);
    r2.putAll(map);
  }

  @Override
  protected void doDestroys(String uniqueName, int start, int end) {
    Region r1 = getRootRegion(uniqueName + "-r1");
    Region r2 = getRootRegion(uniqueName + "-r2");

    for (int i = start; i < end; i++) {
      r1.destroy("K" + i);
      r2.destroy("K" + i);
    }
  }

  @Override
  protected void checkWithGet(String uniqueName, int start, int end,
      boolean expectValue) {
    Region r1 = getRootRegion(uniqueName + "-r1");
    Region r2 = getRootRegion(uniqueName + "-r2");
    for (int i = start; i < end; i++) {
      String expected = expectValue ? "V" + i : null;
      assertEquals("Mismatch on key " + i, expected, r1.get("K" + i));
      assertEquals("Mismatch on key " + i, expected, r2.get("K" + i));
    }
  }

  protected void checkWithGetAll(String uniqueName, ArrayList arrayl) {
    Region r1 = getRootRegion(uniqueName + "-r1");
    Region r2 = getRootRegion(uniqueName + "-r2");
    Map map1 = r1.getAll(arrayl);
    Map map2 = r2.getAll(arrayl);
    for (Object e : map1.keySet()) {
      String v = e.toString().replaceFirst("K", "V");
      assertTrue("Reading entries failed for key " + e + " where value = "
          + map1.get(e), v.equals(map1.get(e)));
      assertTrue("Reading entries failed for key " + e + " where value = "
          + map2.get(e), v.equals(map2.get(e)));
    }
  }

  @Override
  protected void verifyHDFSData(VM vm, String uniqueName) throws Exception {
    HashMap<String, HashMap<String, String>> filesToEntriesMap = createFilesAndEntriesMap(
        vm, uniqueName, uniqueName + "-r1");
    HashMap<String, String> entriesMap = new HashMap<String, String>();
    for (Map.Entry<String, HashMap<String, String>> e : filesToEntriesMap
        .entrySet()) {
      entriesMap.putAll(e.getValue());
    }

    verifyInEntriesMap(entriesMap, 1, 50, "vm0");
    verifyInEntriesMap(entriesMap, 40, 100, "vm1");
    verifyInEntriesMap(entriesMap, 40, 100, "vm2");
    verifyInEntriesMap(entriesMap, 90, 150, "vm3");

    filesToEntriesMap = createFilesAndEntriesMap(vm, uniqueName, uniqueName
        + "-r2");
    entriesMap = new HashMap<String, String>();
    for (Map.Entry<String, HashMap<String, String>> e : filesToEntriesMap
        .entrySet()) {
      entriesMap.putAll(e.getValue());
    }

    verifyInEntriesMap(entriesMap, 1, 50, "vm0");
    verifyInEntriesMap(entriesMap, 40, 100, "vm1");
    verifyInEntriesMap(entriesMap, 40, 100, "vm2");
    verifyInEntriesMap(entriesMap, 90, 150, "vm3");
  }
}
