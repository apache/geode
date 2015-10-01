/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueStats;
import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogSetReader.HoplogIterator;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.SequenceFileHoplog;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogStatistics;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerHelper;

import dunit.AsyncInvocation;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;

@SuppressWarnings({"serial", "rawtypes", "unchecked"})
public abstract class RegionWithHDFSTestBase extends CacheTestCase {

  protected String tmpDir;

  public static String homeDir = null;

  protected abstract void checkWithGetAll(String uniqueName, ArrayList arrayl);

  protected abstract void checkWithGet(String uniqueName, int start,
      int end, boolean expectValue);

  protected abstract void doDestroys(final String uniqueName, int start, int end);

  protected abstract void doPutAll(final String uniqueName, Map map);

  protected abstract void doPuts(final String uniqueName, int start, int end);

  protected abstract SerializableCallable getCreateRegionCallable(final int totalnumOfBuckets, final int batchSizeMB,
      final int maximumEntries, final String folderPath, final String uniqueName, final int batchInterval, final boolean queuePersistent, 
      final boolean writeonly, final long timeForRollover, final long maxFileSize);
  
  protected abstract void verifyHDFSData(VM vm, String uniqueName) throws Exception ;
  
  protected abstract AsyncInvocation doAsyncPuts(VM vm, final String regionName, 
      final int start, final int end, final String suffix) throws Exception;
  
  public RegionWithHDFSTestBase(String name) {
    super(name);
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      SerializableCallable cleanUp = cleanUpStoresAndDisconnect();
      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        // This store will be deleted by the first VM itself. Invocations from
        // subsequent VMs will be no-op.
        vm.invoke(cleanUp);
      }
    }
  }

  public SerializableCallable cleanUpStoresAndDisconnect() throws Exception {
    SerializableCallable cleanUp = new SerializableCallable("cleanUpStoresAndDisconnect") {
      public Object call() throws Exception {
        disconnectFromDS();
        File file;
        if (homeDir != null) {
          file = new File(homeDir);
          FileUtil.delete(file);
          homeDir = null;
        }
        file = new File(tmpDir);
        FileUtil.delete(file);
        return 0;
      }
    };
    return cleanUp;
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    tmpDir = /*System.getProperty("java.io.tmpdir") + "/" +*/ "RegionWithHDFSBasicDUnitTest_" + System.nanoTime();
  }
  
  int createServerRegion(VM vm, final int totalnumOfBuckets, 
      final int batchSize, final int maximumEntries, final String folderPath, 
      final String uniqueName, final int batchInterval) {
    return createServerRegion(vm, totalnumOfBuckets, 
        batchSize, maximumEntries, folderPath, 
        uniqueName, batchInterval, false, false);
  }

  protected int createServerRegion(VM vm, final int totalnumOfBuckets, 
      final int batchSizeMB, final int maximumEntries, final String folderPath, 
      final String uniqueName, final int batchInterval, final boolean writeonly,
      final boolean queuePersistent) {
    return createServerRegion(vm, totalnumOfBuckets, 
        batchSizeMB, maximumEntries, folderPath, 
        uniqueName, batchInterval, writeonly, queuePersistent, -1, -1);
  }
  protected int createServerRegion(VM vm, final int totalnumOfBuckets, 
      final int batchSizeMB, final int maximumEntries, final String folderPath, 
      final String uniqueName, final int batchInterval, final boolean writeonly,
      final boolean queuePersistent, final long timeForRollover, final long maxFileSize) {
    SerializableCallable createRegion = getCreateRegionCallable(
        totalnumOfBuckets, batchSizeMB, maximumEntries, folderPath, uniqueName,
        batchInterval, queuePersistent, writeonly, timeForRollover, maxFileSize);

    return (Integer) vm.invoke(createRegion);
  }
  protected AsyncInvocation createServerRegionAsync(VM vm, final int totalnumOfBuckets, 
      final int batchSizeMB, final int maximumEntries, final String folderPath, 
      final String uniqueName, final int batchInterval, final boolean writeonly,
      final boolean queuePersistent) {
    SerializableCallable createRegion = getCreateRegionCallable(
        totalnumOfBuckets, batchSizeMB, maximumEntries, folderPath, uniqueName,
        batchInterval, queuePersistent, writeonly, -1, -1);

    return vm.invokeAsync(createRegion);
  }
  protected AsyncInvocation createServerRegionAsync(VM vm, final int totalnumOfBuckets, 
      final int batchSizeMB, final int maximumEntries, final String folderPath, 
      final String uniqueName, final int batchInterval, final boolean writeonly,
      final boolean queuePersistent, final long timeForRollover, final long maxFileSize) {
    SerializableCallable createRegion = getCreateRegionCallable(
        totalnumOfBuckets, batchSizeMB, maximumEntries, folderPath, uniqueName,
        batchInterval, queuePersistent, writeonly, timeForRollover, maxFileSize);

    return vm.invokeAsync(createRegion);
  }
  
  /**
   * Does puts, gets, destroy and getAll. Since there are many updates 
   * most of the time the data is not found in memory and queue and 
   * is fetched from HDFS
   * @throws Throwable 
   */
  public void testGetFromHDFS() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final String uniqueName = getName();
    final String homeDir = "../../testGetFromHDFS";
    
    createServerRegion(vm0, 7, 1, 50, homeDir, uniqueName, 50, false, true);
    createServerRegion(vm1, 7, 1, 50, homeDir, uniqueName, 50, false, true);
    
    // Do some puts
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        doPuts(uniqueName, 0, 40);
        return null;
      }
    });
    
    // Do some puts and destroys 
    // some order manipulation has been done because of an issue: 
    // " a higher version update on a key can be batched and 
    // sent to HDFS before a lower version update on the same key 
    // is batched and sent to HDFS. This will cause the latest 
    // update on a key in an older file. Hence, a fetch from HDFS 
    // will return an older update from a newer file."
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        doPuts(uniqueName, 40, 50);
        doDestroys(uniqueName, 40, 50);
        doPuts(uniqueName, 50, 100);
        doPuts(uniqueName, 30, 40);
        return null;
      }
    });
    
    // do some more puts and destroy 
    // some order manipulation has been done because of an issue: 
    // " a higher version update on a key can be batched and 
    // sent to HDFS before a lower version update on the same key 
    // is batched and sent to HDFS. This will cause the latest 
    // update on a key in an older file. Hence, a fetch from HDFS 
    // will return an older update from a newer file."
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        doPuts(uniqueName, 80, 90);
        doDestroys(uniqueName, 80, 90);
        doPuts(uniqueName, 110, 200);
        doPuts(uniqueName, 90, 110);
        return null;
      }
      
    });
    
    // get and getall the values and compare them. 
    SerializableCallable checkData = new SerializableCallable() {
      public Object call() throws Exception {
        checkWithGet(uniqueName, 0, 40, true);
        checkWithGet(uniqueName, 40, 50, false);
        checkWithGet(uniqueName, 50, 80, true);
        checkWithGet(uniqueName, 80, 90, false);
        checkWithGet(uniqueName, 90, 200, true);
        checkWithGet(uniqueName, 200, 201, false);
        
        ArrayList arrayl = new ArrayList();
        for (int i =0; i< 200; i++) {
          String k = "K" + i;
          if ( !((40 <= i && i < 50) ||   (80 <= i && i < 90)))
            arrayl.add(k);
        }
        checkWithGetAll(uniqueName, arrayl);
        
        return null;
      }
    };
    vm1.invoke(checkData);
    
    //Restart the members and verify that we can still get the data
    closeCache(vm0);
    closeCache(vm1);
    AsyncInvocation async0 = createServerRegionAsync(vm0, 7, 1, 50, homeDir, uniqueName, 50, false, true);
    AsyncInvocation async1 = createServerRegionAsync(vm1, 7, 1, 50, homeDir, uniqueName, 50, false, true);
    
    async0.getResult();
    async1.getResult();
    
    
    // get and getall the values and compare them.
    vm1.invoke(checkData);
  
    //TODO:HDFS we are just reading the files here. Need to verify 
    // once the folder structure is finalized. 
    dumpFiles(vm1, uniqueName);
    
  }

  /**
   * puts a few entries (keys with multiple updates ). Gets them immediately. 
   * High probability that it gets it from async queue. 
   */
  public void testGetForAsyncQueue() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    final String uniqueName = getName();
    final String homeDir = "../../testGetForAsyncQueue";
    
    createServerRegion(vm0, 2, 5, 1, homeDir, uniqueName, 10000);
    createServerRegion(vm1, 2, 5, 1, homeDir, uniqueName, 10000);
    
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        doPuts(uniqueName, 0, 4);
        return null;
      }
    });
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        doPuts(uniqueName, 0, 2);
        doDestroys(uniqueName, 2, 3);
        doPuts(uniqueName, 3, 7);
        
        checkWithGet(uniqueName, 0, 2, true);
        checkWithGet(uniqueName, 2, 3, false);
        checkWithGet(uniqueName, 3, 7, true);
        return null;
      }
    });
  }

  /**
   * puts a few entries (keys with multiple updates ). Calls getAll immediately. 
   * High probability that it gets it from async queue. 
   */
  public void testGetAllForAsyncQueue() {
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    final String uniqueName = getName();
    createServerRegion(vm0, 2, 5, 2, uniqueName, uniqueName, 10000);
    createServerRegion(vm1, 2, 5, 2, uniqueName, uniqueName, 10000);
    
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        doPuts(uniqueName, 0, 4);
        return null;
      }
    });
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        doPuts(uniqueName, 1, 5);
  
        ArrayList arrayl = new ArrayList();
        for (int i =0; i< 5; i++) {
          String k = "K" + i;
          arrayl.add(k);
        }
        checkWithGetAll(uniqueName, arrayl);
        return null;
      }
    });
  }

  /**
   * puts a few entries (keys with multiple updates ). Calls getAll immediately. 
   * High probability that it gets it from async queue. 
   */
  public void testPutAllForAsyncQueue() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    final String uniqueName = getName();
    final String homeDir = "../../testPutAllForAsyncQueue";
    createServerRegion(vm0, 2, 5, 2, homeDir, uniqueName, 10000);
    createServerRegion(vm1, 2, 5, 2, homeDir, uniqueName, 10000);
    
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        HashMap putAllmap = new HashMap();
        for (int i =0; i< 4; i++)
          putAllmap.put("K" + i, "V"+ i );
        doPutAll(uniqueName, putAllmap);
        return null;
      }
    });
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        HashMap putAllmap = new HashMap();
        for (int i =1; i< 5; i++)
          putAllmap.put("K" + i, "V"+ i );
        doPutAll(uniqueName, putAllmap);
        checkWithGet(uniqueName, 0, 5, true);
        return null;
      }
    });
  }

  /**
   * Does putAll and get. Since there are many updates 
   * most of the time the data is not found in memory and queue and 
   * is fetched from HDFS
   */
  public void _testPutAllAndGetFromHDFS() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    final String uniqueName = getName();
    final String homeDir = "../../testPutAllAndGetFromHDFS";
    createServerRegion(vm0, 7, 1, 500, homeDir, uniqueName, 500);
    createServerRegion(vm1, 7, 1, 500, homeDir, uniqueName, 500);
    
    // Do some puts
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
          
        HashMap putAllmap = new HashMap();
        
        for (int i =0; i< 500; i++)
          putAllmap.put("K" + i, "V"+ i );
        doPutAll(uniqueName, putAllmap);
        return null;
      }
    });
    
    // Do putAll and some  destroys 
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        HashMap putAllmap = new HashMap();
        for (int i = 500; i< 1000; i++)
          putAllmap.put("K" + i, "V"+ i );
        doPutAll(uniqueName, putAllmap);
        return null;
      }
    });
    
    // do some more puts 
    // some order manipulation has been done because of an issue: 
    // " a higher version update on a key can be batched and 
    // sent to HDFS before a lower version update on the same key 
    // is batched and sent to HDFS. This will cause the latest 
    // update on a key in an older file. Hence, a fetch from HDFS 
    // will return an older update from a newer file."
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        HashMap putAllmap = new HashMap();
        for (int i =1100; i< 2000; i++)
          putAllmap.put("K" + i, "V"+ i );
        doPutAll(uniqueName, putAllmap);
        putAllmap = new HashMap();
        for (int i = 900; i< 1100; i++)
          putAllmap.put("K" + i, "V"+ i );
        doPutAll(uniqueName, putAllmap);
        return null;
      }
      
    });
    
    // get and getall the values and compare them. 
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        checkWithGet(uniqueName, 0, 2000, true);
        checkWithGet(uniqueName, 2000,  2001, false);
        
        ArrayList arrayl = new ArrayList();
        for (int i =0; i< 2000; i++) {
          String k = "K" + i;
          arrayl.add(k);
        }
        checkWithGetAll(uniqueName, arrayl);
        return null;
      }
    });
    
  }

  public void _testWObasicClose() throws Throwable{
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    
    String homeDir = "../../testWObasicClose";
    final String uniqueName = getName();

    createServerRegion(vm0, 11, 1,  500, homeDir, uniqueName, 500, true, false);
    createServerRegion(vm1, 11, 1,  500, homeDir, uniqueName, 500, true, false);
    createServerRegion(vm2, 11, 1,  500, homeDir, uniqueName, 500, true, false);
    createServerRegion(vm3, 11, 1,  500, homeDir, uniqueName, 500, true, false);
    
    AsyncInvocation a1 = doAsyncPuts(vm0, uniqueName, 1, 50, "vm0");
    AsyncInvocation a2 = doAsyncPuts(vm1, uniqueName, 40, 100, "vm1");
    AsyncInvocation a3 = doAsyncPuts(vm2, uniqueName, 40, 100, "vm2");
    AsyncInvocation a4 = doAsyncPuts(vm3, uniqueName, 90, 150, "vm3");
    
    a1.join();
    a2.join();
    a3.join();
    a4.join();
   
    Thread.sleep(5000); 
    cacheClose (vm0, false);
    cacheClose (vm1, false);
    cacheClose (vm2, false);
    cacheClose (vm3, false);
    
    AsyncInvocation async1 = createServerRegionAsync(vm0, 11, 1,  500, homeDir, uniqueName, 500, true, false);
    AsyncInvocation async2 = createServerRegionAsync(vm1, 11, 1,  500, homeDir, uniqueName, 500, true, false);
    AsyncInvocation async3 = createServerRegionAsync(vm2, 11, 1,  500, homeDir, uniqueName, 500, true, false);
    AsyncInvocation async4 = createServerRegionAsync(vm3, 11, 1,  500, homeDir, uniqueName, 500, true, false);
    async1.getResult();
    async2.getResult();
    async3.getResult();
    async4.getResult();
    
    verifyHDFSData(vm0, uniqueName); 
    
    cacheClose (vm0, false);
    cacheClose (vm1, false);
    cacheClose (vm2, false);
    cacheClose (vm3, false);
  }
  
  
  protected void cacheClose(VM vm, final boolean sleep){
    vm.invoke( new SerializableCallable() {
      public Object call() throws Exception {
        if (sleep)
          Thread.sleep(2000);
        getCache().getLogger().info("Cache close in progress "); 
        getCache().close();
        getCache().getLogger().info("Cache closed");
        return null;
      }
    });
    
  }
  
  protected void verifyInEntriesMap (HashMap<String, String> entriesMap, int start, int end, String suffix) {
    for (int i =start; i< end; i++) {
      String k = "K" + i;
      String v = "V"+ i + suffix;
      Object s = entriesMap.get(v);
      assertTrue( "The expected key " + k+ " didn't match the received value " + s + ". value: " + v, k.equals(s));
    }
  }
  
  /**
   * Reads all the sequence files and returns the list of key value pairs persisted. 
   * Returns the key value pair as <value, key> tuple as there can be multiple values 
   * for a key
   * @throws Exception
   */
  protected HashMap<String, HashMap<String, String>>  createFilesAndEntriesMap(VM vm0, final String uniqueName, final String regionName) throws Exception {
    HashMap<String, HashMap<String, String>> entriesToFileMap = (HashMap<String, HashMap<String, String>>) 
    vm0.invoke( new SerializableCallable() {
      public Object call() throws Exception {
        HashMap<String, HashMap<String, String>> entriesToFileMap = new HashMap<String, HashMap<String, String>>();
        HDFSStoreImpl hdfsStore = (HDFSStoreImpl) ((GemFireCacheImpl)getCache()).findHDFSStore(uniqueName);
        FileSystem fs = hdfsStore.getFileSystem();
        System.err.println("dumping file names in HDFS directory: " + hdfsStore.getHomeDir());
        try {
          Path basePath = new Path(hdfsStore.getHomeDir());
          Path regionPath = new Path(basePath, regionName);
          RemoteIterator<LocatedFileStatus> files = fs.listFiles(regionPath, true);
          
          while(files.hasNext()) {
            HashMap<String, String> entriesMap = new HashMap<String, String>();
            LocatedFileStatus next = files.next();
            /* MergeGemXDHDFSToGFE - Disabled as I am not pulling in DunitEnv */
            // System.err.println(DUnitEnv.get().getPid() + " - " + next.getPath());
            System.err.println(" - " + next.getPath());
            readSequenceFile(fs, next.getPath(), entriesMap);
            entriesToFileMap.put(next.getPath().getName(), entriesMap);
          }
        } catch (FileNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        
        return entriesToFileMap;
      }
      @SuppressWarnings("deprecation")
      public void readSequenceFile(FileSystem inputFS, Path sequenceFileName,  
          HashMap<String, String> entriesMap) throws IOException {
        SequenceFileHoplog hoplog = new SequenceFileHoplog(inputFS, sequenceFileName, null);
        HoplogIterator<byte[], byte[]> iter = hoplog.getReader().scan();
        try {
          while (iter.hasNext()) {
            iter.next();
            PersistedEventImpl te = UnsortedHoplogPersistedEvent.fromBytes(iter.getValue());
            String stringkey = ((String)CacheServerHelper.deserialize(iter.getKey()));
            String value = (String) te.getDeserializedValue();
            entriesMap.put(value, stringkey);
            if (getCache().getLoggerI18n().fineEnabled())
              getCache().getLoggerI18n().fine("Key: " + stringkey + " value: " + value  + " path " + sequenceFileName.getName());
          }
        } catch (Exception e) {
          assertTrue(e.toString(), false);
        }
        iter.close();
        hoplog.close();
     }
    });
    return entriesToFileMap;
  }
 protected SerializableCallable validateEmpty(VM vm0, final int numEntries, final String uniqueName) {
    SerializableCallable validateEmpty = new SerializableCallable("validateEmpty") {
      public Object call() throws Exception {
        Region r = getRootRegion(uniqueName);
        
        assertTrue(r.isEmpty());
        
        //validate region is empty on peer as well
        assertFalse(r.entrySet().iterator().hasNext());
        //Make sure the region is empty
        for (int i =0; i< numEntries; i++) {
          assertEquals("failure on key K" + i , null, r.get("K" + i));
        }
        
        return null;
      }
    };
    
    vm0.invoke(validateEmpty);
    return validateEmpty;
  }

  protected void closeCache(VM vm0) {
    //Restart and validate still empty.
    SerializableRunnable closeCache = new SerializableRunnable("close cache") {
      @Override
      public void run() {
        getCache().close();
        disconnectFromDS();
      }
    };
    
    vm0.invoke(closeCache);
  }

  protected void verifyDataInHDFS(VM vm0, final String uniqueName, final boolean shouldHaveData,
      final boolean wait, final boolean waitForQueueToDrain, final int numEntries) {
        vm0.invoke(new SerializableCallable("check for data in hdfs") {
          @Override
          public Object call() throws Exception {
            
            HDFSRegionDirector director = HDFSRegionDirector.getInstance();
            final SortedOplogStatistics stats = director.getHdfsRegionStats("/" + uniqueName);
            waitForCriterion(new WaitCriterion() {
              @Override
              public boolean done() {
                return stats.getActiveFileCount() > 0 == shouldHaveData;
              }
              
              @Override
              public String description() {
                return "Waiting for active file count to be greater than 0: " + stats.getActiveFileCount() + " stats=" + System.identityHashCode(stats);
              }
            }, 30000, 100, true);
            
            if(waitForQueueToDrain) {
              PartitionedRegion region = (PartitionedRegion) getCache().getRegion(uniqueName);
              final AsyncEventQueueStats queueStats = region.getHDFSEventQueueStats();
              waitForCriterion(new WaitCriterion() {
                @Override
                public boolean done() {
                  return queueStats.getEventQueueSize() <= 0;
                }
                
                @Override
                public String description() {
                  return "Waiting for queue stats to reach 0: " + queueStats.getEventQueueSize();
                }
              }, 30000, 100, true);
            }
            return null;
          }
        });
      }

  protected void doPuts(VM vm0, final String uniqueName, final int numEntries) {
    // Do some puts
    vm0.invoke(new SerializableCallable("do puts") {
      public Object call() throws Exception {
        Region r = getRootRegion(uniqueName);
        for (int i =0; i< numEntries; i++)
          r.put("K" + i, "V"+ i );
        return null;
      }
    });
  }

  protected void validate(VM vm1, final String uniqueName, final int numEntries) {
    SerializableCallable validate = new SerializableCallable("validate") {
      public Object call() throws Exception {
        Region r = getRootRegion(uniqueName);
        
        for (int i =0; i< numEntries; i++) {
          assertEquals("failure on key K" + i , "V"+ i, r.get("K" + i));
        }
        
        return null;
      }
    };
    vm1.invoke(validate);
  }

  protected void dumpFiles(VM vm0, final String uniqueName) {
    vm0.invoke(new SerializableRunnable() {
  
      @Override
      public void run() {
        HDFSStoreImpl hdfsStore = (HDFSStoreImpl) ((GemFireCacheImpl)getCache()).findHDFSStore(uniqueName);
        FileSystem fs;
        try {
          fs = hdfsStore.getFileSystem();
        } catch (IOException e1) {
          throw new HDFSIOException(e1.getMessage(), e1);
        }
        System.err.println("dumping file names in HDFS directory: " + hdfsStore.getHomeDir());
        try {
          RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(hdfsStore.getHomeDir()), true);
          
          while(files.hasNext()) {
            LocatedFileStatus next = files.next();
            /* MergeGemXDHDFSToGFE - Disabled as I am not pulling in DunitEnv */
            // System.err.println(DUnitEnv.get().getPid() + " - " + next.getPath());
            System.err.println(" - " + next.getPath());
          }
        } catch (FileNotFoundException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        
      }
      
    });
  }

}
