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

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.cache.persistence.BackupManager;
import com.gemstone.gemfire.internal.cache.persistence.RestoreScript;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

/**
 *
 */
@Category(IntegrationTest.class)
public class BackupJUnitTest {
  protected static GemFireCacheImpl cache = null;
  protected static File TMP_DIR;
  protected static File cacheXmlFile;

  protected static DistributedSystem ds = null;
  protected static Properties props = new Properties();

  static {
  }

  private File backupDir;
  private File[] diskDirs;
  private final Random random = new Random();
  
  private String getName() {
    return "BackupJUnitTest_"+System.identityHashCode(this);
  }

  @Before
  public void setUp() throws Exception {
    if (TMP_DIR == null) {
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, "");
      String tmpDirName = System.getProperty("java.io.tmpdir");
      TMP_DIR = tmpDirName == null ? new File("") : new File(tmpDirName); 
      try {
        URL url = BackupJUnitTest.class.getResource("BackupJUnitTest.cache.xml");
        cacheXmlFile = new File(url.toURI().getPath());
      } catch (URISyntaxException e) {
        throw new ExceptionInInitializerError(e);
      }
      props.setProperty(CACHE_XML_FILE, cacheXmlFile.getAbsolutePath());
      props.setProperty(LOG_LEVEL, "config"); // to keep diskPerf logs smaller
    }

    createCache();
    
    backupDir = new File(TMP_DIR, getName() + "backup_Dir");
    backupDir.mkdir();
    diskDirs = new File[2];
    diskDirs[0] = new File(TMP_DIR, getName() + "_diskDir1");
    diskDirs[0].mkdir();
    diskDirs[1] = new File(TMP_DIR, getName() + "_diskDir2");
    diskDirs[1].mkdir();
  }

  private void createCache() throws IOException {
    cache = (GemFireCacheImpl) new CacheFactory(props).create();
    ds = cache.getDistributedSystem();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
    FileUtil.delete(backupDir);
    FileUtil.delete(diskDirs[0]);
    FileUtil.delete(diskDirs[1]);
  }

  private void destroyDiskDirs() throws IOException {
    FileUtil.delete(diskDirs[0]);
    diskDirs[0].mkdir();
    FileUtil.delete(diskDirs[1]);
    diskDirs[1].mkdir();
  }
  
  @Test
  public void testBackupAndRecover() throws IOException, InterruptedException {
    backupAndRecover(new RegionCreator() {
      public Region createRegion() {
        DiskStoreImpl ds = createDiskStore();
        Region region = BackupJUnitTest.this.createRegion(); 
        return region;        
      }
    });
  }
  
  @Test
  public void testBackupAndRecoverOldConfig() throws IOException, InterruptedException {
    backupAndRecover(new RegionCreator() {
      public Region createRegion() {
        DiskStoreImpl ds = createDiskStore();
        RegionFactory rf = new RegionFactory();
        rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        rf.setDiskDirs(diskDirs);
        DiskWriteAttributesFactory daf = new DiskWriteAttributesFactory();
        daf.setMaxOplogSize(1);
        rf.setDiskWriteAttributes(daf.create());
        Region region = rf.create("region");
        return region;        
      }
    });
  }
  
  public void backupAndRecover(RegionCreator regionFactory) throws IOException, InterruptedException {
    Region region = regionFactory.createRegion();

    //Put enough data to roll some oplogs
    for(int i =0; i < 1024; i++) {
      region.put(i, getBytes(i));
    }
    
    for(int i =0; i < 512; i++) {
      region.destroy(i);
    }
    
    for(int i =1024; i < 2048; i++) {
      region.put(i, getBytes(i));
    }
    
    // This section of the test is for bug 43951
    findDiskStore().forceRoll();
    // add a put to the current crf
    region.put("junk", "value");
    // do a destroy of a key in a previous oplog
    region.destroy(2047);
    // do a destroy of the key in the current crf
    region.destroy("junk");
    // the current crf is now all garbage but
    // we need to keep the drf around since the older
    // oplog has a create that it deletes.
    findDiskStore().forceRoll();
    // restore the deleted entry.
    region.put(2047, getBytes(2047));

    for(DiskStoreImpl store : cache.listDiskStoresIncludingRegionOwned()) {
      store.flush();
    }

    cache.close();
    createCache();
    region = regionFactory.createRegion();
    validateEntriesExist(region, 512, 2048);
    for(int i =0; i < 512; i++) {
      assertNull(region.get(i));
    }
    
    BackupManager backup = cache.startBackup(cache.getDistributedSystem().getDistributedMember());
    backup.prepareBackup();
    backup.finishBackup(backupDir, null, false);
    
    //Put another key to make sure we restore
    //from a backup that doesn't contain this key
    region.put("A", "A");
    
    cache.close();
    
    //Make sure the restore script refuses to run before we destroy the files. 
    restoreBackup(true);

    //Make sure the disk store is unaffected by the failed restore
    createCache();
    region = regionFactory.createRegion();
    validateEntriesExist(region, 512, 2048);
    for(int i =0; i < 512; i++) {
      assertNull(region.get(i));
    }
    assertEquals("A", region.get("A"));
    
    region.put("B", "B");
    
    cache.close();
    //destroy the disk directories
    destroyDiskDirs();
    
    //Now the restore script should work 
    restoreBackup(false);
    
    //Make sure the cache has the restored backup
    createCache();
    region = regionFactory.createRegion();
    validateEntriesExist(region, 512, 2048);
    for(int i =0; i < 512; i++) {
      assertNull(region.get(i));
    }
    
    assertNull(region.get("A"));
    assertNull(region.get("B"));
  }
  
  
  @Test
  public void testBackupEmptyDiskStore() throws IOException, InterruptedException {
    DiskStoreImpl ds = createDiskStore();

    BackupManager backup = cache.startBackup(cache.getDistributedSystem().getDistributedMember());
    backup.prepareBackup();
    backup.finishBackup(backupDir, null, false);
    assertEquals("No backup files should have been created", Collections.emptyList(), Arrays.asList(backupDir.list()));
  }
  
  @Test
  public void testBackupOverflowOnlyDiskStore() throws IOException, InterruptedException {
    DiskStoreImpl ds = createDiskStore();
    Region region = createOverflowRegion();
    //Put another key to make sure we restore
    //from a backup that doesn't contain this key
    region.put("A", "A");

    BackupManager backup = cache.startBackup(cache.getDistributedSystem().getDistributedMember());
    backup.prepareBackup();
    backup.finishBackup(backupDir, null, false);
    
    
    assertEquals("No backup files should have been created", Collections.emptyList(), Arrays.asList(backupDir.list()));
  }

  
  @Test
  public void testCompactionDuringBackup() throws IOException, InterruptedException {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setDiskDirs(diskDirs);
    dsf.setMaxOplogSize(1);
    dsf.setAutoCompact(false);
    dsf.setAllowForceCompaction(true);
    dsf.setCompactionThreshold(20);
    String name = "diskStore";
    DiskStoreImpl ds = (DiskStoreImpl) dsf.create(name);
    
    Region region = createRegion();

    //Put enough data to roll some oplogs
    for(int i =0; i < 1024; i++) {
      region.put(i, getBytes(i));
    }
    
    RestoreScript script= new RestoreScript();
    ds.startBackup(backupDir, null, script);
    
    for(int i =2; i < 1024; i++) {
      assertTrue(region.destroy(i) != null);
    }
    assertTrue(ds.forceCompaction());
    //Put another key to make sure we restore
    //from a backup that doesn't contain this key
    region.put("A", "A");
    
    ds.finishBackup(new BackupManager(cache.getDistributedSystem().getDistributedMember(), cache));
    script.generate(backupDir);
    
    cache.close();
    destroyDiskDirs();
    restoreBackup(false);
    createCache();
    ds = createDiskStore();
    region = createRegion();
    validateEntriesExist(region, 0, 1024);
    
    assertNull(region.get("A"));
  }
  
  @Test
  public void testBackupCacheXml() throws Exception {
    DiskStoreImpl ds = createDiskStore();
    createRegion();

    BackupManager backup = cache.startBackup(cache.getDistributedSystem().getDistributedMember());
    backup.prepareBackup();
    backup.finishBackup(backupDir, null, false);
    File cacheXmlBackup = FileUtil.find(backupDir, ".*config.cache.xml");
    assertTrue(cacheXmlBackup.exists());
    byte[] expectedBytes = getBytes(cacheXmlFile);
    byte[] backupBytes = getBytes(cacheXmlBackup);
    assertEquals(expectedBytes.length, backupBytes.length);
    for(int i = 0; i < expectedBytes.length; i++) {
      assertEquals("byte "+ i, expectedBytes[i], backupBytes[i]);
    }
  }
  
  private byte[] getBytes(File file) throws IOException {
    //The cache xml file should be small enough to fit in one byte array
    int size = (int) file.length();
    byte[] contents = new byte[size];
    FileInputStream fis = new FileInputStream(file);
    try {
      assertEquals(size, fis.read(contents));
      assertEquals(-1, fis.read());
    } finally {
      fis.close();
    }
    return contents;
  }

  private void validateEntriesExist(Region region, int start, int end) {
    for(int i =start; i < end; i++) {
      byte[] bytes = (byte[]) region.get(i);
      byte[] expected = getBytes(i);
      assertTrue("Null entry " + i, bytes != null);
      assertEquals("Size mismatch on entry " + i, expected.length, bytes.length);
      for(int j = 0; j < expected.length; j++) {
        assertEquals("Byte wrong on entry " + i + ", byte " + j, expected[j], bytes[j]);
      }
      
    }
  }

  private byte[] getBytes(int i) {
    byte[] data = new byte[1024];
    random.setSeed(i);
    random.nextBytes(data);
    return data;
  }

  private void restoreBackup(boolean expectFailure) throws IOException, InterruptedException {
    List<File> restoreScripts = FileUtil.findAll(backupDir, ".*restore.*");
    assertEquals("Restore scripts " + restoreScripts, 1, restoreScripts.size());
    for(File script : restoreScripts) {
      execute(script, expectFailure);
    }
    
  }

  private void execute(File script, boolean expectFailure) throws IOException, InterruptedException {
    ProcessBuilder pb = new ProcessBuilder(script.getAbsolutePath());
    pb.redirectErrorStream(true);
    Process process = pb.start();
    
    InputStream is = process.getInputStream();
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    String line;
    while((line = br.readLine()) != null) {
      System.out.println("OUTPUT:" + line);
      //TODO validate output
    };
    
    int result = process.waitFor();
    boolean isWindows = script.getName().endsWith("bat");
    //On Windows XP, the process returns 0 even though we exit with a non-zero status.
    //So let's not bother asserting the return value on XP.
    if(!isWindows) {
      if(expectFailure) {
        assertEquals(1, result);
      } else {
        assertEquals(0, result);
      }
    }
    
  }

  protected Region createRegion() {
    RegionFactory rf = new RegionFactory();
    rf.setDiskStoreName("diskStore");
    rf.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    Region region = rf.create("region");
    return region;
  }
  
  private Region createOverflowRegion() {
    RegionFactory rf = new RegionFactory();
    rf.setDiskStoreName("diskStore");
    rf.setEvictionAttributes(EvictionAttributes.createLIFOEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
    rf.setDataPolicy(DataPolicy.NORMAL);
    Region region = rf.create("region");
    return region;
  }

  private DiskStore findDiskStore() {
    return this.cache.findDiskStore("diskStore");
  }
  private DiskStoreImpl createDiskStore() {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setDiskDirs(diskDirs);
    dsf.setMaxOplogSize(1);
    String name = "diskStore";
    DiskStoreImpl ds = (DiskStoreImpl) dsf.create(name);
    return ds;
  }
  
  private static interface RegionCreator {
    public Region createRegion();
  }

}
