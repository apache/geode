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
package org.apache.geode.internal.cache.backup;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;

public class BackupIntegrationTest {

  private static final String DISK_STORE_NAME = "diskStore";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private GemFireCacheImpl cache = null;
  private File incrementalDir;
  private File cacheXmlFile;

  private Properties props = new Properties();

  private File backupDir;
  private File[] diskDirs;
  private final Random random = new Random();

  private String getName() {
    return "BackupIntegrationTest_" + System.identityHashCode(this);
  }

  @Before
  public void setUp() throws Exception {
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    incrementalDir = temporaryFolder.newFolder("incremental");
    try {
      URL url = BackupIntegrationTest.class.getResource("BackupIntegrationTest.cache.xml");
      cacheXmlFile = new File(url.toURI().getPath());
    } catch (URISyntaxException e) {
      throw new ExceptionInInitializerError(e);
    }
    props.setProperty(CACHE_XML_FILE, cacheXmlFile.getAbsolutePath());
    props.setProperty(LOG_LEVEL, "config"); // to keep diskPerf logs smaller

    createCache();

    backupDir = temporaryFolder.newFolder("backup_Dir");
    backupDir.mkdir();
    diskDirs = new File[2];
    diskDirs[0] = temporaryFolder.newFolder("disk_Dir1");
    diskDirs[0].mkdir();
    diskDirs[1] = temporaryFolder.newFolder("disk_Dir2");
    diskDirs[1].mkdir();
  }

  private void createCache() {
    cache = (GemFireCacheImpl) new CacheFactory(props).create();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
    FileUtils.deleteDirectory(backupDir);
    FileUtils.deleteDirectory(diskDirs[0]);
    FileUtils.deleteDirectory(diskDirs[1]);
  }

  private void destroyDiskDirs() throws IOException {
    FileUtils.deleteDirectory(diskDirs[0]);
    FileUtils.deleteDirectory(diskDirs[1]);
  }

  @Test
  public void testBackupAndRecover() throws Exception {
    RegionCreator regionCreator = () -> {
      createDiskStore();
      return createRegion();
    };

    Region<Object, Object> region = regionCreator.createRegion();

    putDataAndRollOplogs(0, region);

    for (DiskStore store : cache.listDiskStoresIncludingRegionOwned()) {
      store.flush();
    }

    cache.close();
    createCache();
    region = regionCreator.createRegion();
    validateEntriesExist(region, 512, 2048);
    validateEntriesDoNotExist(region, 0, 512);

    BackupService backup = cache.getBackupService();
    BackupWriter writer = getBackupWriter();
    backup.prepareBackup(cache.getMyId(), writer);
    backup.doBackup();

    // Put another key to make sure we restore
    // from a backup that doesn't contain this key
    region.put("A", "A");

    cache.close();

    // Make sure the restore script refuses to run before we destroy the files.
    restoreBackup(backupDir, true);

    // Make sure the disk store is unaffected by the failed restore
    createCache();
    region = regionCreator.createRegion();
    validateEntriesExist(region, 512, 2048);
    validateEntriesDoNotExist(region, 0, 512);
    assertEquals("A", region.get("A"));

    region.put("B", "B");

    cache.close();
    // destroy the disk directories
    destroyDiskDirs();

    // Now the restore script should work
    restoreBackup(backupDir, false);

    // Make sure the cache has the restored backup
    createCache();
    region = regionCreator.createRegion();
    validateEntriesExist(region, 512, 2048);
    validateEntriesDoNotExist(region, 0, 512);

    assertNull(region.get("A"));
    assertNull(region.get("B"));
  }

  @Test
  public void testIncrementalBackupAndRecover() throws Exception {
    RegionCreator regionCreator = () -> {
      createDiskStore();
      return createRegion();
    };

    Region<Object, Object> region = regionCreator.createRegion();

    putDataAndRollOplogs(0, region);

    for (DiskStore store : cache.listDiskStoresIncludingRegionOwned()) {
      store.flush();
    }

    cache.close();
    createCache();
    region = regionCreator.createRegion();
    validateEntriesExist(region, 512, 2048);
    for (int i = 0; i < 512; i++) {
      assertNull(region.get(i));
    }

    BackupService backup = cache.getBackupService();
    BackupWriter baseWriter = getBackupWriter();
    backup.prepareBackup(cache.getMyId(), baseWriter);
    backup.doBackup();

    // Add more data for incremental to have something to backup
    putDataAndRollOplogs(1, region);

    Properties backupProperties =
        new BackupConfigFactory()
            .withBaselineDirPath(baseWriter.getBackupDirectory().getParent().toString())
            .withTargetDirPath(incrementalDir.toString()).createBackupProperties();
    BackupWriter incrementalWriter = BackupWriterFactory.FILE_SYSTEM.createWriter(backupProperties,
        cache.getMyId().toString());
    backup.prepareBackup(cache.getMyId(), incrementalWriter);
    backup.doBackup();

    // Put another key to make sure we restore
    // from a backup that doesn't contain this key
    region.put("A", "A");

    cache.close();

    // Make sure the restore script refuses to run before we destroy the files.
    restoreBackup(incrementalDir, true);

    // Make sure the disk store is unaffected by the failed restore
    createCache();
    region = regionCreator.createRegion();
    validateEntriesExist(region, 512, 2048);
    for (int i = 0; i < 512; i++) {
      assertNull(region.get(i));
    }
    assertEquals("A", region.get("A"));

    region.put("B", "B");

    cache.close();
    // destroy the disk directories
    destroyDiskDirs();

    // Now the restore script should work
    restoreBackup(incrementalDir, false);

    // Make sure the cache has the restored backup
    createCache();
    region = regionCreator.createRegion();
    validateEntriesExist(region, 512, 2048);
    validateEntriesExist(region, 2560, 4096);
    validateEntriesDoNotExist(region, 0, 512);
    validateEntriesDoNotExist(region, 2048, 2560);

    assertNull(region.get("A"));
    assertNull(region.get("B"));
  }

  private void putDataAndRollOplogs(int step, Region<Object, Object> region) {
    int startOne = step * 2048;
    int startTwo = startOne + 1024;

    // Put enough data to roll some oplogs
    for (int i = startOne; i < startOne + 1024; i++) {
      region.put(i, getBytes(i));
    }

    for (int i = startOne; i < startOne + 512; i++) {
      region.destroy(i);
    }

    for (int i = startTwo; i < startTwo + 1024; i++) {
      region.put(i, getBytes(i));
    }

    // This section of the test is for bug 43951
    findDiskStore().forceRoll();
    // add a put to the current crf
    region.put("junk", "value");
    // do a destroy of a key in a previous oplog
    region.destroy(startTwo + 1023);
    // do a destroy of the key in the current crf
    region.destroy("junk");
    // the current crf is now all garbage but
    // we need to keep the drf around since the older
    // oplog has a create that it deletes.
    findDiskStore().forceRoll();
    // restore the deleted entry.
    region.put(startTwo + 1023, getBytes(startTwo + 1023));
  }

  @Test
  public void testBackupEmptyDiskStore() throws Exception {
    createDiskStore();

    BackupService backup = cache.getBackupService();
    BackupWriter writer = getBackupWriter();
    backup.prepareBackup(cache.getMyId(), writer);
    backup.doBackup();
    assertEquals("No backup files should have been created", Collections.emptyList(),
        Arrays.asList(backupDir.list()));
  }

  private BackupWriter getBackupWriter() {
    Properties backupProperties =
        new BackupConfigFactory().withTargetDirPath(backupDir.toString()).createBackupProperties();
    return BackupWriterFactory.FILE_SYSTEM.createWriter(backupProperties,
        cache.getMyId().toString());
  }

  @Test
  public void testBackupOverflowOnlyDiskStore() throws Exception {
    createDiskStore();
    Region region = createOverflowRegion();
    // Put another key to make sure we restore
    // from a backup that doesn't contain this key
    region.put("A", "A");

    BackupService backup = cache.getBackupService();
    backup.prepareBackup(cache.getMyId(), getBackupWriter());
    backup.doBackup();


    assertEquals("No backup files should have been created", Collections.emptyList(),
        Arrays.asList(backupDir.list()));
  }

  @Test
  public void testCompactionDuringBackup() throws Exception {
    DiskStoreFactory dsf = cache.createDiskStoreFactory();
    dsf.setDiskDirs(diskDirs);
    dsf.setMaxOplogSize(1);
    dsf.setAutoCompact(false);
    dsf.setAllowForceCompaction(true);
    dsf.setCompactionThreshold(20);
    DiskStoreImpl ds = (DiskStoreImpl) dsf.create(DISK_STORE_NAME);

    Region<Object, Object> region = createRegion();

    // Put enough data to roll some oplogs
    for (int i = 0; i < 1024; i++) {
      region.put(i, getBytes(i));
    }

    BackupService backupService = cache.getBackupService();
    backupService.prepareBackup(cache.getMyId(), getBackupWriter());
    final Region theRegion = region;
    final DiskStore theDiskStore = ds;
    CompletableFuture.runAsync(() -> destroyAndCompact(theRegion, theDiskStore));
    backupService.doBackup();

    cache.close();
    destroyDiskDirs();
    restoreBackup(backupDir, false);
    createCache();
    createDiskStore();
    region = createRegion();
    validateEntriesExist(region, 0, 1024);

    assertNull(region.get("A"));
  }

  private void destroyAndCompact(Region<Object, Object> region, DiskStore diskStore) {
    for (int i = 2; i < 1024; i++) {
      assertTrue(region.destroy(i) != null);
    }
    assertTrue(diskStore.forceCompaction());
    // Put another key to make sure we restore
    // from a backup that doesn't contain this key
    region.put("A", "A");
  }

  @Test
  public void testBackupCacheXml() throws Exception {
    createDiskStore();
    createRegion();

    BackupService backupService = cache.getBackupService();
    backupService.prepareBackup(cache.getMyId(), getBackupWriter());
    backupService.doBackup();
    Collection<File> fileCollection = FileUtils.listFiles(backupDir,
        new RegexFileFilter("BackupIntegrationTest.cache.xml"), DirectoryFileFilter.DIRECTORY);
    assertEquals(1, fileCollection.size());
    File cacheXmlBackup = fileCollection.iterator().next();
    assertTrue(cacheXmlBackup.exists());
    byte[] expectedBytes = getBytes(cacheXmlFile);
    byte[] backupBytes = getBytes(cacheXmlBackup);
    assertEquals(expectedBytes.length, backupBytes.length);
    for (int i = 0; i < expectedBytes.length; i++) {
      assertEquals("byte " + i, expectedBytes[i], backupBytes[i]);
    }
  }

  private byte[] getBytes(File file) throws IOException {
    // The cache xml file should be small enough to fit in one byte array
    int size = (int) file.length();
    byte[] contents = new byte[size];
    try (FileInputStream fis = new FileInputStream(file)) {
      assertEquals(size, fis.read(contents));
      assertEquals(-1, fis.read());
    }
    return contents;
  }

  private void validateEntriesExist(Region region, int start, int end) {
    for (int i = start; i < end; i++) {
      byte[] bytes = (byte[]) region.get(i);
      byte[] expected = getBytes(i);
      assertTrue("Null entry " + i, bytes != null);
      assertEquals("Size mismatch on entry " + i, expected.length, bytes.length);
      for (int j = 0; j < expected.length; j++) {
        assertEquals("Byte wrong on entry " + i + ", byte " + j, expected[j], bytes[j]);
      }
    }
  }

  private void validateEntriesDoNotExist(Region region, int start, int end) {
    for (int i = start; i < end; i++) {
      assertFalse("Entry " + i + " exists", region.containsKey(i));
    }
  }

  private byte[] getBytes(int i) {
    byte[] data = new byte[1024];
    random.setSeed(i);
    random.nextBytes(data);
    return data;
  }

  private void restoreBackup(File backupDir, boolean expectFailure)
      throws IOException, InterruptedException {
    Collection<File> restoreScripts = FileUtils.listFiles(backupDir,
        new RegexFileFilter(".*restore.*"), DirectoryFileFilter.DIRECTORY);
    assertNotNull(restoreScripts);
    assertEquals("Restore scripts " + restoreScripts, 1, restoreScripts.size());
    for (File script : restoreScripts) {
      execute(script, expectFailure);
    }

  }

  private void execute(File script, boolean expectFailure)
      throws IOException, InterruptedException {
    List<String> command = new ArrayList<>();

    System.out.println("EXECUTING:" + script.getCanonicalPath());
    boolean isWindows = script.getName().endsWith("bat");
    if (isWindows) {
      command.add("cmd.exe");
      command.add("/c");
    }

    command.add(script.getCanonicalPath());
    ProcessBuilder pb = new ProcessBuilder(command);
    pb.redirectErrorStream(true);
    Process process = pb.start();

    InputStream is = process.getInputStream();
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    String line;
    while ((line = br.readLine()) != null) {
      System.out.println("OUTPUT:" + line);
      // TODO validate output
    }

    int result = process.waitFor();
    // On Windows XP, the process returns 0 even though we exit with a non-zero status.
    // So let's not bother asserting the return value on XP.
    if (!isWindows) {
      if (expectFailure) {
        assertEquals(1, result);
      } else {
        assertEquals(0, result);
      }
    }

  }

  private Region createRegion() {
    RegionFactory regionFactory = cache.createRegionFactory();
    regionFactory.setDiskStoreName(DISK_STORE_NAME);
    regionFactory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    return regionFactory.create("region");
  }

  private Region createOverflowRegion() {
    RegionFactory regionFactory = cache.createRegionFactory();
    regionFactory.setDiskStoreName(DISK_STORE_NAME);
    regionFactory.setEvictionAttributes(
        EvictionAttributes.createLIFOEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
    regionFactory.setDataPolicy(DataPolicy.NORMAL);
    return regionFactory.create("region");
  }

  private DiskStore findDiskStore() {
    return cache.findDiskStore(DISK_STORE_NAME);
  }

  private void createDiskStore() {
    DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
    diskStoreFactory.setDiskDirs(diskDirs);
    diskStoreFactory.setMaxOplogSize(1);
    diskStoreFactory.create(DISK_STORE_NAME);
  }

  private interface RegionCreator {
    Region<Object, Object> createRegion();
  }

}
