 /*=========================================================================
   * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
   * This product is protected by U.S. and international copyright
   * and intellectual property laws. Pivotal products are covered by
   * one or more patents listed at http://www.pivotal.io/patents.
   *=========================================================================
   */

package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheXmlException;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.AbstractHoplogOrganizer;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogConfig;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.control.HeapMemoryMonitor;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.junit.experimental.categories.Category;

/**
 * A test class for testing the configuration option for HDFS 
 * 
 * @author Hemant Bhanawat
 * @author Ashvin Agrawal
 */
@Category({IntegrationTest.class, HoplogTest.class})
public class HDFSConfigJUnitTest extends TestCase {
  private GemFireCacheImpl c;

  public HDFSConfigJUnitTest() {
    super();
  }

  @Override
  public void setUp() {
    System.setProperty(HoplogConfig.ALLOW_LOCAL_HDFS_PROP, "true");
    this.c = createCache();
    AbstractHoplogOrganizer.JUNIT_TEST_RUN = true;
  }

  @Override
  public void tearDown() {
    this.c.close();
  }
    
    public void testHDFSStoreCreation() throws Exception {
      this.c.close();
      this.c = createCache();
      try {
        HDFSStoreFactory hsf = this.c.createHDFSStoreFactory();
        HDFSStore store = hsf.create("myHDFSStore");
        RegionFactory rf1 = this.c.createRegionFactory(RegionShortcut.PARTITION_HDFS);
        Region r1 = rf1.setHDFSStoreName("myHDFSStore").create("r1");
       
        r1.put("k1", "v1");
        
        assertTrue("Mismatch in attributes, actual.batchsize: " + store.getBatchSize() + " and expected batchsize: 32", store.getBatchSize()== 32);
        assertTrue("Mismatch in attributes, actual.isPersistent: " + store.getBufferPersistent() + " and expected isPersistent: false", store.getBufferPersistent()== false);
        assertEquals(false, r1.getAttributes().getHDFSWriteOnly());
        assertTrue("Mismatch in attributes, actual.getDiskStoreName: " + store.getDiskStoreName() + " and expected getDiskStoreName: null", store.getDiskStoreName()== null);
        assertTrue("Mismatch in attributes, actual.getFileRolloverInterval: " + store.getWriteOnlyFileRolloverInterval() + " and expected getFileRolloverInterval: 3600", store.getWriteOnlyFileRolloverInterval() == 3600);
        assertTrue("Mismatch in attributes, actual.getMaxFileSize: " + store.getWriteOnlyFileSizeLimit() + " and expected getMaxFileSize: 256MB", store.getWriteOnlyFileSizeLimit() == 256);
        this.c.close();
        
        
        this.c = createCache();
        hsf = this.c.createHDFSStoreFactory();
        hsf.create("myHDFSStore");
        
        r1 = this.c.createRegionFactory(RegionShortcut.PARTITION_WRITEONLY_HDFS_STORE).setHDFSStoreName("myHDFSStore")
              .create("r1");
       
        r1.put("k1", "v1");
        assertTrue("Mismatch in attributes, actual.batchsize: " + store.getBatchSize() + " and expected batchsize: 32", store.getBatchSize()== 32);
        assertTrue("Mismatch in attributes, actual.isPersistent: " + store.getBufferPersistent() + " and expected isPersistent: false", store.getBufferPersistent()== false);
        assertTrue("Mismatch in attributes, actual.isRandomAccessAllowed: " + r1.getAttributes().getHDFSWriteOnly() + " and expected isRandomAccessAllowed: true", r1.getAttributes().getHDFSWriteOnly()== true);
        assertTrue("Mismatch in attributes, actual.getDiskStoreName: " + store.getDiskStoreName() + " and expected getDiskStoreName: null", store.getDiskStoreName()== null);
        assertTrue("Mismatch in attributes, actual.batchInterval: " + store.getBatchInterval() + " and expected batchsize: 60000", store.getBatchInterval()== 60000);
        assertTrue("Mismatch in attributes, actual.isDiskSynchronous: " + store.getSynchronousDiskWrite() + " and expected isDiskSynchronous: true", store.getSynchronousDiskWrite()== true);
        
        this.c.close();

        this.c = createCache();
        
        File directory = new File("HDFS" + "_disk_"
            + System.currentTimeMillis());
        directory.mkdir();
        File[] dirs1 = new File[] { directory };
        DiskStoreFactory dsf = this.c.createDiskStoreFactory();
        dsf.setDiskDirs(dirs1);
        dsf.create("mydisk");
        
        
        hsf = this.c.createHDFSStoreFactory();
        hsf.setBatchSize(50);
        hsf.setDiskStoreName("mydisk");
        hsf.setBufferPersistent(true);
        hsf.setBatchInterval(50);
        hsf.setSynchronousDiskWrite(false);
        hsf.setHomeDir("/home/hemant");
        hsf.setNameNodeURL("mymachine");
        hsf.setWriteOnlyFileSizeLimit(1);
        hsf.setWriteOnlyFileRolloverInterval(10);
        hsf.create("myHDFSStore");
        
        
        r1 = this.c.createRegionFactory(RegionShortcut.PARTITION_WRITEONLY_HDFS_STORE).setHDFSStoreName("myHDFSStore")
            .setHDFSWriteOnly(true).create("r1");
       
        r1.put("k1", "v1");
        store = c.findHDFSStore(r1.getAttributes().getHDFSStoreName());
        
        assertTrue("Mismatch in attributes, actual.batchsize: " + store.getBatchSize() + " and expected batchsize: 50", store.getBatchSize()== 50);
        assertTrue("Mismatch in attributes, actual.isPersistent: " + store.getBufferPersistent() + " and expected isPersistent: true", store.getBufferPersistent()== true);
        assertTrue("Mismatch in attributes, actual.isRandomAccessAllowed: " + r1.getAttributes().getHDFSWriteOnly() + " and expected isRandomAccessAllowed: true", r1.getAttributes().getHDFSWriteOnly()== true);
        assertTrue("Mismatch in attributes, actual.getDiskStoreName: " + store.getDiskStoreName() + " and expected getDiskStoreName: mydisk", store.getDiskStoreName()== "mydisk");
        assertTrue("Mismatch in attributes, actual.HDFSStoreName: " + r1.getAttributes().getHDFSStoreName() + " and expected getDiskStoreName: myHDFSStore", r1.getAttributes().getHDFSStoreName()== "myHDFSStore");
        assertTrue("Mismatch in attributes, actual.getFolderPath: " + ((GemFireCacheImpl)this.c).findHDFSStore("myHDFSStore").getHomeDir() + " and expected getDiskStoreName: /home/hemant", ((GemFireCacheImpl)this.c).findHDFSStore("myHDFSStore").getHomeDir()== "/home/hemant");
        assertTrue("Mismatch in attributes, actual.getNamenode: " + ((GemFireCacheImpl)this.c).findHDFSStore("myHDFSStore").getNameNodeURL()+ " and expected getDiskStoreName: mymachine", ((GemFireCacheImpl)this.c).findHDFSStore("myHDFSStore").getNameNodeURL()== "mymachine");
        assertTrue("Mismatch in attributes, actual.batchInterval: " + store.getBatchInterval() + " and expected batchsize: 50 ", store.getBatchSize()== 50);
        assertTrue("Mismatch in attributes, actual.isDiskSynchronous: " + store.getSynchronousDiskWrite() + " and expected isPersistent: false", store.getSynchronousDiskWrite()== false);
        assertTrue("Mismatch in attributes, actual.getFileRolloverInterval: " + store.getWriteOnlyFileRolloverInterval() + " and expected getFileRolloverInterval: 10", store.getWriteOnlyFileRolloverInterval() == 10);
        assertTrue("Mismatch in attributes, actual.getMaxFileSize: " + store.getWriteOnlyFileSizeLimit() + " and expected getMaxFileSize: 1MB", store.getWriteOnlyFileSizeLimit() == 1);
        this.c.close();
      } finally {
        this.c.close();
      }
    }
       
    public void testCacheXMLParsing() throws Exception {
      try {
        this.c.close();

        Region r1 = null;

        // use a cache.xml to recover
        this.c = createCache();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(baos), true); 
        pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
//      pw.println("<?xml version=\"1.0\"?>");
//      pw.println("<!DOCTYPE cache PUBLIC");
//      pw.println("  \"-//GemStone Systems, Inc.//GemFire Declarative Caching 7.5//EN\"");
//      pw.println("  \"http://www.gemstone.com/dtd/cache7_5.dtd\">");
        pw.println("<cache ");
        pw.println("xmlns=\"http://schema.pivotal.io/gemfire/cache\"");
        pw.println("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
        pw.println(" xsi:schemaLocation=\"http://schema.pivotal.io/gemfire/cache http://schema.pivotal.io/gemfire/cache/cache-9.0.xsd\"");
        pw.println("version=\"9.0\">");

        pw.println("  <hdfs-store name=\"myHDFSStore\" namenode-url=\"mynamenode\"  home-dir=\"mypath\" />");
        pw.println("  <region name=\"r1\" refid=\"PARTITION_HDFS\">");
        pw.println("    <region-attributes hdfs-store-name=\"myHDFSStore\"/>");
        pw.println("  </region>");
        pw.println("</cache>");
        pw.close();
        byte[] bytes = baos.toByteArray();  
        this.c.loadCacheXml(new ByteArrayInputStream(bytes));
        
        r1 = this.c.getRegion("/r1");
        HDFSStoreImpl store = c.findHDFSStore(r1.getAttributes().getHDFSStoreName());
        r1.put("k1", "v1");
        assertTrue("Mismatch in attributes, actual.batchsize: " + store.getBatchSize() + " and expected batchsize: 32", store.getBatchSize()== 32);
        assertTrue("Mismatch in attributes, actual.isPersistent: " + store.getBufferPersistent() + " and expected isPersistent: false", store.getBufferPersistent()== false);
        assertEquals(false, r1.getAttributes().getHDFSWriteOnly());
        assertTrue("Mismatch in attributes, actual.getDiskStoreName: " + store.getDiskStoreName() + " and expected getDiskStoreName: null", store.getDiskStoreName()== null);
        assertTrue("Mismatch in attributes, actual.getFileRolloverInterval: " + store.getWriteOnlyFileRolloverInterval() + " and expected getFileRolloverInterval: 3600", store.getWriteOnlyFileRolloverInterval() == 3600);
        assertTrue("Mismatch in attributes, actual.getMaxFileSize: " + store.getWriteOnlyFileSizeLimit() + " and expected getMaxFileSize: 256MB", store.getWriteOnlyFileSizeLimit() == 256);
        
        this.c.close();
        
        // use a cache.xml to recover
        this.c = createCache();
        baos = new ByteArrayOutputStream();
        pw = new PrintWriter(new OutputStreamWriter(baos), true);
        pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
//      pw.println("<?xml version=\"1.0\"?>");
//      pw.println("<!DOCTYPE cache PUBLIC");
//      pw.println("  \"-//GemStone Systems, Inc.//GemFire Declarative Caching 7.5//EN\"");
//      pw.println("  \"http://www.gemstone.com/dtd/cache7_5.dtd\">");
        pw.println("<cache ");
        pw.println("xmlns=\"http://schema.pivotal.io/gemfire/cache\"");
        pw.println("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
        pw.println(" xsi:schemaLocation=\"http://schema.pivotal.io/gemfire/cache http://schema.pivotal.io/gemfire/cache/cache-9.0.xsd\"");
        pw.println("version=\"9.0\">");
        pw.println("  <hdfs-store name=\"myHDFSStore\" namenode-url=\"mynamenode\"  home-dir=\"mypath\" />");
        pw.println("  <region name=\"r1\" refid=\"PARTITION_WRITEONLY_HDFS_STORE\">");
        pw.println("    <region-attributes hdfs-store-name=\"myHDFSStore\"/>");
        pw.println("  </region>");
        pw.println("</cache>");
        pw.close();
        bytes = baos.toByteArray();  
        this.c.loadCacheXml(new ByteArrayInputStream(bytes));
        
        r1 = this.c.getRegion("/r1");
        store = c.findHDFSStore(r1.getAttributes().getHDFSStoreName());
        r1.put("k1", "v1");
        assertTrue("Mismatch in attributes, actual.batchsize: " + store.getBatchSize() + " and expected batchsize: 32", store.getBatchSize()== 32);
        assertTrue("Mismatch in attributes, actual.isPersistent: " + store.getBufferPersistent() + " and expected isPersistent: false", store.getBufferPersistent()== false);
        assertTrue("Mismatch in attributes, actual.isRandomAccessAllowed: " + r1.getAttributes().getHDFSWriteOnly() + " and expected isRandomAccessAllowed: false", r1.getAttributes().getHDFSWriteOnly()== false);
        assertTrue("Mismatch in attributes, actual.getDiskStoreName: " + store.getDiskStoreName() + " and expected getDiskStoreName: null", store.getDiskStoreName()== null);
        
        this.c.close();
        
        // use a cache.xml to recover
        this.c = createCache();
        baos = new ByteArrayOutputStream();
        pw = new PrintWriter(new OutputStreamWriter(baos), true);
        pw.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
//        pw.println("<?xml version=\"1.0\"?>");
//        pw.println("<!DOCTYPE cache PUBLIC");
//        pw.println("  \"-//GemStone Systems, Inc.//GemFire Declarative Caching 7.5//EN\"");
//        pw.println("  \"http://www.gemstone.com/dtd/cache7_5.dtd\">");
        pw.println("<cache ");
        pw.println("xmlns=\"http://schema.pivotal.io/gemfire/cache\"");
        pw.println("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"");
        pw.println(" xsi:schemaLocation=\"http://schema.pivotal.io/gemfire/cache http://schema.pivotal.io/gemfire/cache/cache-9.0.xsd\"");
        pw.println("version=\"9.0\">");

        pw.println("  <disk-store name=\"mydiskstore\"/>");
        pw.println("  <hdfs-store name=\"myHDFSStore\" namenode-url=\"mynamenode\"  home-dir=\"mypath\" max-write-only-file-size=\"1\" write-only-file-rollover-interval=\"10\" ");
        pw.println("    batch-size=\"151\" buffer-persistent =\"true\" disk-store=\"mydiskstore\" synchronous-disk-write=\"false\" batch-interval=\"50\"");
        pw.println("  />");
        pw.println("  <region name=\"r1\" refid=\"PARTITION_WRITEONLY_HDFS_STORE\">");
        pw.println("    <region-attributes hdfs-store-name=\"myHDFSStore\" hdfs-write-only=\"false\">");
        pw.println("    </region-attributes>");
        pw.println("  </region>");
        pw.println("</cache>");
        pw.close();
        bytes = baos.toByteArray();
        this.c.loadCacheXml(new ByteArrayInputStream(bytes));
        
        r1 = this.c.getRegion("/r1");
        store = c.findHDFSStore(r1.getAttributes().getHDFSStoreName());
        r1.put("k1", "v1");
        assertTrue("Mismatch in attributes, actual.batchsize: " + store.getBatchSize() + " and expected batchsize: 151", store.getBatchSize()== 151);
        assertTrue("Mismatch in attributes, actual.isPersistent: " + store.getBufferPersistent() + " and expected isPersistent: true", store.getBufferPersistent()== true);
        assertTrue("Mismatch in attributes, actual.isRandomAccessAllowed: " + r1.getAttributes().getHDFSWriteOnly() + " and expected isRandomAccessAllowed: true", r1.getAttributes().getHDFSWriteOnly()== false);
        assertTrue("Mismatch in attributes, actual.getDiskStoreName: " + store.getDiskStoreName() + " and expected getDiskStoreName: mydiskstore", store.getDiskStoreName().equals("mydiskstore"));
        assertTrue("Mismatch in attributes, actual.HDFSStoreName: " + r1.getAttributes().getHDFSStoreName() + " and expected getDiskStoreName: myHDFSStore", r1.getAttributes().getHDFSStoreName().equals("myHDFSStore"));
        assertTrue("Mismatch in attributes, actual.getFolderPath: " + ((GemFireCacheImpl)this.c).findHDFSStore("myHDFSStore").getHomeDir() + " and expected getDiskStoreName: mypath", ((GemFireCacheImpl)this.c).findHDFSStore("myHDFSStore").getHomeDir().equals("mypath"));
        assertTrue("Mismatch in attributes, actual.getNamenode: " + ((GemFireCacheImpl)this.c).findHDFSStore("myHDFSStore").getNameNodeURL()+ " and expected getDiskStoreName: mynamenode", ((GemFireCacheImpl)this.c).findHDFSStore("myHDFSStore").getNameNodeURL().equals("mynamenode"));
        assertTrue("Mismatch in attributes, actual.batchInterval: " + store.getBatchInterval() + " and expected batchsize: 50", store.getBatchInterval()== 50);
        assertTrue("Mismatch in attributes, actual.isDiskSynchronous: " + store.getSynchronousDiskWrite() + " and expected isDiskSynchronous: false", store.getSynchronousDiskWrite()== false);
        assertTrue("Mismatch in attributes, actual.getFileRolloverInterval: " + store.getWriteOnlyFileRolloverInterval() + " and expected getFileRolloverInterval: 10", store.getWriteOnlyFileRolloverInterval() == 10);
        assertTrue("Mismatch in attributes, actual.getMaxFileSize: " + store.getWriteOnlyFileSizeLimit() + " and expected getMaxFileSize: 1MB", store.getWriteOnlyFileSizeLimit() == 1);
        
        this.c.close();
      } finally {
          this.c.close();
      }
    }
   
  /**
   * Validates if hdfs store conf is getting completely and correctly parsed
   */
  public void testHdfsStoreConfFullParsing() {
    String conf = createStoreConf("123");
    this.c.loadCacheXml(new ByteArrayInputStream(conf.getBytes()));
    HDFSStoreImpl store = ((GemFireCacheImpl)this.c).findHDFSStore("store");
    assertEquals("namenode url mismatch.", "url", store.getNameNodeURL());
    assertEquals("home-dir mismatch.", "dir", store.getHomeDir());
    assertEquals("hdfs-client-config-file mismatch.", "client", store.getHDFSClientConfigFile());
    assertEquals("read-cache-size mismatch.", 24.5f, store.getBlockCacheSize());
    
    assertFalse("compaction auto-compact mismatch.", store.getMinorCompaction());
    assertTrue("compaction auto-major-compact mismatch.", store.getMajorCompaction());
    assertEquals("compaction max-concurrency", 23, store.getMinorCompactionThreads());
    assertEquals("compaction max-major-concurrency", 27, store.getMajorCompactionThreads());
    assertEquals("compaction major-interval", 711, store.getPurgeInterval());
  }
  
  /**
   * Validates that the config defaults are set even with minimum XML configuration 
   */
  public void testHdfsStoreConfMinParse() {
    this.c.loadCacheXml(new ByteArrayInputStream(XML_MIN_CONF.getBytes()));
    HDFSStoreImpl store = ((GemFireCacheImpl)this.c).findHDFSStore("store");
    assertEquals("namenode url mismatch.", "url", store.getNameNodeURL());
    assertEquals("home-dir mismatch.", "gemfire", store.getHomeDir());
    
    assertTrue("compaction auto-compact mismatch.", store.getMinorCompaction());
    assertTrue("compaction auto-major-compact mismatch.", store.getMajorCompaction());
    assertEquals("compaction max-input-file-size mismatch.", 512, store.getInputFileSizeMax());
    assertEquals("compaction min-input-file-count.", 4, store.getInputFileCountMin());
    assertEquals("compaction max-iteration-size.", 10, store.getInputFileCountMax());
    assertEquals("compaction max-concurrency", 10, store.getMinorCompactionThreads());
    assertEquals("compaction max-major-concurrency", 2, store.getMajorCompactionThreads());
    assertEquals("compaction major-interval", 720, store.getMajorCompactionInterval());
    assertEquals("compaction cleanup-interval", 30, store.getPurgeInterval());
  }
  
  /**
   * Validates that cache creation fails if a compaction configuration is
   * provided which is not applicable to the selected compaction strategy
   */
  public void testHdfsStoreInvalidCompactionConf() {
    String conf = createStoreConf("123");
    try {
      this.c.loadCacheXml(new ByteArrayInputStream(conf.getBytes()));
      // expected
    } catch (CacheXmlException e) {
      fail();
    }
  }
  
  /**
   * Validates that cache creation fails if a compaction configuration is
   * provided which is not applicable to the selected compaction strategy
   */
  public void testInvalidConfigCheck() throws Exception {
    this.c.close();

    this.c = createCache();

    HDFSStoreFactory hsf; 
    hsf = this.c.createHDFSStoreFactory();
    
    try {
      hsf.setInputFileSizeMax(-1);
      fail("validation failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      hsf.setInputFileCountMin(-1);
      fail("validation failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      hsf.setInputFileCountMax(-1);
      //expected
      fail("validation failed");
    } catch (IllegalArgumentException e) {
    }
    try {
      hsf.setMinorCompactionThreads(-1);
      fail("validation failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      hsf.setMajorCompactionInterval(-1);
      fail("validation failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      hsf.setMajorCompactionThreads(-1);
      fail("validation failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      hsf.setPurgeInterval(-1);
      fail("validation failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      hsf.setInputFileCountMin(2);
      hsf.setInputFileCountMax(1);
      hsf.create("test");
      fail("validation failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
    try {
      hsf.setInputFileCountMax(1);
      hsf.setInputFileCountMin(2);
      hsf.create("test");
      fail("validation failed");
    } catch (IllegalArgumentException e) {
      //expected
    }
  }
  
  /**
   * Validates cache creation fails if invalid integer size configuration is provided
   * @throws Exception
   */
  public void testHdfsStoreConfInvalidInt() throws Exception {
    String conf = createStoreConf("NOT_INTEGER");
    try {
      this.c.loadCacheXml(new ByteArrayInputStream(conf.getBytes()));
      fail();
    } catch (CacheXmlException e) {
      // expected
    }
  }
  

  private static String XML_MIN_CONF = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n"
  + "<cache \n"
  + "xmlns=\"http://schema.pivotal.io/gemfire/cache\"\n"
  + "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
  + " xsi:schemaLocation=\"http://schema.pivotal.io/gemfire/cache http://schema.pivotal.io/gemfire/cache/cache-9.0.xsd\"\n"
  + "version=\"9.0\">" +
          "  <hdfs-store name=\"store\" namenode-url=\"url\" />" +
          "</cache>";
   
  private static String XML_FULL_CONF = "<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n"
                                        + "<cache \n"
                                        + "xmlns=\"http://schema.pivotal.io/gemfire/cache\"\n"
                                        + "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
                                        + " xsi:schemaLocation=\"http://schema.pivotal.io/gemfire/cache http://schema.pivotal.io/gemfire/cache/cache-9.0.xsd\"\n"
                                        + "version=\"9.0\">"
      + "  <hdfs-store name=\"store\" namenode-url=\"url\" "
      + "              home-dir=\"dir\" "
      + "              read-cache-size=\"24.5\" "
      + "              max-write-only-file-size=\"FILE_SIZE_CONF\" "
      + "              minor-compaction-threads = \"23\""
      + "              major-compaction-threads = \"27\""
      + "              major-compaction=\"true\" "
      + "              minor-compaction=\"false\" "
      + "              major-compaction-interval=\"781\" "
      + "              purge-interval=\"711\" hdfs-client-config-file=\"client\" />\n"
      + "</cache>";
  // potential replacement targets
  String FILE_SIZE_CONF_SUBSTRING = "FILE_SIZE_CONF";
  
  private String createStoreConf(String fileSize) {
    String result = XML_FULL_CONF;
    
    String replaceWith = (fileSize == null) ? "123" : fileSize;
    result = result.replaceFirst(FILE_SIZE_CONF_SUBSTRING, replaceWith);

    return result;
  }
  
  public void _testBlockCacheConfiguration() throws Exception {
    this.c.close();
    this.c = createCache();
    try {
      HDFSStoreFactory hsf = this.c.createHDFSStoreFactory();
      
      //Configure a block cache to cache about 20 blocks.
      long heapSize = HeapMemoryMonitor.getTenuredPoolMaxMemory();
      int blockSize = StoreFile.DEFAULT_BLOCKSIZE_SMALL;
      int blockCacheSize = 5 * blockSize;
      int entrySize = blockSize / 2;
      
      
      float percentage = 100 * (float) blockCacheSize / (float) heapSize;
      hsf.setBlockCacheSize(percentage);
      HDFSStoreImpl store = (HDFSStoreImpl) hsf.create("myHDFSStore");
      RegionFactory rf1 = this.c.createRegionFactory(RegionShortcut.PARTITION_HDFS);
      //Create a region that evicts everything
      LocalRegion r1 = (LocalRegion) rf1.setHDFSStoreName("myHDFSStore").setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1)).create("r1");
     
      //Populate about many times our block cache size worth of data
      //We want to try to cache at least 5 blocks worth of index and metadata
      byte[] value = new byte[entrySize];
      int numEntries = 10 * blockCacheSize / entrySize;
      for(int i = 0; i < numEntries; i++) {
        r1.put(i, value);
      }

      //Wait for the events to be written to HDFS.
      Set<String> queueIds = r1.getAsyncEventQueueIds();
      assertEquals(1, queueIds.size());
      AsyncEventQueueImpl queue = (AsyncEventQueueImpl) c.getAsyncEventQueue(queueIds.iterator().next());
      long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
      while(queue.size() > 0 && System.nanoTime() < end) {
        Thread.sleep(10);
      }
      assertEquals(0, queue.size());
      
      
      Thread.sleep(10000);

      //Do some reads to cache some blocks. Note that this doesn't
      //end up caching data blocks, just index and bloom filters blocks.
      for(int i = 0; i < numEntries; i++) {
        r1.get(i);
      }
      
      long statSize = store.getStats().getBlockCache().getBytesCached();
      assertTrue("Block cache stats expected to be near " + blockCacheSize + " was " + statSize, 
          blockCacheSize / 2  < statSize &&
          statSize <=  2 * blockCacheSize);
      
      long currentSize = store.getBlockCache().getCurrentSize();
      assertTrue("Block cache size expected to be near " + blockCacheSize + " was " + currentSize, 
          blockCacheSize / 2  < currentSize &&
          currentSize <= 2 * blockCacheSize);
      
    } finally {
      this.c.close();
    }
  }

  protected GemFireCacheImpl createCache() {
    return (GemFireCacheImpl) new CacheFactory().set("mcast-port", "0").set("log-level", "info")
    .create();
  }
}
