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
package org.apache.geode.internal.compression;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.compression.Compressor;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Tests configured and badly configured cache.xml files with regards to compression.
 */
@Category(DistributedTest.class)
public class CompressionCacheConfigDUnitTest extends JUnit4CacheTestCase {

  /**
   * The name of our test region.
   */
  public static final String REGION_NAME = "compressedRegion";
  
  /**
   * Sample cache.xml with a recognized compressor.
   */
  private static final String GOOD_COMPRESSOR = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE cache PUBLIC \"-//GemStone Systems, Inc.//GemFire Declarative Cache 8.0//EN\" \"http://www.gemstone.com/dtd/cache8_0.dtd\">\n<cache lock-lease=\"120\" lock-timeout=\"60\" search-timeout=\"300\" is-server=\"true\" copy-on-read=\"false\">\n<region name=\"compressedRegion\">\n<region-attributes data-policy=\"replicate\" cloning-enabled=\"true\">\n<compressor>\n<class-name>org.apache.geode.compression.SnappyCompressor</class-name>\n</compressor>\n</region-attributes>\n</region>\n</cache>";

  /**
   * Sample cache.xml with an unrecognized compressor.
   */
  private static final String BAD_COMPRESSOR = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE cache PUBLIC \"-//GemStone Systems, Inc.//GemFire Declarative Cache 8.0//EN\" \"http://www.gemstone.com/dtd/cache8_0.dtd\">\n<cache lock-lease=\"120\" lock-timeout=\"60\" search-timeout=\"300\" is-server=\"true\" copy-on-read=\"false\">\n<region name=\"compressedRegion\">\n<region-attributes data-policy=\"replicate\" cloning-enabled=\"true\">\n<compressor>\n<class-name>BAD_COMPRESSOR</class-name>\n</compressor>\n</region-attributes>\n</region>\n</cache>";

  /**
   * Asserts that a member is successfully initialized with a compressed region when
   * a compressor is included in the region attributes.
   */
  @Test
  public void testCreateCacheWithGoodCompressor() throws Exception {
    File cacheXml = createCacheXml(GOOD_COMPRESSOR);
    assertTrue(createCacheOnVM(getVM(0),cacheXml.getCanonicalPath()));
    assertCompressorOnVM(getVM(0), new SnappyCompressor(), REGION_NAME);
    cleanup(getVM(0));
    cacheXml.delete();
  }
  
  /**
   * Asserts that member initialization fails when an unrecognized compressor is declared in the
   * cache.xml.
   */
  @Test
  public void testCreateCacheWithBadCompressor() throws Exception {
    IgnoredException.addIgnoredException("Unable to load class BAD_COMPRESSOR");
    File cacheXml = createCacheXml(BAD_COMPRESSOR);
    IgnoredException expectedException = IgnoredException.addIgnoredException("While reading Cache XML file");
    try {
      assertFalse(createCacheOnVM(getVM(0), cacheXml.getCanonicalPath()));
    } finally {
      expectedException.remove();
      cacheXml.delete();
    }
  }
  
  /**
   * Asserts that a region is compressed using a given compressor.
   * @param vm a peer.
   * @param compressor a compressor.
   * @param regionName a compressed region.
   */
  private void assertCompressorOnVM(final VM vm,final Compressor compressor,final String regionName) {
   vm.invoke(new SerializableRunnable() {
    @Override
    public void run() {
      Region<String,String> region = getCache().getRegion(regionName);
      assertNotNull(region);
      assertTrue(compressor.equals(((LocalRegion) region).getCompressor()));
    }     
   });
  }
  
  /**
   * Creates a new Cache for a given VM using a cache.xml.
   * @param vm a peer.
   * @param cacheXml a declaritive xml file.
   * @return true if the cache was created, false otherwise.
   */
  private boolean createCacheOnVM(final VM vm,final String cacheXml) {
    return (Boolean) vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        try {
          disconnectFromDS();
          Properties props = new Properties();
          props.setProperty(CACHE_XML_FILE, cacheXml);
          LogWriterUtils.getLogWriter().info("<ExpectedException action=add>ClassNotFoundException</ExpectedException>");
          getSystem(props);
          assertNotNull(getCache());
          return Boolean.TRUE;
        } catch(Exception e) {
          LogWriterUtils.getLogWriter().error("Could not create the cache", e);
          return Boolean.FALSE;
        } finally {
          LogWriterUtils.getLogWriter().info("<ExpectedException action=remove>ClassNotFoundException</ExpectedException>");
        }
      }      
    });
  }
  
  /**
   * Creates a temporary cache.xml on the file system. 
   * @param contents cache.xml contents.
   * @return A File representing the created file.
   * @throws IOException something bad happened.
   */
  private File createCacheXml(String contents) throws IOException{
    File cacheXml = File.createTempFile("cache", "xml");
    PrintStream pstream = new PrintStream(cacheXml);
    pstream.print(contents);
    pstream.close();
    
    return cacheXml;
  }

  /**
   * Returns the VM for a given identifier.
   * @param vm a virtual machine identifier.
   */
  private VM getVM(int vm) {
    return Host.getHost(0).getVM(vm);
  }

  /**
   * Removes created regions from a VM.
   * @param vm the virtual machine to cleanup.
   */
  private void cleanup(final VM vm) {
    vm.invoke(new SerializableRunnable() {      
      @Override
      public void run() {
        Region<String,String> region = getCache().getRegion(REGION_NAME);         
        assertNotNull(region);
        region.destroyRegion();
        disconnectFromDS();
      }
    });        
  }
}

