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
package org.apache.geode.cache30;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;

import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXml;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.cache.xmlcache.ClientCacheCreation;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class CacheXmlTestCase extends JUnit4CacheTestCase {

  /** The file used by this test (method) to initialize the cache */
  private File xmlFile;

  /** set this to false if a test needs a non-loner distributed system */
  static boolean lonerDistributedSystem = true;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    this.xmlFile = null;
    GemFireCacheImpl.testCacheXml = null;

    waitForNoRebalancing();
    Invoke.invokeInEveryVM(CacheXmlTestCase::waitForNoRebalancing);

    super.preTearDownCacheTestCase();
  }

  /**
   * Some tests run so quickly that the rebalance operation doesn't even have time to start before
   * regions are already being destroyed - GEDOE-4312.
   */
  private static void waitForNoRebalancing() {
    if (cache != null && !cache.isClosed()) {
      await().until(() -> {
        return cache.getResourceManager().getRebalanceOperations().size() == 0;
      });
    }
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * The version of GemFire tested by this class. This method should be overridden by subclasses.
   *
   * @see CacheXml#VERSION_3_0
   */
  protected String getGemFireVersion() {
    return CacheXml.VERSION_3_0;
  }

  protected boolean getUseSchema() {
    return false;
  }

  /**
   * Sets the file used by this test to initialize the cache
   */
  protected void setXmlFile(File xmlFile) {
    this.xmlFile = xmlFile;
  }

  /**
   * Finds an XML file with the given name. Looks in $JTESTS.
   */
  protected File findFile(String fileName) throws IOException {
    return copyResourceToDirectory(this.temporaryFolder.getRoot(), fileName);
    // String path = TestUtil.getResourcePath(getClass(), fileName);
    // return new File(path);
  }

  protected File copyResourceToDirectory(File directory, String fileName) throws IOException {
    URL url = getClass().getResource(fileName);
    File file = new File(directory, fileName);
    FileUtils.copyURLToFile(url, file);
    return file;
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    if (this.xmlFile != null) {
      props.setProperty(CACHE_XML_FILE, this.xmlFile.toString());
    }

    // make it a loner
    if (lonerDistributedSystem) {
      props.setProperty(MCAST_PORT, "0");
      props.setProperty(LOCATORS, "");
    }

    return props;
  }

  /**
   * Uses the {@link CacheXmlGenerator} to generate an XML file from the given
   * {@link CacheCreation}. It then creates a {@link Cache} from the XML and asserts that it is the
   * same as the cache described in the <code>CacheCreation</code>.
   */
  protected void testXml(CacheCreation creation) throws IOException {
    testXml(creation, true);
  }

  protected void testXml(CacheCreation creation, boolean checkSame) throws IOException {
    File root = this.temporaryFolder.getRoot();
    File dir = new File(root, "XML_" + getGemFireVersion());
    dir.mkdirs();
    File file = new File(dir, getUniqueName() + ".xml");

    final boolean useSchema = getUseSchema();
    final String version = getGemFireVersion();

    PrintWriter pw = new PrintWriter(new FileWriter(file), true);
    CacheXmlGenerator.generate(creation, pw, useSchema, version);
    pw.close();

    setXmlFile(file);

    boolean client = creation instanceof ClientCacheCreation;
    Cache cache = getCache(client);

    try {
      if (checkSame && !creation.sameAs(cache)) {
        StringWriter sw = new StringWriter();
        CacheXmlGenerator.generate(creation, new PrintWriter(sw, true), useSchema, version);
        CacheXmlGenerator.generate(cache, new PrintWriter(sw, true), useSchema, version);
        fail(sw.toString());
      }
    } catch (RuntimeException re) {
      StringWriter sw = new StringWriter();
      CacheXmlGenerator.generate(creation, new PrintWriter(sw, true), useSchema, version);
      CacheXmlGenerator.generate(cache, new PrintWriter(sw, true), useSchema, version);
      fail(sw.toString(), re);
    }
  }
}
