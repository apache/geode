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
package com.gemstone.gemfire.cache30;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.ClientCacheCreation;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.util.test.TestUtil;

public class CacheXmlTestCase extends CacheTestCase {

  /** The file used by this test (method) to initialize the cache */
  private File xmlFile;
  
  /** set this to false if a test needs a non-loner distributed system */
  static boolean lonerDistributedSystem = true;

  public CacheXmlTestCase(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    disconnectAllFromDS();
  }

  @Override
  protected final void preTearDownCacheTestCase() throws Exception {
    this.xmlFile = null;    
  }
  
  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * The version of GemFire tested by this class.  This method should
   * be overridden by subclasses.
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
   * Finds an XML file with the given name.  Looks in $JTESTS.
   */
  protected File findFile(String fileName) {
    String path = TestUtil.getResourcePath(getClass(), fileName);
    return new File(path);
  }

  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    if (this.xmlFile != null) {
      props.setProperty(DistributionConfig.CACHE_XML_FILE_NAME,
                        this.xmlFile.toString());
    }

    // make it a loner
    if (lonerDistributedSystem) {
      props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    }

    return props;
  }

  /**
   * Uses the {@link CacheXmlGenerator} to generate an XML file from
   * the given {@link CacheCreation}.  It then creates a {@link Cache}
   * from the XML and asserts that it is the same as the cache
   * described in the <code>CacheCreation</code>.
   */
  protected void testXml(CacheCreation creation) {
    testXml(creation, true);
  }
  protected void testXml(CacheCreation creation, boolean checkSame) {

    File dir = new File("XML_" + this.getGemFireVersion());
    dir.mkdirs();
    File file = new File(dir, this.getUniqueName() + ".xml");

    final boolean useSchema = getUseSchema();
    final String version = getGemFireVersion();
    try {
      PrintWriter pw = new PrintWriter(new FileWriter(file), true);
      CacheXmlGenerator.generate(creation, pw, useSchema, version);
      pw.close();

    } catch (IOException ex) {
      String s = "While generating XML";
      Assert.fail(s, ex);
    }

    setXmlFile(file);

    boolean client = creation instanceof ClientCacheCreation;
    Cache cache = getCache(client);

    try {
      if (checkSame && !creation.sameAs(cache)) {
        StringWriter sw = new StringWriter();
        CacheXmlGenerator.generate(creation, new PrintWriter(sw, true),
            useSchema, version);
        CacheXmlGenerator.generate(cache, new PrintWriter(sw, true),
            useSchema, version);
        fail(sw.toString());
      }
    } catch (RuntimeException re) {
      StringWriter sw = new StringWriter();
      CacheXmlGenerator.generate(creation, new PrintWriter(sw, true),
          useSchema, version);
      CacheXmlGenerator.generate(cache, new PrintWriter(sw, true),
          useSchema, version);
      Assert.fail(sw.toString(), re);
    }
  }
}
