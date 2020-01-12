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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.Properties;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.DistributedSystem;

/**
 * TRAC #40255: Parameterization of XML configuration file
 */
@Ignore("Test is broken and was named *JUnitDisabledTest")
public class CacheXmlParameterizationRegressionTest {

  private static final String CACHE_XML = CacheXmlParameterizationRegressionTest.class
      .getResource("CacheXmlParameterizationRegressionTest_cache.xml").getFile();
  private static final String GEMFIRE_PROPERTIES = CacheXmlParameterizationRegressionTest.class
      .getResource("CacheXmlParameterizationRegressionTest_gemfire.properties").getFile();

  private static final String ATTR_PROPERTY_STRING = "region.disk.store";
  private static final String ATTR_PROPERTY_VALUE = "teststore";
  private static final String NESTED_ATTR_PROPERTY_STRING = "custom-nested.test";
  private static final String NESTED_ATTR_PROPERTY_VALUE = "disk";
  private static final String ELEMENT_PROPERTY_STRING = "custom-string.element";
  private static final String ELEMENT_PROPERTY_VALUE = "example-string";
  private static final String CONCAT_ELEMENT_PROPERTY_STRING = "concat.test";
  private static final String CONCAT_ELEMENT_PROPERTY_VALUE = "-name";
  private static final String ELEMENT_KEY_VALUE = "example-value";

  private DistributedSystem ds;
  private Cache cache;

  @After
  public void tearDown() throws Exception {
    if (cache != null) {
      cache.close();
      cache = null;
    }
    if (ds != null) {
      ds.disconnect();
      ds = null;
    }
  }

  @Test
  public void testResolveReplacePropertyStringForLonerCache() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    System.setProperty("gemfirePropertyFile", GEMFIRE_PROPERTIES);
    props.setProperty(CACHE_XML_FILE, CACHE_XML);
    System.setProperty(NESTED_ATTR_PROPERTY_STRING, NESTED_ATTR_PROPERTY_VALUE);
    System.setProperty(ATTR_PROPERTY_STRING, ATTR_PROPERTY_VALUE);
    System.setProperty(ELEMENT_PROPERTY_STRING, ELEMENT_PROPERTY_VALUE);
    System.setProperty(CONCAT_ELEMENT_PROPERTY_STRING, CONCAT_ELEMENT_PROPERTY_VALUE);

    // create the directory where data is going to be stored
    File dir = new File("persistData1");
    dir.mkdir();

    ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);

    Region exampleRegion = cache.getRegion("example-region");
    RegionAttributes<Object, Object> attrs = exampleRegion.getAttributes();

    // Check if disk store got same name as passed in system properties in setup().
    assertEquals(attrs.getDiskStoreName(), System.getProperty(ATTR_PROPERTY_STRING));
    assertNotNull(exampleRegion.get(ELEMENT_PROPERTY_VALUE + CONCAT_ELEMENT_PROPERTY_VALUE));
    assertEquals(exampleRegion.get(ELEMENT_PROPERTY_VALUE + CONCAT_ELEMENT_PROPERTY_VALUE),
        ELEMENT_KEY_VALUE);
    assertNotNull(exampleRegion.get(ELEMENT_PROPERTY_VALUE));
    assertEquals(exampleRegion.get(ELEMENT_PROPERTY_VALUE), CONCAT_ELEMENT_PROPERTY_VALUE);
  }

  @Test
  public void testResolveReplacePropertyStringForNonLonerCache() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "10333");
    props.setProperty(LOCATORS, "");
    System.setProperty("gemfirePropertyFile", GEMFIRE_PROPERTIES);
    props.setProperty(CACHE_XML_FILE, CACHE_XML);
    System.setProperty(NESTED_ATTR_PROPERTY_STRING, NESTED_ATTR_PROPERTY_VALUE);
    System.setProperty(ATTR_PROPERTY_STRING, ATTR_PROPERTY_VALUE);
    System.setProperty(ELEMENT_PROPERTY_STRING, ELEMENT_PROPERTY_VALUE);
    System.setProperty(CONCAT_ELEMENT_PROPERTY_STRING, CONCAT_ELEMENT_PROPERTY_VALUE);

    // create the directory where data is going to be stored
    File dir = new File("persistData1");
    dir.mkdir();

    ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);

    Region exampleRegion = cache.getRegion("example-region");
    RegionAttributes<Object, Object> attrs = exampleRegion.getAttributes();

    // Check if disk store got same name as passed in system properties in setup().
    assertEquals(attrs.getDiskStoreName(), System.getProperty(ATTR_PROPERTY_STRING));
    assertNotNull(exampleRegion.get(ELEMENT_PROPERTY_VALUE + CONCAT_ELEMENT_PROPERTY_VALUE));
    assertEquals(exampleRegion.get(ELEMENT_PROPERTY_VALUE + CONCAT_ELEMENT_PROPERTY_VALUE),
        ELEMENT_KEY_VALUE);
  }
}
