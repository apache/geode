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
package com.gemstone.gemfire.internal.logging.log4j;

import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * UnitTest for ConfigLocator which is used to find the Log4J 2 configuration file.
 * 
 * @since GemFire 8.2
 */
@Category(IntegrationTest.class)
public class ConfigLocatorJUnitTest {

  private static Set<String> suffixesNotFoundTested = new HashSet<String>();
  private static Set<String> suffixesFoundTested = new HashSet<String>();
  
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  
  @AfterClass
  public static void afterClass() {
    // ensure that every suffix was tested by Found and NotFound
    Set<String> suffixes = new HashSet<String>(Arrays.asList(ConfigLocator.SUFFIXES));
    try {
      assertEquals(suffixes, suffixesNotFoundTested);
      assertEquals(suffixes, suffixesFoundTested);
    } finally {
      suffixesNotFoundTested = null;
      suffixesFoundTested = null;
    }
  }
  
  @Test
  public void testYamlConfigIsNotFoundWhenNotInClasspath() throws Exception {
    configIsNotFoundWhenNotInClasspath(testingNotFoundSuffix(ConfigLocator.SUFFIX_TEST_YAML));
  }
  
  @Test
  public void testYamlConfigIsFoundWhenInClasspath() throws Exception {
    configIsFoundWhenInClasspath(testingFoundSuffix(ConfigLocator.SUFFIX_TEST_YAML));
  }
  
  @Test
  public void testYmlConfigIsNotFoundWhenNotInClasspath() throws Exception {
    configIsNotFoundWhenNotInClasspath(testingNotFoundSuffix(ConfigLocator.SUFFIX_TEST_YML));
  }
  
  @Test
  public void testYmlConfigIsFoundWhenInClasspath() throws Exception {
    configIsFoundWhenInClasspath(testingFoundSuffix(ConfigLocator.SUFFIX_TEST_YML));
  }
  
  @Test
  public void testJsonConfigIsNotFoundWhenNotInClasspath() throws Exception {
    configIsNotFoundWhenNotInClasspath(testingNotFoundSuffix(ConfigLocator.SUFFIX_TEST_JSON));
  }
  
  @Test
  public void testJsonConfigIsFoundWhenInClasspath() throws Exception {
    configIsFoundWhenInClasspath(testingFoundSuffix(ConfigLocator.SUFFIX_TEST_JSON));
  }
  
  @Test
  public void testJsnConfigIsNotFoundWhenNotInClasspath() throws Exception {
    configIsNotFoundWhenNotInClasspath(testingNotFoundSuffix(ConfigLocator.SUFFIX_TEST_JSN));
  }
  
  @Test
  public void testJsnConfigIsFoundWhenInClasspath() throws Exception {
    configIsFoundWhenInClasspath(testingFoundSuffix(ConfigLocator.SUFFIX_TEST_JSN));
  }
  
  @Test
  public void testXmlConfigIsNotFoundWhenNotInClasspath() throws Exception {
    configIsNotFoundWhenNotInClasspath(testingNotFoundSuffix(ConfigLocator.SUFFIX_TEST_XML));
  }
  
  @Test
  public void testXmlConfigIsFoundWhenInClasspath() throws Exception {
    configIsFoundWhenInClasspath(testingFoundSuffix(ConfigLocator.SUFFIX_TEST_XML));
  }
  
  @Test
  public void yamlConfigIsNotFoundWhenNotInClasspath() throws Exception {
    configIsNotFoundWhenNotInClasspath(testingNotFoundSuffix(ConfigLocator.SUFFIX_YAML));
  }
  
  @Test
  public void yamlConfigIsFoundWhenInClasspath() throws Exception {
    configIsFoundWhenInClasspath(testingFoundSuffix(ConfigLocator.SUFFIX_YAML));
  }
  
  @Test
  public void ymlConfigIsNotFoundWhenNotInClasspath() throws Exception {
    configIsNotFoundWhenNotInClasspath(testingNotFoundSuffix(ConfigLocator.SUFFIX_YML));
  }
  
  @Test
  public void ymlConfigIsFoundWhenInClasspath() throws Exception {
    configIsFoundWhenInClasspath(testingFoundSuffix(ConfigLocator.SUFFIX_YML));
  }
  
  @Test
  public void jsonConfigIsNotFoundWhenNotInClasspath() throws Exception {
    configIsNotFoundWhenNotInClasspath(testingNotFoundSuffix(ConfigLocator.SUFFIX_JSON));
  }
  
  @Test
  public void jsonConfigIsFoundWhenInClasspath() throws Exception {
    configIsFoundWhenInClasspath(testingFoundSuffix(ConfigLocator.SUFFIX_JSON));
  }
  
  @Test
  public void jsnConfigIsNotFoundWhenNotInClasspath() throws Exception {
    configIsNotFoundWhenNotInClasspath(testingNotFoundSuffix(ConfigLocator.SUFFIX_JSN));
  }
  
  @Test
  public void jsnConfigIsFoundWhenInClasspath() throws Exception {
    configIsFoundWhenInClasspath(testingFoundSuffix(ConfigLocator.SUFFIX_JSN));
  }
  
  @Test
  public void xmlConfigIsNotFoundWhenNotInClasspath() throws Exception {
    configIsNotFoundWhenNotInClasspath(testingNotFoundSuffix(ConfigLocator.SUFFIX_XML));
  }
  
  @Test
  public void xmlConfigIsFoundWhenInClasspath() throws Exception {
    configIsFoundWhenInClasspath(testingFoundSuffix(ConfigLocator.SUFFIX_XML));
  }
  
  private String testingNotFoundSuffix(final String suffix) {
    // ensure each suffix is only tested for NotFound once
    assertFalse(suffixesNotFoundTested.contains(suffix)); 
    suffixesNotFoundTested.add(suffix);
    return suffix;
  }
  
  private String testingFoundSuffix(final String suffix) {
    // ensure each suffix is only tested for Found once
    assertFalse(suffixesFoundTested.contains(suffix));
    suffixesFoundTested.add(suffix);
    return suffix;
  }
  
  private void configIsNotFoundWhenNotInClasspath(final String suffix) throws Exception {
    String fileName = ConfigLocator.PREFIX + suffix;
    assumeTrue(null == getClass().getClassLoader().getResource(fileName));
    assumeTrue(null == ConfigLocator.findConfigInClasspath());
    
    // create xml config file outside of classpath
    File configFile = folder.newFile(fileName);
    assertTrue(configFile.isFile());
    
    // make sure xml config file is not found
    assertNull(ConfigLocator.findConfigInClasspath());
  }
  
  private void configIsFoundWhenInClasspath(final String suffix) throws Exception {
    String fileName = ConfigLocator.PREFIX + suffix;
    assumeTrue(null == getClass().getClassLoader().getResource(fileName));
    assumeTrue(null == ConfigLocator.findConfigInClasspath());

    // create config file
    File configFile = folder.newFile(fileName);
    assertTrue(configFile.isFile());
    
    assertNull(ConfigLocator.findConfigInClasspath());
    
    // create class loader
    ClassLoader customLoader = new URLClassLoader(new URL[] { folder.getRoot().toURI().toURL() });
    assertNotNull(customLoader.getResource(fileName));
    
    // temporarily add class loader to the classpath searched by ConfigLocator 
    ClassLoader tccl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(customLoader);
      
      // verify config file can be found in classpath
      URL url = ConfigLocator.findConfigInClasspath();
      assertNotNull(url);
      assertEquals(configFile.toURI().toURL(), url);
    } finally {
      Thread.currentThread().setContextClassLoader(tccl);
    }

    assertNull(ConfigLocator.findConfigInClasspath());
  }
}
