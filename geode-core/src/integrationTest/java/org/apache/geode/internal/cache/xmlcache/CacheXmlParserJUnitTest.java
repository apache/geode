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
package org.apache.geode.internal.cache.xmlcache;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.Attributes;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.services.module.impl.ServiceLoaderModuleService;

/**
 * Test cases for {@link CacheXmlParser}.
 *
 * @since GemFire 8.1
 */
public class CacheXmlParserJUnitTest {

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public final TemporaryFolder temporaryFolderRule = new TemporaryFolder();

  private static final String NAMESPACE_URI =
      "urn:java:org.apache.geode.internal.cache.xmlcache.MockXmlParser";

  @After
  public void tearDown() throws Exception {
    InternalDistributedSystem.removeSystem(InternalDistributedSystem.getConnectedInstance());
  }

  /**
   * Test {@link CacheXmlParser#getDelegate(String)}.
   *
   * Asserts that a delegate is found and that the stack and logWriter are setup correctly.
   *
   * Asserts that delegate is cached between calls and that the same instance is returned.
   *
   * Asserts that null is returned when no {@link XmlParser} is registered for namespace.
   *
   * @since GemFire 8.1
   */
  @Test
  public void testGetDelegate() {
    final TestCacheXmlParser cacheXmlParser = new TestCacheXmlParser();
    cacheXmlParser.init(new ServiceLoaderModuleService(LogService.getLogger()));
    assertThat(cacheXmlParser.getDelegates()).as("delegates should be empty.").isEmpty();

    final MockXmlParser delegate = (MockXmlParser) cacheXmlParser.getDelegate(NAMESPACE_URI);
    assertThat(delegate).as("Delegate should be found in classpath.").isNotNull();
    assertThat(cacheXmlParser.stack).as("Should have same stack as cacheXmlParser.")
        .isSameAs(delegate.stack);
    assertThat(cacheXmlParser.documentLocator).as("Should have same stack as cacheXmlParser.")
        .isSameAs(delegate.documentLocator);
    assertThat(cacheXmlParser.getDelegates().size()).as("Should be exactly 1 delegate.")
        .isEqualTo(1);
    assertThat(cacheXmlParser.getDelegates().get(NAMESPACE_URI))
        .as("There should be an entry in delegates cache.").isNotNull();
    assertThat(cacheXmlParser.getDelegates().get(NAMESPACE_URI))
        .as("Cached delegate should match the one from get.").isSameAs(delegate);

    final MockXmlParser delegate2 = (MockXmlParser) cacheXmlParser.getDelegate(NAMESPACE_URI);
    assertThat(delegate2).as("Delegate should be the same between gets.").isSameAs(delegate);
    assertThat(cacheXmlParser.getDelegates().size()).as("Should still be exactly 1 delegate.")
        .isEqualTo(1);
    assertThat(cacheXmlParser.getDelegate("--nothing-should-use-this-namespace--")).isNull();
  }

  /**
   * Test that {@link CacheXmlParser} can parse the test cache.xml file.
   *
   * @since Geode 1.2
   */
  @Test
  public void testCacheXmlParserWithSimplePool() {
    Properties nonDefault = new Properties();
    nonDefault.setProperty(MCAST_PORT, "0"); // loner

    assertThatCode(() -> {
      ClientCache cache = new ClientCacheFactory(nonDefault).set("cache-xml-file",
          "xmlcache/CacheXmlParserJUnitTest.testSimpleClientCacheXml.cache.xml").create();
      cache.close();
    }).doesNotThrowAnyException();
  }

  /**
   * Test that {@link CacheXmlParser} can parse the test cache.xml file, using the Apache Xerces XML
   * parser.
   *
   * @since Geode 1.3
   */
  @Test
  public void testCacheXmlParserWithSimplePoolXerces() {
    System.setProperty("javax.xml.parsers.SAXParserFactory",
        "org.apache.xerces.jaxp.SAXParserFactoryImpl");

    testCacheXmlParserWithSimplePool();
  }

  @Test
  public void cacheXmlParserShouldCorrectlyHandleWithMultiplePools() {
    Properties nonDefault = new Properties();
    nonDefault.setProperty(MCAST_PORT, "0"); // loner


    ClientCache cache = new ClientCacheFactory(nonDefault).set("cache-xml-file",
        "xmlcache/CacheXmlParserJUnitTest.testMultiplePools.cache.xml").create();
    assertThat(cache.getRegion("regionOne").getAttributes().getPoolName()).isEqualTo("poolOne");
    assertThat(cache.getRegion("regionTwo").getAttributes().getPoolName()).isEqualTo("poolTwo");
    cache.close();
  }

  @Test
  public void cacheXmlParserShouldThrowExceptionWhenPoolDoesNotExist() {
    Properties nonDefault = new Properties();
    nonDefault.setProperty(MCAST_PORT, "0"); // loner

    assertThatThrownBy(() -> new ClientCacheFactory(nonDefault).set("cache-xml-file",
        "xmlcache/CacheXmlParserJUnitTest.testRegionWithNonExistingPool.cache.xml").create())
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("The connection pool nonExistingPool has not been created");
  }

  @Test
  public void cacheXmlParserShouldCorrectlyHandleDiskUsageWarningPercentage() {
    String diskStoreName = "myDiskStore";
    System.setProperty("DISK_STORE_NAME", diskStoreName);
    System.setProperty("DISK_STORE_DIRECTORY", temporaryFolderRule.getRoot().getAbsolutePath());

    Cache cache =
        new CacheFactory()
            .set("cache-xml-file",
                "xmlcache/CacheXmlParserJUnitTest.testDiskUsageWarningPercentage.cache.xml")
            .create();
    DiskStore diskStore = cache.findDiskStore(diskStoreName);
    assertThat(diskStore).isNotNull();
    assertThat(diskStore.getDiskUsageWarningPercentage()).isEqualTo(70);
    cache.close();
  }

  /**
   * Test that {@link CacheXmlParser} falls back to DTD parsing when locale language is not English.
   *
   * @since Geode 1.0
   */
  @Test
  public void testDTDFallbackWithNonEnglishLocal() {
    CacheXmlParser.parse(this.getClass().getResourceAsStream(
        "CacheXmlParserJUnitTest.testDTDFallbackWithNonEnglishLocal.cache.xml"),
        new ServiceLoaderModuleService(LogService.getLogger()));

    final Locale previousLocale = Locale.getDefault();
    try {
      Locale.setDefault(Locale.JAPAN);
      CacheXmlParser.parse(this.getClass().getResourceAsStream(
          "CacheXmlParserJUnitTest.testDTDFallbackWithNonEnglishLocal.cache.xml"),
          new ServiceLoaderModuleService(LogService.getLogger()));
    } finally {
      Locale.setDefault(previousLocale);
    }
  }

  /**
   * Test that {@link CacheXmlParser} falls back to DTD parsing when locale language is not English,
   * using the Apache Xerces XML parser.
   *
   * @since Geode 1.3
   */
  @Test
  public void testDTDFallbackWithNonEnglishLocalXerces() {
    System.setProperty("javax.xml.parsers.SAXParserFactory",
        "org.apache.xerces.jaxp.SAXParserFactoryImpl");

    testDTDFallbackWithNonEnglishLocal();
  }

  /**
   * Get access to {@link CacheXmlParser} protected methods and fields.
   *
   * @since GemFire 8.1
   */
  private static class TestCacheXmlParser extends CacheXmlParser {
    static Field delegatesField;
    static Method getDelegateMethod;

    static {
      try {
        delegatesField = CacheXmlParser.class.getDeclaredField("delegates");
        delegatesField.setAccessible(true);

        getDelegateMethod = CacheXmlParser.class.getDeclaredMethod("getDelegate", String.class);
        getDelegateMethod.setAccessible(true);
      } catch (NoSuchFieldException | SecurityException | NoSuchMethodException e) {
        throw new IllegalStateException(e);
      }
    }

    /**
     * @return {@link CacheXmlParser} private delegates field.
     * @since GemFire 8.1
     */
    @SuppressWarnings("unchecked")
    HashMap<String, XmlParser> getDelegates() {
      try {
        return (HashMap<String, XmlParser>) delegatesField.get(this);
      } catch (IllegalArgumentException | IllegalAccessException e) {
        throw new IllegalStateException(e);
      }
    }

    /**
     * Access to {@link CacheXmlParser} getDelegate(String) method.
     *
     * @since GemFire 8.1
     */
    XmlParser getDelegate(final String namespaceUri) {
      try {
        return (XmlParser) getDelegateMethod.invoke(this, namespaceUri);
      } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  public static class MockXmlParser extends AbstractXmlParser {

    @Override
    public String getNamespaceUri() {
      return "urn:java:org.apache.geode.internal.cache.xmlcache.MockXmlParser";
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void endElement(String uri, String localName, String qName) {
      throw new UnsupportedOperationException();
    }
  }
}
