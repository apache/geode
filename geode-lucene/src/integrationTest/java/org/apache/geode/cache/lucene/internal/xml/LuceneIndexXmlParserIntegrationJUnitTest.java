/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.xml;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.test.LuceneTestSerializer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.Extension;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXmlParser;
import org.apache.geode.internal.cache.xmlcache.RegionCreation;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneIndexXmlParserIntegrationJUnitTest {

  @Rule
  public TestName name = new TestName();

  public void tearDownCacheAfterCreateIndexTestIsCompleted(Cache cache) {
    if (cache != null) {
      cache.close();
    }
  }

  /**
   * Test that we parse the index fields correctly
   */
  @Test
  public void parseIndex() throws FileNotFoundException {
    RegionCreation region = createRegionCreation("region");
    Map<String, String[]> expectedIndexes = new HashMap<>();
    expectedIndexes.put("index1", new String[] {"a", "b", "c", "d"});
    expectedIndexes.put("index2", new String[] {"f", "g"});
    validateExpectedIndexes(region, expectedIndexes);
  }

  /**
   * Test that we parse the index fields correctly
   */
  @Test(expected = CacheXmlException.class)
  public void invalidXmlShouldThrowParseException() throws FileNotFoundException {
    RegionCreation region = createRegionCreation("region");
  }

  /**
   * Test that we parse the index fields correctly
   */
  @Test(expected = CacheXmlException.class)
  public void invalidLuceneElementShouldThrowParseException() throws FileNotFoundException {
    RegionCreation region = createRegionCreation("region");
  }

  /**
   * Test that we parse the index fields correctly
   */
  @Test(expected = CacheXmlException.class)
  public void invalidXmlLocationShouldThrowParseException() throws FileNotFoundException {
    RegionCreation region = createRegionCreation("region");
  }

  @Test
  public void parseIndexWithAnalyzers() throws FileNotFoundException {
    RegionCreation region = createRegionCreation("region");

    // Validate expected indexes
    Map<String, String[]> expectedIndexes = new HashMap<>();
    expectedIndexes.put("index", new String[] {"a", "b", "c"});
    validateExpectedIndexes(region, expectedIndexes);

    // Validate expected analyzers
    Map<String, Map<String, Class<? extends Analyzer>>> expectedIndexAnalyzers = new HashMap<>();
    Map<String, Class<? extends Analyzer>> expectedFieldAnalyzers = new HashMap<>();
    expectedFieldAnalyzers.put("a", KeywordAnalyzer.class);
    expectedFieldAnalyzers.put("b", SimpleAnalyzer.class);
    expectedFieldAnalyzers.put("c", ClassicAnalyzer.class);
    expectedIndexAnalyzers.put("index", expectedFieldAnalyzers);
    validateExpectedAnalyzers(region, expectedIndexAnalyzers);
  }

  @Test
  public void parseIndexWithSerializerAndStringProperty() throws FileNotFoundException {
    RegionCreation region = createRegionCreation("region");

    // Validate expected indexes
    Map<String, String[]> expectedIndexes = new HashMap<>();
    expectedIndexes.put("index", new String[] {"a"});
    validateExpectedIndexes(region, expectedIndexes);

    // Validate expected serializer
    Properties expected = new Properties();
    expected.setProperty("param_from_xml", "value_from_xml");
    validateExpectedSerializer(region, expected);
  }

  @Test
  public void parseIndexWithSerializerAndDeclarableProperty() throws FileNotFoundException {
    RegionCreation region = createRegionCreation("region");

    // Validate expected indexes
    Map<String, String[]> expectedIndexes = new HashMap<>();
    expectedIndexes.put("index", new String[] {"a"});
    validateExpectedIndexes(region, expectedIndexes);

    // Validate expected serializer
    LuceneTestSerializer nestedSerializer = new LuceneTestSerializer();
    nestedSerializer.getProperties().setProperty("nested_param", "nested_value");
    Properties expected = new Properties();
    expected.put("param_from_xml", nestedSerializer);
    validateExpectedSerializer(region, expected);
  }

  @Test
  public void parseIndexWithSerializer() throws FileNotFoundException {
    RegionCreation region = createRegionCreation("region");

    // Validate expected indexes
    Map<String, String[]> expectedIndexes = new HashMap<>();
    expectedIndexes.put("index", new String[] {"a"});
    validateExpectedIndexes(region, expectedIndexes);

    // Validate expected serializer
    Properties expected = new Properties();
    validateExpectedSerializer(region, expected);
  }

  private RegionCreation createRegionCreation(String regionName) throws FileNotFoundException {
    CacheXmlParser parser = CacheXmlParser.parse(new FileInputStream(getXmlFileForTest()));
    CacheCreation cacheCreation = parser.getCacheCreation();
    // Some of the tests in this class needs to have the declarables initialized.
    // cacheCreation.create(InternalCache) would do this but it was too much work
    // to mock the InternalCache for all the ways create used it.
    // So instead we just call initializeDeclarablesMap which is also what create does.
    cacheCreation.initializeDeclarablesMap(mock(InternalCache.class));
    return (RegionCreation) cacheCreation.getRegion(regionName);
  }

  private void validateExpectedIndexes(RegionCreation region,
      Map<String, String[]> expectedIndexes) {
    for (Extension extension : region.getExtensionPoint().getExtensions()) {
      LuceneIndexCreation index = (LuceneIndexCreation) extension;
      assertEquals(SEPARATOR + "region", index.getRegionPath());
      assertArrayEquals(expectedIndexes.remove(index.getName()), index.getFieldNames());
    }
    assertEquals(Collections.emptyMap(), expectedIndexes);
  }

  private void validateExpectedAnalyzers(RegionCreation region,
      Map<String, Map<String, Class<? extends Analyzer>>> expectedIndexAnalyzers) {
    for (Extension extension : region.getExtensionPoint().getExtensions()) {
      LuceneIndexCreation index = (LuceneIndexCreation) extension;
      expectedIndexAnalyzers.remove(index.getName());
    }
    assertEquals(Collections.emptyMap(), expectedIndexAnalyzers);
  }

  private void validateExpectedSerializer(RegionCreation region, Properties expectedProps) {
    Extension extension = region.getExtensionPoint().getExtensions().iterator().next();
    LuceneIndexCreation index = (LuceneIndexCreation) extension;
    LuceneSerializer testSerializer = index.getLuceneSerializer();
    assertThat(testSerializer).isInstanceOf(LuceneTestSerializer.class);
    Properties p = ((LuceneTestSerializer) testSerializer).getProperties();
    assertEquals(expectedProps, p);
  }

  /**
   * Test that the Index creation objects get appropriately translated into a real index.
   *
   */
  @Test
  public void createIndex() throws FileNotFoundException {
    CacheFactory cf = new CacheFactory();
    cf.set(MCAST_PORT, "0");
    cf.set(CACHE_XML_FILE, getXmlFileForTest());
    Cache cache = cf.create();

    try {
      LuceneService service = LuceneServiceProvider.get(cache);
      assertEquals(3, service.getAllIndexes().size());
      LuceneIndex index1 = service.getIndex("index1", SEPARATOR + "region");
      LuceneIndex index2 = service.getIndex("index2", SEPARATOR + "region");
      LuceneIndex index3 = service.getIndex("index3", SEPARATOR + "region");
      assertArrayEquals(index1.getFieldNames(), new String[] {"a", "b", "c", "d"});
      assertArrayEquals(index2.getFieldNames(), new String[] {"f", "g"});
      assertArrayEquals(index3.getFieldNames(), new String[] {"h", "i", "j"});
    } finally {
      tearDownCacheAfterCreateIndexTestIsCompleted(cache);
    }
  }

  private String getXmlFileForTest() {
    return createTempFileFromResource(getClass(),
        getClass().getSimpleName() + "." + name.getMethodName() + ".cache.xml").getAbsolutePath();
  }

}
