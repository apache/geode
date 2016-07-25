/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal.xml;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.extension.Extension;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlParser;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionCreation;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.gemstone.gemfire.util.test.TestUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class LuceneIndexXmlParserIntegrationJUnitTest {

  @Rule
  public TestName name = new TestName();
  
  @After
  public void tearDown() {
    Cache cache = GemFireCacheImpl.getInstance();
    if(cache != null) {
      cache.close();
    }
  }
  
  /**
   * Test that we parse the index fields correctly
   */
  @Test
  public void parseIndex() throws FileNotFoundException {
    RegionCreation region = createRegionCreation("region");
    Map<String, String[]> expectedIndexes = new HashMap<String, String[]>();
    expectedIndexes.put("index1", new String[] {"a", "b", "c", "d"});
    expectedIndexes.put("index2", new String[] {"f", "g"});
    validateExpectedIndexes(region, expectedIndexes);
  }

  @Test
  public void parseIndexWithAnalyzers() throws FileNotFoundException {
    RegionCreation region = createRegionCreation("region");

    // Validate expected indexes
    Map<String, String[]> expectedIndexes = new HashMap<String, String[]>();
    expectedIndexes.put("index", new String[] {"a", "b", "c"});
    validateExpectedIndexes(region, expectedIndexes);

    // Validate expected analyzers
    Map<String, Map<String,Class<? extends Analyzer>>> expectedIndexAnalyzers = new HashMap<>();
    Map<String,Class<? extends Analyzer>> expectedFieldAnalyzers = new HashMap<>();
    expectedFieldAnalyzers.put("a", KeywordAnalyzer.class);
    expectedFieldAnalyzers.put("b", SimpleAnalyzer.class);
    expectedFieldAnalyzers.put("c", ClassicAnalyzer.class);
    expectedIndexAnalyzers.put("index", expectedFieldAnalyzers);
    validateExpectedAnalyzers(region, expectedIndexAnalyzers);
  }

  private RegionCreation createRegionCreation(String regionName) throws FileNotFoundException {
    CacheXmlParser parser = CacheXmlParser.parse(new FileInputStream(getXmlFileForTest()));
    CacheCreation cache = parser.getCacheCreation();
    return (RegionCreation) cache.getRegion(regionName);
  }

  private void validateExpectedIndexes(RegionCreation region, Map<String, String[]> expectedIndexes) {
    for(Extension extension : region.getExtensionPoint().getExtensions()) {
      LuceneIndexCreation index = (LuceneIndexCreation) extension;
      assertEquals("/region", index.getRegionPath());
      assertArrayEquals(expectedIndexes.remove(index.getName()), index.getFieldNames());
    }
    assertEquals(Collections.emptyMap(),expectedIndexes);
  }

  private void validateExpectedAnalyzers(RegionCreation region, Map<String, Map<String,Class<? extends Analyzer>>> expectedIndexAnalyzers) {
    for(Extension extension : region.getExtensionPoint().getExtensions()) {
      LuceneIndexCreation index = (LuceneIndexCreation) extension;
      expectedIndexAnalyzers.remove(index.getName());
    }
    assertEquals(Collections.emptyMap(),expectedIndexAnalyzers);
  }

  /**
   * Test that the Index creation objects get appropriately translated
   * into a real index.
   * @throws FileNotFoundException
   */
  @Test
  public void createIndex() throws FileNotFoundException {
    CacheFactory cf = new CacheFactory();
    cf.set(MCAST_PORT, "0");
    cf.set(CACHE_XML_FILE, getXmlFileForTest());
    Cache cache = cf.create();

    LuceneService service = LuceneServiceProvider.get(cache);
    assertEquals(3, service.getAllIndexes().size());
    LuceneIndex index1 = service.getIndex("index1", "/region");
    LuceneIndex index2 = service.getIndex("index2", "/region");
    LuceneIndex index3 = service.getIndex("index3", "/region");
    assertArrayEquals(index1.getFieldNames(), new String[] {"a", "b", "c", "d"});
    assertArrayEquals(index2.getFieldNames(), new String[] { "f", "g"});
    assertArrayEquals(index3.getFieldNames(), new String[] { "h", "i", "j"});
  }

  private String getXmlFileForTest() {
    return TestUtil.getResourcePath(getClass(), getClass().getSimpleName() + "." + name.getMethodName() + ".cache.xml");
  }

}
