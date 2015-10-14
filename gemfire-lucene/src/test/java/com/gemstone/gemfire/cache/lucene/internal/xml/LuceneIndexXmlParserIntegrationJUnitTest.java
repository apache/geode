package com.gemstone.gemfire.cache.lucene.internal.xml;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

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
    CacheXmlParser parser = CacheXmlParser.parse(new FileInputStream(getXmlFileForTest()));
    CacheCreation cache = parser.getCacheCreation();
    RegionCreation region = (RegionCreation) cache.getRegion("region");
    Map<String, String[]> expectedIndexes = new HashMap<String, String[]>();
    expectedIndexes.put("index1", new String[] {"a", "b", "c", "d"});
    expectedIndexes.put("index2", new String[] { "f", "g"});
    for(Extension extension : region.getExtensionPoint().getExtensions()) {
      LuceneIndexCreation index = (LuceneIndexCreation) extension;
      assertEquals("/region", index.getRegionPath());
      assertArrayEquals(expectedIndexes.remove(index.getName()), index.getFieldNames());
    }
    
    assertEquals(Collections.emptyMap(),expectedIndexes);
  }

  /**
   * Test that the Index creation objects get appropriately translated
   * into a real index.
   * @throws FileNotFoundException
   */
  @Test
  public void createIndex() throws FileNotFoundException {
    CacheFactory cf = new CacheFactory();
    cf.set("mcast-port", "0");
    cf.set("cache-xml-file", getXmlFileForTest());
    Cache cache = cf.create();

    LuceneService service = LuceneServiceProvider.get(cache);
    assertEquals(2, service.getAllIndexes().size());
    LuceneIndex index1 = service.getIndex("index1", "/region");
    LuceneIndex index2 = service.getIndex("index2", "/region");
    assertArrayEquals(index1.getFieldNames(), new String[] {"a", "b", "c", "d"});
    assertArrayEquals(index2.getFieldNames(), new String[] { "f", "g"});
  }

  private String getXmlFileForTest() {
    return TestUtil.getResourcePath(getClass(), getClass().getSimpleName() + "." + name.getMethodName() + ".cache.xml");
  }

}
