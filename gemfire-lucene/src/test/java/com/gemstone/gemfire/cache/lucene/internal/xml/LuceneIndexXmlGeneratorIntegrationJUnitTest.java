package com.gemstone.gemfire.cache.lucene.internal.xml;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class LuceneIndexXmlGeneratorIntegrationJUnitTest {
  
  /**
   * Test of generating and reading cache configuration back in.
   */
  @Test
  public void generateWithFields() {
    Cache cache = new CacheFactory().set("mcast-port", "0").create();
    cache.createRegionFactory(RegionShortcut.PARTITION).create("region");
    LuceneService service = LuceneServiceProvider.get(cache);
    
    service.createIndex("index", "region", "a", "b", "c");
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter pw = new PrintWriter(baos);
    CacheXmlGenerator.generate(cache, pw, true, false, false);
    pw.flush();
    
    cache.close();
    cache = new CacheFactory().set("mcast-port", "0").create();
    
    byte[] bytes = baos.toByteArray();
    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    System.out.println("---FILE---");
    System.out.println(new String(bytes, Charset.defaultCharset()));
    cache.loadCacheXml(new ByteArrayInputStream(bytes));
    
    LuceneService service2 = LuceneServiceProvider.get(cache);
    assertTrue(service != service2);
    
    LuceneIndex index = service2.getIndex("index", "region");
    assertNotNull(index);
    
    assertArrayEquals(new String[] {"a", "b", "c"}, index.getFieldNames());
  }

}
