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
    LuceneService service = LuceneServiceProvider.get(cache);
    service.createIndex("index", "region", "a", "b", "c");
    cache.createRegionFactory(RegionShortcut.PARTITION).create("region");
    
    
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
