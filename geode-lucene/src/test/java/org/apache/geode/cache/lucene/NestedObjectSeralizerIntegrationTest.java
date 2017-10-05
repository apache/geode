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
package org.apache.geode.cache.lucene;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.lucene.test.Customer;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class NestedObjectSeralizerIntegrationTest extends LuceneIntegrationTest {

  private static int WAIT_FOR_FLUSH_TIME = 10000;
  private static final Logger logger = LogService.getLogger();
  LuceneQuery<Integer, Customer> query;
  PageableLuceneQueryResults<Integer, Customer> results;

  private Region createRegionAndIndex() {
    luceneService.createIndexFactory().setLuceneSerializer(new FlatFormatSerializer())
        .addField("name").addField("contact.name").addField("contact.email", new KeywordAnalyzer())
        .addField("contact.address").addField("contact.homepage.content")
        .addField("contact.homepage.id").addField(LuceneService.REGION_VALUE_FIELD)
        .create(INDEX_NAME, REGION_NAME);

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    return region;
  }

  private Region createRegionAndIndexOnInvalidFields() {
    luceneService.createIndexFactory().setLuceneSerializer(new FlatFormatSerializer())
        .addField("name").addField("contact").addField("contact.page").addField("contact.missing")
        .addField("missing2").create(INDEX_NAME, REGION_NAME);

    Region region = createRegion(REGION_NAME, RegionShortcut.PARTITION);
    return region;
  }

  private void feedSomeNestedObjects(Region region) throws InterruptedException {
    region.put("object-13", new Customer("Tommy Jackson", "Tommi Jackson", 13));
    region.put("object-14", new Customer("Johnny Jackson", "Johnni Jackson", 14));
    region.put("object-15", new Customer("Johnny Jackson2", "Johnni Jackson2", 15));
    region.put("object-16", new Customer("Johnny Jackson21", "Johnni Jackson21", 16));
    region.put("key-1", "region value 1");
    region.put("key-2", "region value 2");
    region.put("key-3", "region value 3");
    region.put("key-4", "region value 4");

    LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    luceneService.waitUntilFlushed(INDEX_NAME, REGION_NAME, WAIT_FOR_FLUSH_TIME,
        TimeUnit.MILLISECONDS);
  }

  @Test
  public void queryOnContactNameWithExpression() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "contact.name:jackson2*", "name");
    results = query.findPages();
    assertEquals(2, results.size());
    printResults(results);
  }

  @Test
  public void queryOnContactNameWithExactMath() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "\"Johnni Jackson\"", "contact.name");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  @Test
  public void queryOnNameWithWrongValue() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "\"Johnni Jackson\"", "name");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnNameWithExactMatch() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "\"Johnny Jackson\"", "name");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  @Test
  public void queryOnContactEmailWithAnalyzer() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    // query-3: contact.email with KeywordAnalyzer
    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "Johnni.Jackson2@pivotal.io", "contact.email");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  @Test
  public void queryOnNonExistEmailField() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "Johnni.Jackson2@pivotal.io", "email");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnContactAddressWithStandardAnalyzer()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "97006",
        "contact.address");
    results = query.findPages();
    assertEquals(4, results.size());
    printResults(results);
  }

  @Test
  public void queryOnNonExistAddressField() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "97006",
        "address");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnThreeLayerField() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "contact.homepage.content:Hello", "name");
    results = query.findPages();
    printResults(results);
    assertEquals(4, results.size());
  }

  @Test
  public void queryOnThirdLayerFieldDirectlyShouldNotGetResult()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "Hello",
        "content");
    results = query.findPages();
    printResults(results);
  }

  @Test
  public void queryOnRegionValueField() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndex();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "region",
        LuceneService.REGION_VALUE_FIELD);
    results = query.findPages();
    printResults(results);
    assertEquals(4, results.size());
  }

  @Test
  public void nonExistFieldsShouldBeIgnored() throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    LuceneQuery query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "Jackson2*", "name");
    PageableLuceneQueryResults<Integer, Customer> results = query.findPages();
    assertEquals(2, results.size());
    printResults(results);
  }

  @Test
  public void queryOnNotIndexedFieldShouldReturnNothing()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "\"Johnni Jackson\"", "contact.name");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryWithExactMatchWhileIndexOnSomeWrongFields()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "\"Johnny Jackson\"", "name");
    results = query.findPages();
    assertEquals(1, results.size());
    printResults(results);
  }

  @Test
  public void queryOnNotIndexedFieldWithAnalyzerShouldReturnNothing()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "Johnni.Jackson2@pivotal.io", "contact.email");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnNotIndexedContactAddressFieldShouldReturnNothing()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "97006",
        "contact.address");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnNotIndexedThreeLayerFieldShouldReturnNothing()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME,
        "contact.homepage.content:Hello", "name");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnNotExistSecondLevelFieldShouldReturnNothing()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    query = luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "*",
        "contact.missing");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  @Test
  public void queryOnNotExistTopLevelFieldShouldReturnNothing()
      throws InterruptedException, LuceneQueryException {
    Region region = createRegionAndIndexOnInvalidFields();
    feedSomeNestedObjects(region);

    query =
        luceneService.createLuceneQueryFactory().create(INDEX_NAME, REGION_NAME, "*", "missing2");
    results = query.findPages();
    assertEquals(0, results.size());
  }

  private void printResults(PageableLuceneQueryResults<Integer, Customer> results) {
    if (results.size() > 0) {
      while (results.hasNext()) {
        results.next().stream().forEach(struct -> {
          logger.info("Result is:" + struct.getValue());
        });
      }
    }
  }

}
