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

package com.gemstone.gemfire.cache.lucene;

import static com.gemstone.gemfire.cache.RegionShortcut.*;
import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.*;
import static junitparams.JUnitParamsRunner.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexCreationProfile;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexFactory;
import com.gemstone.gemfire.cache.lucene.internal.LuceneRawIndex;
import com.gemstone.gemfire.cache.lucene.internal.LuceneRawIndexFactory;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities;
import com.gemstone.gemfire.cache.lucene.test.TestObject;
import com.gemstone.gemfire.internal.cache.BucketNotFoundException;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import com.jayway.awaitility.Awaitility;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

/**
 * Tests of creating lucene indexes on regions. All tests of index creation
 * use cases should be in classes starting with LuceneIndexCreation*. Most
 * tests belong in this class, except for:
 * <ul>
 * <li> Tests that use persistence are in {@link LuceneIndexCreationPersistenceIntegrationTest}  </li>
 * <li> Tests that use offheap are in {@link LuceneIndexCreationOffHeapIntegrationTest}  </li>
 * </ul>
 */
@Category(IntegrationTest.class)
@RunWith(JUnitParamsRunner.class)
public class LuceneIndexCreationIntegrationTest extends LuceneIntegrationTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldCreateIndexWriterWithAnalyzersWhenSettingPerFieldAnalyzers()
    throws BucketNotFoundException, InterruptedException
  {
    Map<String, Analyzer> analyzers = new HashMap<>();

    final RecordingAnalyzer field1Analyzer = new RecordingAnalyzer();
    final RecordingAnalyzer field2Analyzer = new RecordingAnalyzer();
    analyzers.put("field1", field1Analyzer);
    analyzers.put("field2", field2Analyzer);
    luceneService.createIndex(INDEX_NAME, REGION_NAME, analyzers);
    Region region = createRegion();
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    region.put("key1", new TestObject());
    verifyIndexFinishFlushing(cache, INDEX_NAME, REGION_NAME);
    assertEquals(analyzers, index.getFieldAnalyzers());
    assertEquals(Arrays.asList("field1"), field1Analyzer.analyzedfields);
    assertEquals(Arrays.asList("field2"), field2Analyzer.analyzedfields);
  }

  @Test
  @Parameters({"0", "1", "2"})
  public void shouldUseRedundancyForInternalRegionsWhenUserRegionHasRedundancy(int redundancy) {
    createIndex("text");
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundancy);

    cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT).setPartitionAttributes(paf.create()).create(REGION_NAME);
    verifyInternalRegions(region -> {
      assertEquals(redundancy, region.getAttributes().getPartitionAttributes().getRedundantCopies());
    });
  }

  @Test
  public void shouldNotUseIdleTimeoutForInternalRegionsWhenUserRegionHasIdleTimeout() {
    createIndex("text");
    cache.createRegionFactory(RegionShortcut.PARTITION)
      .setEntryIdleTimeout(new ExpirationAttributes(5))
      .create(REGION_NAME);

    verifyInternalRegions(region -> {
      assertEquals(0, region.getAttributes().getEntryIdleTimeout().getTimeout());
    });
  }

  @Test
  public void shouldNotUseTTLForInternalRegionsWhenUserRegionHasTTL() {
    createIndex("text");
    cache.createRegionFactory(RegionShortcut.PARTITION)
      .setEntryTimeToLive(new ExpirationAttributes(5))
      .create(REGION_NAME);

    verifyInternalRegions(region -> {
      assertEquals(0, region.getAttributes().getEntryTimeToLive().getTimeout());
    });
  }

  @Test
  public void shouldUseFixedPartitionsForInternalRegions() {
    createIndex("text");

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory<>();
    final FixedPartitionAttributes fixedAttributes = FixedPartitionAttributes.createFixedPartition("A", true, 1);
    partitionAttributesFactory.addFixedPartitionAttributes(fixedAttributes);
    cache.createRegionFactory(RegionShortcut.PARTITION)
      .setPartitionAttributes(partitionAttributesFactory.create())
      .create(REGION_NAME);

    verifyInternalRegions(region -> {
      //Fixed partitioned regions don't allow you to specify the partitions on the colocated region
      assertNull(region.getAttributes().getPartitionAttributes().getFixedPartitionAttributes());
      assertTrue(((PartitionedRegion) region).isFixedPartitionedRegion());
    });
  }

  @Test
  public void shouldCreateRawIndexIfSpecifiedItsFactory()
    throws BucketNotFoundException, InterruptedException
  {
    Map<String, Analyzer> analyzers = new HashMap<>();

    final RecordingAnalyzer field1Analyzer = new RecordingAnalyzer();
    final RecordingAnalyzer field2Analyzer = new RecordingAnalyzer();
    analyzers.put("field1", field1Analyzer);
    analyzers.put("field2", field2Analyzer);
    LuceneServiceImpl.luceneIndexFactory = new LuceneRawIndexFactory();
    try {
      luceneService.createIndex(INDEX_NAME, REGION_NAME, analyzers);
      Region region = createRegion();
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertTrue(index instanceof LuceneRawIndex);
      region.put("key1", new TestObject());
      verifyIndexFinishFlushing(cache, INDEX_NAME, REGION_NAME);
      assertEquals(analyzers, index.getFieldAnalyzers());
      assertEquals(Arrays.asList("field1"), field1Analyzer.analyzedfields);
      assertEquals(Arrays.asList("field2"), field2Analyzer.analyzedfields);
    } finally {
      LuceneServiceImpl.luceneIndexFactory = new LuceneIndexFactory();
    }
  }

  @Test(expected = IllegalStateException.class)
  public void cannotCreateLuceneIndexAfterRegionHasBeenCreated() throws IOException, ParseException {
    createRegion();
    createIndex("field1", "field2", "field3");
  }

  @Test
  public void cannotCreateLuceneIndexForReplicateRegion() throws IOException, ParseException {
    try {
      createIndex("field1", "field2", "field3");
      this.cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);
      fail("Should not have been able to create index");
    } catch (UnsupportedOperationException e) {
      assertEquals("Lucene indexes on replicated regions are not supported", e.getMessage());
      assertNull(cache.getRegion(REGION_NAME));
    }
  }

  @Test
  public void cannotCreateLuceneIndexForRegionWithEviction() throws IOException, ParseException {
    try {
      createIndex("field1", "field2", "field3");
      RegionFactory regionFactory = this.cache.createRegionFactory(RegionShortcut.PARTITION);
      regionFactory.setEvictionAttributes(
        EvictionAttributes.createLIFOEntryAttributes(100, EvictionAction.LOCAL_DESTROY));
      regionFactory.create(REGION_NAME);
    } catch (UnsupportedOperationException e) {
      assertEquals("Lucene indexes on regions with eviction and action local destroy are not supported", e.getMessage());
      assertNull(cache.getRegion(REGION_NAME));
    }
  }

  @Test
  public void cannotCreateLuceneIndexWithExistingIndexName()
  {
    expectedException.expect(IllegalArgumentException.class);
    createIndex("field1","field2","field3");
    createIndex("field4","field5","field6");
  }

  @Test
  public void shouldReturnAllDefinedIndexes()
  {
    LuceneServiceImpl luceneServiceImpl = (LuceneServiceImpl) luceneService;
    luceneServiceImpl.createIndex(INDEX_NAME,REGION_NAME,"field1","field2","field3");
    luceneServiceImpl.createIndex("index2", "region2", "field4","field5","field6");
    final Collection<LuceneIndexCreationProfile> indexList = luceneServiceImpl.getAllDefinedIndexes();

    assertEquals(Arrays.asList(INDEX_NAME,"index2"), indexList.stream().map(LuceneIndexCreationProfile::getIndexName)
                                                              .sorted().collect(Collectors.toList()));
    createRegion();
    assertEquals(Collections.singletonList("index2"), indexList.stream().map(LuceneIndexCreationProfile::getIndexName)
                                                    .collect(Collectors.toList()));
  }

  @Test
  public void shouldRemoveDefinedIndexIfRegionAlreadyExists()
  {
    try {
      createRegion();
      createIndex("field1", "field2", "field3");

    } catch(IllegalStateException e) {
      assertEquals("The lucene index must be created before region", e.getMessage());
      assertEquals(0,((LuceneServiceImpl) luceneService).getAllDefinedIndexes().size());
    }
  }
  private void verifyInternalRegions(Consumer<LocalRegion> verify) {
    LuceneTestUtilities.verifyInternalRegions(luceneService, cache, verify);
  }

  private Region createRegion() {
    return createRegion(REGION_NAME, RegionShortcut.PARTITION);
  }

  private void createIndex(String ... fieldNames) {
    LuceneTestUtilities.createIndex(cache, fieldNames);
  }

  private static class RecordingAnalyzer extends Analyzer {

    private List<String> analyzedfields = new ArrayList<String>();

    @Override protected TokenStreamComponents createComponents(final String fieldName) {
      analyzedfields.add(fieldName);
      return new TokenStreamComponents(new KeywordTokenizer());
    }
  }
}
