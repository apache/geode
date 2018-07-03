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
package org.apache.geode.cache.lucene.internal.cli.functions;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.LuceneIndexForPartitionedRegion;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexDetails;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexStatus;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})

public class LuceneListIndexFunctionJUnitTest {


  @Test
  @SuppressWarnings("unchecked")
  public void executeListLuceneIndexWhenReindexingInProgress() {
    GemFireCacheImpl cache = Fakes.cache();
    final String serverName = "mockedServer";
    LuceneServiceImpl service = mock(LuceneServiceImpl.class);
    when(cache.getService(InternalLuceneService.class)).thenReturn(service);

    FunctionContext context = mock(FunctionContext.class);
    ResultSender resultSender = mock(ResultSender.class);
    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);

    LuceneIndexForPartitionedRegion index1 = getMockLuceneIndex("index1");

    PartitionedRegion userRegion = mock(PartitionedRegion.class);
    when(cache.getRegion(index1.getRegionPath())).thenReturn(userRegion);

    PartitionedRegionDataStore userRegionDataStore = mock(PartitionedRegionDataStore.class);
    when(userRegion.getDataStore()).thenReturn(userRegionDataStore);

    BucketRegion userBucket = mock(BucketRegion.class);
    when(userRegionDataStore.getLocalBucketById(1)).thenReturn(userBucket);

    when(userBucket.isEmpty()).thenReturn(false);

    ArrayList<LuceneIndex> allIndexes = new ArrayList();
    allIndexes.add(index1);
    when(service.getAllIndexes()).thenReturn(allIndexes);

    PartitionedRegion mockFileRegion = mock(PartitionedRegion.class);
    when(index1.getFileAndChunkRegion()).thenReturn(mockFileRegion);

    PartitionedRegionDataStore mockPartitionedRegionDataStore =
        mock(PartitionedRegionDataStore.class);
    when(mockFileRegion.getDataStore()).thenReturn(mockPartitionedRegionDataStore);

    Set<Integer> bucketSet = new HashSet<>();
    bucketSet.add(1);

    when(mockPartitionedRegionDataStore.getAllLocalPrimaryBucketIds()).thenReturn(bucketSet);

    when(index1.isIndexAvailable(1)).thenReturn(false);

    LuceneListIndexFunction function = new LuceneListIndexFunction();
    function.execute(context);

    ArgumentCaptor<Set> resultCaptor = ArgumentCaptor.forClass(Set.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    Set<String> result = resultCaptor.getValue();

    TreeSet expectedResult = new TreeSet();
    expectedResult
        .add(new LuceneIndexDetails(index1, serverName, LuceneIndexStatus.INDEXING_IN_PROGRESS));

    assertEquals(1, result.size());
    assertEquals(expectedResult, result);

  }

  private LuceneIndexForPartitionedRegion getMockLuceneIndex(final String indexName) {
    LuceneIndexForPartitionedRegion index = mock(LuceneIndexForPartitionedRegion.class);
    String[] searchableFields = {"field1", "field2"};
    Map<String, Analyzer> fieldAnalyzers = new HashMap<>();
    fieldAnalyzers.put("field1", new StandardAnalyzer());
    fieldAnalyzers.put("field2", new KeywordAnalyzer());
    when(index.getName()).thenReturn(indexName);
    when(index.getRegionPath()).thenReturn("/region");
    when(index.getFieldNames()).thenReturn(searchableFields);
    when(index.getFieldAnalyzers()).thenReturn(fieldAnalyzers);
    return index;
  }

}
