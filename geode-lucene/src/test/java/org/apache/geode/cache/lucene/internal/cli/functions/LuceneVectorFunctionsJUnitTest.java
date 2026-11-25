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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneQuery;
import org.apache.geode.cache.lucene.LuceneQueryException;
import org.apache.geode.cache.lucene.LuceneQueryFactory;
import org.apache.geode.cache.lucene.LuceneQueryProvider;
import org.apache.geode.cache.lucene.LuceneResultStruct;
import org.apache.geode.cache.lucene.PageableLuceneQueryResults;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
import org.apache.geode.cache.lucene.internal.InternalLuceneService;
import org.apache.geode.cache.lucene.internal.LuceneIndexFactoryImpl;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneResultStructImpl;
import org.apache.geode.cache.lucene.internal.VectorKnnQueryProvider;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.cache.lucene.internal.cli.LuceneQueryInfo;
import org.apache.geode.cache.lucene.internal.cli.LuceneSearchResults;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.fake.Fakes;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class LuceneVectorFunctionsJUnitTest {

  @Test
  @SuppressWarnings("unchecked")
  public void createIndexFunctionRegistersVectorFieldsOnFactory() {
    GemFireCacheImpl cache = Fakes.cache();
    InternalLuceneService service = mock(InternalLuceneService.class);
    LuceneIndexFactoryImpl factory = mock(LuceneIndexFactoryImpl.class);
    when(cache.getService(InternalLuceneService.class)).thenReturn(service);
    when(service.createIndexFactory()).thenReturn(factory);

    VectorIndexMetadata meta =
        new VectorIndexMetadata("embedding", 128, VectorSimilarityFunction.COSINE, true);
    LuceneIndexInfo info = new LuceneIndexInfo("vecIdx", SEPARATOR + "docs", new String[0], null,
        null, Collections.singletonMap("embedding", meta));

    FunctionContext context = mock(FunctionContext.class);
    ResultSender resultSender = mock(ResultSender.class);
    when(context.getCache()).thenReturn(cache);
    when(context.getArguments()).thenReturn(info);
    when(context.getResultSender()).thenReturn(resultSender);

    new LuceneCreateIndexFunction().execute(context);

    verify(factory).addVectorField(eq(meta));
    verify(factory).create(eq("vecIdx"), eq(SEPARATOR + "docs"), eq(false));

    ArgumentCaptor<CliFunctionResult> resultCaptor =
        ArgumentCaptor.forClass(CliFunctionResult.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    assertThat(resultCaptor.getValue().isSuccessful()).isTrue();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void searchIndexFunctionExecutesVectorKnnQueryProvider() throws Exception {
    GemFireCacheImpl cache = Fakes.cache();
    InternalLuceneService service = mock(InternalLuceneService.class);
    LuceneQueryFactory queryFactory = mock(LuceneQueryFactory.class);
    LuceneQuery<String, String> luceneQuery = mock(LuceneQuery.class);
    PageableLuceneQueryResults<String, String> pages = mock(PageableLuceneQueryResults.class);

    when(cache.getService(InternalLuceneService.class)).thenReturn(service);
    when(service.getIndex("vecIdx", "/docs")).thenReturn(mock(LuceneIndex.class));
    when(service.createLuceneQueryFactory()).thenReturn(queryFactory);
    when(queryFactory.setLimit(anyInt())).thenReturn(queryFactory);
    when(queryFactory.create(eq("vecIdx"), eq("/docs"), any(LuceneQueryProvider.class)))
        .thenReturn((LuceneQuery) luceneQuery);
    when(luceneQuery.findPages()).thenReturn(pages);
    when(pages.hasNext()).thenReturn(true, false);
    List<LuceneResultStruct<String, String>> page =
        Collections.singletonList(new LuceneResultStructImpl<>("doc-1", "payload", 0.99f));
    when(pages.next()).thenReturn(page);

    LuceneQueryInfo queryInfo = new LuceneQueryInfo("vecIdx", "/docs", null, null, 5, false,
        "embedding", new float[] {1.0f, 0.0f, 0.0f}, 5, "category:tech");

    FunctionContext context = mock(FunctionContext.class);
    ResultSender resultSender = mock(ResultSender.class);
    when(context.getCache()).thenReturn(cache);
    when(context.getArguments()).thenReturn(queryInfo);
    when(context.getResultSender()).thenReturn(resultSender);

    new LuceneSearchIndexFunction<>().execute(context);

    ArgumentCaptor<LuceneQueryProvider> providerCaptor =
        ArgumentCaptor.forClass(LuceneQueryProvider.class);
    verify(queryFactory).setLimit(5);
    verify(queryFactory).create(eq("vecIdx"), eq("/docs"), providerCaptor.capture());
    assertThat(providerCaptor.getValue()).isInstanceOf(VectorKnnQueryProvider.class);
    VectorKnnQueryProvider provider = (VectorKnnQueryProvider) providerCaptor.getValue();
    assertThat(provider.getVectorField()).isEqualTo("embedding");
    assertThat(provider.getQueryVector()).containsExactly(1.0f, 0.0f, 0.0f);
    assertThat(provider.getK()).isEqualTo(5);
    assertThat(provider.getFilterQuery()).isEqualTo("category:tech");

    ArgumentCaptor<Set> resultCaptor = ArgumentCaptor.forClass(Set.class);
    verify(resultSender).lastResult(resultCaptor.capture());
    assertThat(resultCaptor.getValue()).hasSize(1);
    LuceneSearchResults hit =
        (LuceneSearchResults) resultCaptor.getValue().iterator().next();
    assertThat(hit.getKey()).isEqualTo("doc-1");
    assertThat(hit.getScore()).isEqualTo(0.99f);
  }

  @Test
  public void vectorKnnQueryProviderBuildsFilteredAndUnfilteredKnnQueries() throws Exception {
    LuceneIndexImpl index = mock(LuceneIndexImpl.class);
    when(index.getName()).thenReturn("vecIdx");
    when(index.getAnalyzer()).thenReturn(new StandardAnalyzer());
    when(index.getFieldNames()).thenReturn(new String[] {"category"});
    when(index.getVectorFields()).thenReturn(Collections.singletonMap("embedding",
        new VectorIndexMetadata("embedding", 3, VectorSimilarityFunction.COSINE, true)));

    // Auto-resolve sole vector field + unfiltered
    VectorKnnQueryProvider unfiltered =
        new VectorKnnQueryProvider(null, new float[] {1.0f, 0.0f, 0.0f}, 10, null, null);
    Query q1 = unfiltered.getQuery(index);
    assertThat(q1).isInstanceOf(KnnFloatVectorQuery.class);
    KnnFloatVectorQuery knn1 = (KnnFloatVectorQuery) q1;
    assertThat(knn1.getField()).isEqualTo("embedding");
    assertThat(knn1.getK()).isEqualTo(10);
    assertThat(knn1.getFilter()).isNull();

    // Explicit vector field + pre-filter predicate
    VectorKnnQueryProvider filtered =
        new VectorKnnQueryProvider("embedding", new float[] {0.0f, 1.0f, 0.0f}, 5,
            "category:finance", null);
    Query q2 = filtered.getQuery(index);
    assertThat(q2).isInstanceOf(KnnFloatVectorQuery.class);
    KnnFloatVectorQuery knn2 = (KnnFloatVectorQuery) q2;
    assertThat(knn2.getFilter()).isNotNull();
    assertThat(knn2.getFilter().toString()).contains("category:finance");
  }

  @Test
  public void vectorKnnQueryProviderRejectsDimensionMismatchAndAmbiguousField() {
    LuceneIndexImpl index = mock(LuceneIndexImpl.class);
    when(index.getName()).thenReturn("multiVec");
    Map<String, VectorIndexMetadata> vectors = new LinkedHashMap<>();
    vectors.put("v1", new VectorIndexMetadata("v1", 4, VectorSimilarityFunction.COSINE, false));
    vectors.put("v2", new VectorIndexMetadata("v2", 4, VectorSimilarityFunction.EUCLIDEAN, true));
    when(index.getVectorFields()).thenReturn(vectors);

    assertThatThrownBy(() -> new VectorKnnQueryProvider(null, new float[] {1, 0, 0, 0}, 5, null,
        null).getQuery(index))
            .isInstanceOf(LuceneQueryException.class)
            .hasMessageContaining("Multiple vector fields");

    assertThatThrownBy(() -> new VectorKnnQueryProvider("v1", new float[] {1, 0}, 5, null, null)
        .getQuery(index))
            .isInstanceOf(LuceneQueryException.class)
            .hasMessageContaining("dimension mismatch");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void searchIndexFunctionRespectsExplicitLimitDifferentFromK() throws Exception {
    GemFireCacheImpl cache = Fakes.cache();
    InternalLuceneService service = mock(InternalLuceneService.class);
    LuceneQueryFactory queryFactory = mock(LuceneQueryFactory.class);
    LuceneQuery<String, String> luceneQuery = mock(LuceneQuery.class);
    PageableLuceneQueryResults<String, String> pages = mock(PageableLuceneQueryResults.class);

    when(cache.getService(InternalLuceneService.class)).thenReturn(service);
    when(service.getIndex("vecIdx", "/docs")).thenReturn(mock(LuceneIndex.class));
    when(service.createLuceneQueryFactory()).thenReturn(queryFactory);
    when(queryFactory.setLimit(anyInt())).thenReturn(queryFactory);
    when(queryFactory.create(eq("vecIdx"), eq("/docs"), any(LuceneQueryProvider.class)))
        .thenReturn((LuceneQuery) luceneQuery);
    when(luceneQuery.findPages()).thenReturn(pages);
    when(pages.hasNext()).thenReturn(false);

    LuceneQueryInfo queryInfo = new LuceneQueryInfo("vecIdx", "/docs", null, null, 2, false,
        "embedding", new float[] {1.0f, 0.0f}, 5, null);

    FunctionContext context = mock(FunctionContext.class);
    ResultSender resultSender = mock(ResultSender.class);
    when(context.getCache()).thenReturn(cache);
    when(context.getArguments()).thenReturn(queryInfo);
    when(context.getResultSender()).thenReturn(resultSender);

    new LuceneSearchIndexFunction<>().execute(context);

    ArgumentCaptor<LuceneQueryProvider> providerCaptor =
        ArgumentCaptor.forClass(LuceneQueryProvider.class);
    verify(queryFactory).setLimit(2);
    verify(queryFactory).create(eq("vecIdx"), eq("/docs"), providerCaptor.capture());
    assertThat(((VectorKnnQueryProvider) providerCaptor.getValue()).getK()).isEqualTo(5);
  }

  @Test
  public void vectorKnnQueryProviderFailsFastOnUnqualifiedFilterForPureVectorIndex()
      throws Exception {
    LuceneIndex pureVecIndex = mock(LuceneIndex.class);
    when(pureVecIndex.getName()).thenReturn("pureVec");
    when(pureVecIndex.getFieldNames()).thenReturn(new String[0]);
    when(pureVecIndex.getVectorFields()).thenReturn(Collections.singletonMap("embedding",
        new VectorIndexMetadata("embedding", 2, VectorSimilarityFunction.COSINE, false)));

    // Unqualified filter on pure-vector index must fail fast
    assertThatThrownBy(() -> new VectorKnnQueryProvider("embedding", new float[] {1.0f, 0.0f}, 5,
        "books", null).getQuery(pureVecIndex))
            .isInstanceOf(LuceneQueryException.class)
            .hasMessageContaining("unqualified");

    // Field-qualified filter on pure-vector index succeeds (and closes fallback analyzer)
    Query qualified = new VectorKnnQueryProvider("embedding", new float[] {1.0f, 0.0f}, 5,
        "category:books", null).getQuery(pureVecIndex);
    assertThat(qualified).isInstanceOf(KnnFloatVectorQuery.class);
    assertThat(((KnnFloatVectorQuery) qualified).getFilter().toString()).contains("category:books");
  }
}
