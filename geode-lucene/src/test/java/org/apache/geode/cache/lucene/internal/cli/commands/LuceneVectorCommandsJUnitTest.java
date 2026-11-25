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
package org.apache.geode.cache.lucene.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.lucene.VectorIndexMetadata;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexDetails;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexInfo;
import org.apache.geode.cache.lucene.internal.cli.LuceneIndexStatus;
import org.apache.geode.cache.lucene.internal.cli.LuceneQueryInfo;
import org.apache.geode.cache.lucene.internal.cli.LuceneSearchResults;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneCreateIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneDescribeIndexFunction;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(LuceneTest.class)
public class LuceneVectorCommandsJUnitTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private InternalCache mockCache;

  @Before
  public void setUp() {
    mockCache = mock(InternalCache.class);
    when(mockCache.getSecurityService()).thenReturn(mock(SecurityService.class));
  }

  @Test
  public void createVectorOnlyIndexViaGfshParser() throws Exception {
    LuceneCreateIndexCommand command = spy(new LuceneCreateIndexCommand());
    command.setCache(mockCache);
    ResultCollector<?, ?> rc = mock(ResultCollector.class);
    List<CliFunctionResult> results = Collections.singletonList(
        new CliFunctionResult("server1", CliFunctionResult.StatusState.OK, "Created"));
    doReturn(rc).when(command).executeFunctionOnAllMembers(any(LuceneCreateIndexFunction.class),
        any(LuceneIndexInfo.class));
    doReturn(results).when(rc).getResult();

    gfsh.executeAndAssertThat(command,
        "create lucene index --name=vecIdx --region=/docs --vector-field=embedding "
            + "--vector-dim=128 --vector-similarity=COSINE --vector-sq8=true")
        .statusIsSuccess();

    ArgumentCaptor<LuceneIndexInfo> captor = ArgumentCaptor.forClass(LuceneIndexInfo.class);
    verify(command).executeFunctionOnAllMembers(any(LuceneCreateIndexFunction.class),
        captor.capture());
    LuceneIndexInfo info = captor.getValue();
    assertThat(info.getIndexName()).isEqualTo("vecIdx");
    assertThat(info.getRegionPath()).isEqualTo("/docs");
    assertThat(info.getSearchableFieldNames()).isEmpty();
    assertThat(info.getVectorFields()).containsOnlyKeys("embedding");
    assertThat(info.getVectorFields().get("embedding")).isEqualTo(
        new VectorIndexMetadata("embedding", 128, VectorSimilarityFunction.COSINE, true));
  }

  @Test
  public void createHybridTextAndMultiVectorIndexViaGfshParser() throws Exception {
    LuceneCreateIndexCommand command = spy(new LuceneCreateIndexCommand());
    command.setCache(mockCache);
    ResultCollector<?, ?> rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunctionOnAllMembers(any(LuceneCreateIndexFunction.class),
        any(LuceneIndexInfo.class));
    doReturn(Collections.singletonList(
        new CliFunctionResult("server1", CliFunctionResult.StatusState.OK, "Created")))
            .when(rc).getResult();

    gfsh.executeAndAssertThat(command,
        "create lucene index --name=hybridIdx --region=/docs --field=category,title "
            + "--vector-field=emb1,emb2 --vector-dim=128,64 "
            + "--vector-similarity=COSINE,EUCLIDEAN --vector-sq8=true,false")
        .statusIsSuccess();

    ArgumentCaptor<LuceneIndexInfo> captor = ArgumentCaptor.forClass(LuceneIndexInfo.class);
    verify(command).executeFunctionOnAllMembers(any(LuceneCreateIndexFunction.class),
        captor.capture());
    LuceneIndexInfo info = captor.getValue();
    assertThat(info.getSearchableFieldNames()).containsExactly("category", "title");
    assertThat(info.getVectorFields()).containsKeys("emb1", "emb2");
    assertThat(info.getVectorFields().get("emb1")).isEqualTo(
        new VectorIndexMetadata("emb1", 128, VectorSimilarityFunction.COSINE, true));
    assertThat(info.getVectorFields().get("emb2")).isEqualTo(
        new VectorIndexMetadata("emb2", 64, VectorSimilarityFunction.EUCLIDEAN, false));
  }

  @Test
  public void createIndexRejectsInvalidVectorOptions() {
    LuceneCreateIndexCommand command = spy(new LuceneCreateIndexCommand());
    command.setCache(mockCache);

    gfsh.executeAndAssertThat(command, "create lucene index --name=bad --region=/docs")
        .statusIsError()
        .containsOutput("At least one --field or --vector-field");

    gfsh.executeAndAssertThat(command,
        "create lucene index --name=bad --region=/docs --vector-field=emb")
        .statusIsError()
        .containsOutput("--vector-dim");

    gfsh.executeAndAssertThat(command,
        "create lucene index --name=bad --region=/docs --vector-field=emb --vector-dim=0")
        .statusIsError()
        .containsOutput("dimension");

    gfsh.executeAndAssertThat(command,
        "create lucene index --name=bad --region=/docs --vector-field=emb --vector-dim=16 "
            + "--vector-similarity=INVALID")
        .statusIsError()
        .containsOutput("--vector-similarity");

    gfsh.executeAndAssertThat(command,
        "create lucene index --name=bad --region=/docs --vector-field=emb --vector-dim=16 "
            + "--vector-sq8=maybe")
        .statusIsError()
        .containsOutput("--vector-sq8");

    // Duplicate --vector-field names
    gfsh.executeAndAssertThat(command,
        "create lucene index --name=bad --region=/docs --vector-field=emb,emb --vector-dim=4,8")
        .statusIsError()
        .containsOutput("Duplicate --vector-field");

    // Overlapping --field and --vector-field names
    gfsh.executeAndAssertThat(command,
        "create lucene index --name=bad --region=/docs --field=emb "
            + "--vector-field=emb --vector-dim=4")
        .statusIsError()
        .containsOutput("both --field and --vector-field");
  }

  @Test
  public void describeIndexDisplaysVectorFieldsAndSegmentBytes() throws Exception {
    LuceneDescribeIndexCommand command = spy(new LuceneDescribeIndexCommand());
    command.setCache(mockCache);

    LuceneIndexStats stats = mock(LuceneIndexStats.class);
    when(stats.getQueryExecutions()).thenReturn(5);
    when(stats.getUpdates()).thenReturn(100);
    when(stats.getCommits()).thenReturn(2);
    when(stats.getDocuments()).thenReturn(100);
    when(stats.getSegmentMemoryBytes()).thenReturn(65536L);

    Map<String, VectorIndexMetadata> vectors = new LinkedHashMap<>();
    vectors.put("embedding",
        new VectorIndexMetadata("embedding", 128, VectorSimilarityFunction.COSINE, 16, 100, true));

    LuceneIndexDetails details = new LuceneIndexDetails("vecIdx", "/docs",
        new String[] {"category"}, null, vectors, stats, LuceneIndexStatus.INITIALIZED, "server1",
        null);

    ResultCollector<?, ?> rc = mock(ResultCollector.class);
    doReturn(rc).when(command).executeFunctionOnRegion(any(LuceneDescribeIndexFunction.class),
        any(LuceneIndexInfo.class), eq(true));
    doReturn(Collections.singletonList(details)).when(rc).getResult();

    ResultModel result = command.describeIndex("vecIdx", "/docs");
    TabularResultModel table = result.getTableSection("lucene-indexes");
    assertThat(table.getValuesInColumn("Vector Fields")).hasSize(1);
    assertThat(table.getValuesInColumn("Vector Fields").get(0))
        .contains("embedding")
        .contains("dim=128")
        .contains("COSINE")
        .contains("sq8=true");
    assertThat(table.getValuesInColumn("Segment Bytes")).containsExactly("65536");
  }

  @Test
  public void searchIndexParsesVectorQueryOptionsViaGfshParser() throws Exception {
    LuceneSearchIndexCommand command = spy(new LuceneSearchIndexCommand());
    command.setCache(mockCache);

    ResultCollector<?, ?> rc = mock(ResultCollector.class);
    Set<LuceneSearchResults> hitSet =
        Collections.singleton(new LuceneSearchResults("k1", "v1", 0.95f));
    doReturn(rc).when(command).executeSearch(any(LuceneQueryInfo.class));
    doReturn(Collections.singletonList(hitSet)).when(rc).getResult();

    gfsh.executeAndAssertThat(command,
        "search lucene --name=vecIdx --region=/docs --vector-field=embedding "
            + "--query-vector=0.5,0.5,0.5,0.5 --k=5 --filter-query=category:news")
        .statusIsSuccess();

    ArgumentCaptor<LuceneQueryInfo> captor = ArgumentCaptor.forClass(LuceneQueryInfo.class);
    verify(command).executeSearch(captor.capture());
    LuceneQueryInfo info = captor.getValue();
    assertThat(info.isVectorQuery()).isTrue();
    assertThat(info.getVectorField()).isEqualTo("embedding");
    assertThat(info.getQueryVector()).containsExactly(0.5f, 0.5f, 0.5f, 0.5f);
    assertThat(info.getK()).isEqualTo(5);
    assertThat(info.getEffectiveK()).isEqualTo(5);
    assertThat(info.getFilterQuery()).isEqualTo("category:news");
  }

  @Test
  public void searchIndexRejectsInvalidVectorQueryArguments() {
    LuceneSearchIndexCommand command = spy(new LuceneSearchIndexCommand());
    command.setCache(mockCache);

    gfsh.executeAndAssertThat(command,
        "search lucene --name=vecIdx --region=/docs --query-vector=0.1,notanumber")
        .statusIsError()
        .containsOutput("Invalid float");

    gfsh.executeAndAssertThat(command,
        "search lucene --name=vecIdx --region=/docs --query-vector=0.1,0.2 --k=0")
        .statusIsError()
        .containsOutput("--k must be a positive integer");

    gfsh.executeAndAssertThat(command,
        "search lucene --name=vecIdx --region=/docs")
        .statusIsError()
        .containsOutput("--queryString or --query-vector");

    // --limit <= 0 rejected
    gfsh.executeAndAssertThat(command,
        "search lucene --name=vecIdx --region=/docs --query-vector=0.1,0.2 --limit=0")
        .statusIsError()
        .containsOutput("--limit must be a positive integer");

    // Trailing comma rejected
    gfsh.executeAndAssertThat(command,
        "search lucene --name=vecIdx --region=/docs --query-vector=1.0,2.0,")
        .statusIsError()
        .containsOutput("Empty element");

    // Unmatched bracket rejected
    gfsh.executeAndAssertThat(command,
        "search lucene --name=vecIdx --region=/docs --query-vector=[1.0,2.0")
        .statusIsError()
        .containsOutput("Unmatched brackets");

    // Orphan --k without --query-vector rejected
    gfsh.executeAndAssertThat(command,
        "search lucene --name=vecIdx --region=/docs --queryString=hello --defaultField=cat --k=5")
        .statusIsError()
        .containsOutput("require --query-vector");
  }

  @Test
  public void searchIndexPreservesExplicitLimitAlongsideK() throws Exception {
    LuceneSearchIndexCommand command = spy(new LuceneSearchIndexCommand());
    command.setCache(mockCache);

    ResultCollector<?, ?> rc = mock(ResultCollector.class);
    Set<LuceneSearchResults> hitSet =
        Collections.singleton(new LuceneSearchResults("k1", "v1", 0.95f));
    doReturn(rc).when(command).executeSearch(any(LuceneQueryInfo.class));
    doReturn(Collections.singletonList(hitSet)).when(rc).getResult();

    gfsh.executeAndAssertThat(command,
        "search lucene --name=vecIdx --region=/docs --query-vector=[1.0,0.0] --k=5 --limit=2")
        .statusIsSuccess();

    ArgumentCaptor<LuceneQueryInfo> captor = ArgumentCaptor.forClass(LuceneQueryInfo.class);
    verify(command).executeSearch(captor.capture());
    LuceneQueryInfo info = captor.getValue();
    assertThat(info.getK()).isEqualTo(5);
    assertThat(info.getEffectiveK()).isEqualTo(5);
    assertThat(info.getLimit()).isEqualTo(2);
  }
}
