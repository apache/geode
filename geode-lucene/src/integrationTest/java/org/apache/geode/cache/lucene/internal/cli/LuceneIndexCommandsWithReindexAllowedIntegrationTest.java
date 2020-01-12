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
package org.apache.geode.cache.lucene.internal.cli;

import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.INDEX_NAME;
import static org.apache.geode.cache.lucene.test.LuceneTestUtilities.REGION_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.cache.lucene.internal.repository.serializer.PrimitiveSerializer;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category(LuceneTest.class)
public class LuceneIndexCommandsWithReindexAllowedIntegrationTest
    extends LuceneIndexCommandsIntegrationTest {

  @After
  public void clearLuceneReindexFeatureFlag() {
    LuceneServiceImpl.LUCENE_REINDEX = false;
  }

  @Before
  public void setLuceneReindexFeatureFlag() {
    LuceneServiceImpl.LUCENE_REINDEX = true;
  }

  @Test
  public void whenLuceneReindexingInProgressThenListIndexCommandMustExecuteSuccessfully() {

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "__REGION_VALUE_FIELD");

    createRegion();
    AtomicBoolean stopped = new AtomicBoolean();
    Thread ai = new Thread(() -> {
      int count = 0;
      while (!stopped.get()) {
        server.getCache().getRegion(REGION_NAME).put(count, "hello world" + count);
        count++;
      }
    });
    ai.start();

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");
    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE_LIST_INDEX__STATS, "true");
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess();
    stopped.set(true);
  }

  @Test
  public void whenLuceneReindexAllowedCreateIndexShouldCreateANewIndex() {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");

    createRegion();
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");

    LuceneService luceneService = LuceneServiceProvider.get(server.getCache());
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    assertArrayEquals(new String[] {"field1", "field2", "field3"}, index.getFieldNames());

  }


  @Test
  public void whenLuceneReindexAllowedCreateIndexWithAnalyzersShouldCreateANewIndex() {
    List<String> analyzerNames = new ArrayList<>();
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());
    analyzerNames.add(KeywordAnalyzer.class.getCanonicalName());
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, String.join(",", analyzerNames));

    createRegion();

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");

    LuceneService luceneService = LuceneServiceProvider.get(server.getCache());
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    final Map<String, Analyzer> fieldAnalyzers = index.getFieldAnalyzers();
    assertEquals(StandardAnalyzer.class, fieldAnalyzers.get("field1").getClass());
    assertEquals(KeywordAnalyzer.class, fieldAnalyzers.get("field2").getClass());
    assertEquals(StandardAnalyzer.class, fieldAnalyzers.get("field3").getClass());

  }

  @Test
  public void whenLuceneReindexAllowedCreateIndexWithALuceneSerializerShouldCreateANewIndex() {
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__SERIALIZER,
        PrimitiveSerializer.class.getCanonicalName());

    createRegion();

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");

    LuceneService luceneService = LuceneServiceProvider.get(server.getCache());
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    assertThat(index.getLuceneSerializer()).isInstanceOf(PrimitiveSerializer.class);
  }

  @Test
  public void whenLuceneReindexAllowedCreateIndexShouldTrimAnalyzerNames() {
    List<String> analyzerNames = new ArrayList<>();
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());
    analyzerNames.add(KeywordAnalyzer.class.getCanonicalName());
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER,
        "\"org.apache.lucene.analysis.standard.StandardAnalyzer, org.apache.lucene.analysis.core.KeywordAnalyzer, org.apache.lucene.analysis.standard.StandardAnalyzer\"");

    createRegion();
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");

    LuceneService luceneService = LuceneServiceProvider.get(server.getCache());
    final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
    final Map<String, Analyzer> fieldAnalyzers = index.getFieldAnalyzers();
    assertEquals(StandardAnalyzer.class, fieldAnalyzers.get("field1").getClass());
    assertEquals(KeywordAnalyzer.class, fieldAnalyzers.get("field2").getClass());
    assertEquals(StandardAnalyzer.class, fieldAnalyzers.get("field3").getClass());
  }

  @Test
  public void whenLuceneReindexAllowedCreateIndexWithWhitespaceOrDefaultKeywordAnalyzerShouldUseStandardAnalyzer() {

    createRegion();

    // Test whitespace analyzer name
    String analyzerList = StandardAnalyzer.class.getCanonicalName() + ",     ,"
        + KeywordAnalyzer.class.getCanonicalName();
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "space");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, "'" + analyzerList + "'");

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");

    // Test empty analyzer name
    analyzerList =
        StandardAnalyzer.class.getCanonicalName() + ",," + KeywordAnalyzer.class.getCanonicalName();
    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "empty");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, analyzerList);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");

    // Test keyword analyzer name
    analyzerList = StandardAnalyzer.class.getCanonicalName() + ",DEFAULT,"
        + KeywordAnalyzer.class.getCanonicalName();
    csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, "keyword");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD, "field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER, analyzerList);

    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput("Successfully created lucene index");

    LuceneService luceneService = LuceneServiceProvider.get(server.getCache());
    final LuceneIndex spaceIndex = luceneService.getIndex("space", REGION_NAME);
    final Map<String, Analyzer> spaceFieldAnalyzers = spaceIndex.getFieldAnalyzers();

    final LuceneIndex emptyIndex = luceneService.getIndex("empty", REGION_NAME);
    final Map<String, Analyzer> emptyFieldAnalyzers2 = emptyIndex.getFieldAnalyzers();

    final LuceneIndex keywordIndex = luceneService.getIndex("keyword", REGION_NAME);
    final Map<String, Analyzer> keywordFieldAnalyzers = keywordIndex.getFieldAnalyzers();

    // Test whitespace analyzers
    assertEquals(StandardAnalyzer.class.getCanonicalName(),
        spaceFieldAnalyzers.get("field1").getClass().getCanonicalName());
    assertEquals(StandardAnalyzer.class.getCanonicalName(),
        spaceFieldAnalyzers.get("field2").getClass().getCanonicalName());
    assertEquals(KeywordAnalyzer.class.getCanonicalName(),
        spaceFieldAnalyzers.get("field3").getClass().getCanonicalName());

    // Test empty analyzers
    assertEquals(StandardAnalyzer.class.getCanonicalName(),
        emptyFieldAnalyzers2.get("field1").getClass().getCanonicalName());
    assertEquals(StandardAnalyzer.class.getCanonicalName(),
        emptyFieldAnalyzers2.get("field2").getClass().getCanonicalName());
    assertEquals(KeywordAnalyzer.class.getCanonicalName(),
        emptyFieldAnalyzers2.get("field3").getClass().getCanonicalName());

    // Test keyword analyzers
    assertEquals(StandardAnalyzer.class.getCanonicalName(),
        keywordFieldAnalyzers.get("field1").getClass().getCanonicalName());
    assertEquals(StandardAnalyzer.class.getCanonicalName(),
        keywordFieldAnalyzers.get("field2").getClass().getCanonicalName());
    assertEquals(KeywordAnalyzer.class.getCanonicalName(),
        keywordFieldAnalyzers.get("field3").getClass().getCanonicalName());

  }

}
