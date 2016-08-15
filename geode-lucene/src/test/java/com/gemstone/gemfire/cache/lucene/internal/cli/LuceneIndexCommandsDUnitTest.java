/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.lucene.internal.cli;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneQuery;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexCreationProfile;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexImpl;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.distributed.ConfigurationProperties;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.commands.CliCommandTestBase;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.test.dunit.*;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.jayway.awaitility.Awaitility;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.Collector;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;


import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.*;
import static com.gemstone.gemfire.test.dunit.Assert.*;
import static junitparams.JUnitParamsRunner.$;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@Category(DistributedTest.class)
@RunWith(JUnitParamsRunner.class)
public class LuceneIndexCommandsDUnitTest extends CliCommandTestBase {


  @Before
  public void createJMXManager() {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(null);
  }

  @Test
  public void listIndexShouldReturnExistingIndexWithStats() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE_LIST_INDEX__STATS,"true");
    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(INDEX_NAME));
    assertTrue(resultAsString.contains("Documents"));
  }

  @Test
  public void listIndexShouldReturnExistingIndexWithoutStats() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(INDEX_NAME));
    assertFalse(resultAsString.contains("Documents"));
  }

  @Test
  public void listIndexWhenNoExistingIndexShouldReturnNoIndex() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains("No lucene indexes found"));
  }

  @Test
  public void listIndexShouldReturnCorrectStatus() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndexWithoutRegion(vm1);
    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE_LIST_INDEX__STATS,"true");
    TabularResultData data = (TabularResultData) executeCommandAndGetResult(csb).getResultData();
    assertEquals(Collections.singletonList(INDEX_NAME), data.retrieveAllValues("Index Name"));
    assertEquals(Collections.singletonList("Defined"), data.retrieveAllValues("Status"));
  }

  @Test
  public void listIndexWithStatsShouldReturnCorrectStats() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    Map<String,TestObject> entries=new HashMap<>();
    entries.put("A",new TestObject("field1:value1","field2:value2","field3:value3"));
    entries.put("B",new TestObject("ABC","EFG","HIJ"));

    putEntries(vm1,entries,2);
    queryAndVerify(vm1, "field1:value1", "field1", Collections.singletonList("A"));

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE_LIST_INDEX__STATS,"true");
    TabularResultData data = (TabularResultData) executeCommandAndGetResult(csb).getResultData();

    assertEquals(Collections.singletonList(INDEX_NAME), data.retrieveAllValues("Index Name"));
    assertEquals(Collections.singletonList("Initialized"), data.retrieveAllValues("Status"));
    assertEquals(Collections.singletonList("/region"), data.retrieveAllValues("Region Path"));
    assertEquals(Collections.singletonList("113"), data.retrieveAllValues("Query Executions"));
    assertEquals(Collections.singletonList("2"), data.retrieveAllValues("Commits"));
    assertEquals(Collections.singletonList("2"), data.retrieveAllValues("Updates"));
    assertEquals(Collections.singletonList("2"), data.retrieveAllValues("Documents"));
  }

  @Test
  public void createIndexShouldCreateANewIndex() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(() -> {getCache();});

    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH,REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD,"field1,field2,field3");

    String resultAsString = executeCommandAndLogResult(csb);

    vm1.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      createRegion();
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertArrayEquals(new String[] {"field1", "field2", "field3"}, index.getFieldNames());
    });
  }

  @Test
  public void createIndexWithAnalyzersShouldCreateANewIndex() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(() -> {getCache();});

    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());

    List<String> analyzerNames = new ArrayList<>();
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());
    analyzerNames.add(KeywordAnalyzer.class.getCanonicalName());
    analyzerNames.add(StandardAnalyzer.class.getCanonicalName());


    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH,REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD,"field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER,String.join(",",analyzerNames));

    String resultAsString = executeCommandAndLogResult(csb);

    vm1.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      createRegion();
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      final Map<String, Analyzer> fieldAnalyzers = index.getFieldAnalyzers();
      assertEquals(StandardAnalyzer.class, fieldAnalyzers.get("field1").getClass());
      assertEquals(KeywordAnalyzer.class, fieldAnalyzers.get("field2").getClass());
      assertEquals(StandardAnalyzer.class, fieldAnalyzers.get("field3").getClass());
    });
  }

  @Test
  public void createIndexOnGroupShouldCreateANewIndexOnGroup() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);
    final VM vm2 = Host.getHost(0).getVM(2);
    vm1.invoke(() -> {
      getCache();
    });
    vm2.invoke(() -> {
      Properties props = new Properties();
      props.setProperty(ConfigurationProperties.GROUPS, "group1");
      getSystem(props);
      getCache();
    });

    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH,REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD,"field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__GROUP,"group1");
    String resultAsString = executeCommandAndLogResult(csb);

    vm2.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      createRegion();
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      assertArrayEquals(new String[] {"field1", "field2", "field3"}, index.getFieldNames());
    });

    vm1.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      try {
        createRegion();
        fail("Should have thrown an exception due to the missing index");
      } catch(IllegalStateException expected) {

      }
    });
  }

  @Test
  public void createIndexWithoutRegionShouldReturnCorrectResults() throws Exception{
    final VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(() -> {getCache();});

    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH,REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD,"field1,field2,field3");

    String resultAsString = executeCommandAndLogResult(csb);

    vm1.invoke(() -> {
      LuceneServiceImpl luceneService = (LuceneServiceImpl) LuceneServiceProvider.get(getCache());
      final ArrayList<LuceneIndexCreationProfile> profiles= new ArrayList<> (luceneService.getAllDefinedIndexes());
      assertEquals(1, profiles.size());
      assertEquals(INDEX_NAME, profiles.get(0).getIndexName());
    });
  }

  @Test
  public void createIndexWithNullAnalyzerShouldUseStandardAnalyzer() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(() -> {getCache();});

    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());
    String analyzerList=StandardAnalyzer.class.getCanonicalName()+",null,"+KeywordAnalyzer.class.getCanonicalName();
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH,REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__FIELD,"field1,field2,field3");
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__ANALYZER,analyzerList);

    String resultAsString = executeCommandAndLogResult(csb);

    vm1.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      createRegion();
      final LuceneIndex index = luceneService.getIndex(INDEX_NAME, REGION_NAME);
      final Map<String, Analyzer> fieldAnalyzers = index.getFieldAnalyzers();
      assertEquals(StandardAnalyzer.class.getCanonicalName(), fieldAnalyzers.get("field1").getClass().getCanonicalName());
      assertEquals(StandardAnalyzer.class.getCanonicalName(), fieldAnalyzers.get("field2").getClass().getCanonicalName());
      assertEquals(KeywordAnalyzer.class.getCanonicalName(), fieldAnalyzers.get("field3").getClass().getCanonicalName());
    });
  }

  @Test
  public void describeIndexShouldReturnExistingIndex() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESCRIBE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH,REGION_NAME);
    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(INDEX_NAME));
  }

  @Test
  public void describeIndexShouldNotReturnResultWhenIndexNotFound() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESCRIBE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME,"notAnIndex");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH,REGION_NAME);
    String resultAsString = executeCommandAndLogResult(csb);

    assertTrue(resultAsString.contains("No lucene indexes found"));
  }

  @Test
  public void describeIndexWithoutRegionShouldReturnErrorMessage() throws Exception {

    final VM vm1 = Host.getHost(0).getVM(1);

    createIndexWithoutRegion(vm1);
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_DESCRIBE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME,"notAnIndex");
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH,REGION_NAME);
    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains("Region not found"));
  }

  @Test
  public void searchShouldReturnCorrectResults() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    Map<String,TestObject> entries=new HashMap<>();
    entries.put("A",new TestObject("value1 ","value2","value3"));
    entries.put("B",new TestObject("ABC","EFG","HIJ"));
    entries.put("C",new TestObject("value1","QWE","RTY"));
    entries.put("D",new TestObject("ABC","EFG","HIJ"));
    entries.put("E",new TestObject("value1","ABC","EFG"));
    entries.put("F",new TestObject("ABC","EFG","HIJ"));
    entries.put("G",new TestObject(" value1","JKR","POW"));
    entries.put("H",new TestObject("ABC","EFG","H2J"));
    putEntries(vm1,entries,8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH,REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING,"field1:value1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD,"field1");
    executeCommandAndLogResult(csb);

    TabularResultData data = (TabularResultData) executeCommandAndGetResult(csb).getResultData();
    assertEquals(4,data.retrieveAllValues("key").size());
  }

  @Test
  public void searchShouldReturnNoResults() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    Map<String,TestObject> entries=new HashMap<>();
    entries.put("A",new TestObject("value1 ","value2","value3"));
    entries.put("B",new TestObject("ABC","EFG","HIJ"));
    entries.put("C",new TestObject("value1","QWE","RTY"));
    entries.put("D",new TestObject("ABC","EFG","HIJ"));
    entries.put("E",new TestObject(":value1","ABC","EFG"));
    entries.put("F",new TestObject("ABC","EFG","HIJ"));
    entries.put("G",new TestObject(" value1","JKR","POW"));
    entries.put("H",new TestObject("ABC","EFG","H2J"));
    putEntries(vm1,entries,8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH,REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING,"NotAnExistingValue");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD,"field1");
    executeCommandAndLogResult(csb);

    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(LuceneCliStrings.LUCENE_SEARCH_INDEX__NO_RESULTS_MESSAGE));
  }

  @Test
  public void searchWithLimitShouldReturnCorrectResults() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    Map<String,TestObject> entries=new HashMap<>();
    entries.put("A",new TestObject("value1 ","value2","value3"));
    entries.put("B",new TestObject("ABC","EFG","HIJ"));
    entries.put("C",new TestObject("value1","QWE","RTY"));
    entries.put("D",new TestObject("ABC","EFG","HIJ"));
    entries.put("E",new TestObject("value1","ABC","EFG"));
    entries.put("F",new TestObject("ABC","EFG","HIJ"));
    entries.put("G",new TestObject(" value1","JKR","POW"));
    entries.put("H",new TestObject("ABC","EFG","H2J"));
    putEntries(vm1,entries,8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH,REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING,"field1:value1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD,"field1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__LIMIT,"2");
    executeCommandAndLogResult(csb);
    TabularResultData data = (TabularResultData) executeCommandAndGetResult(csb).getResultData();
    assertEquals(2,data.retrieveAllValues("key").size());
  }

  @Test
  public void searchWithoutFieldNameShouldReturnCorrectResults() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    Map<String,TestObject> entries=new HashMap<>();
    entries.put("A",new TestObject("value1 ","value2","value3"));
    entries.put("B",new TestObject("ABC","EFG","HIJ"));
    entries.put("C",new TestObject("value1","QWE","RTY"));
    entries.put("D",new TestObject("ABC","EFG","HIJ"));
    entries.put("E",new TestObject("value1","ABC","EFG"));
    entries.put("F",new TestObject("ABC","EFG","HIJ"));
    entries.put("G",new TestObject("value1","JKR","POW"));
    entries.put("H",new TestObject("ABC","EFG","H2J"));
    putEntries(vm1,entries,8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH,REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING,"QWE");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD,"field2");
    executeCommandAndLogResult(csb);

    TabularResultData data = (TabularResultData) executeCommandAndGetResult(csb).getResultData();
    assertEquals(1,data.retrieveAllValues("key").size());
  }

  @Test
  public void searchWithInvalidQueryStringShouldReturnError() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    Map<String,TestObject> entries=new HashMap<>();
    entries.put("A",new TestObject("value1 ","value2","value3"));
    entries.put("B",new TestObject("ABC","EFG","HIJ"));;
    putEntries(vm1,entries,2);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH,REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING,"WF~*");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD,"field2");

    String commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Result String :\n ", resultAsString);
    assertEquals(Status.ERROR, commandResult.getStatus());
    assertTrue(resultAsString.contains("Leading wildcard is not allowed: field2:*"));
  }

  @Test
  public void searchOnIndexWithoutRegionShouldReturnError() throws Exception {

    final VM vm1 = Host.getHost(0).getVM(1);

    createIndexWithoutRegion(vm1);
    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "EFG");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field2");

    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains("Region not found"));
  }

  @Test
  public void searchWithoutIndexShouldReturnError() throws Exception {

    final VM vm1 = Host.getHost(0).getVM(1);

    vm1.invoke(() -> createRegion());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME, INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH, REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING, "EFG");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD, "field2");

    String commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Result String :\n ", resultAsString);
    assertEquals(Status.ERROR, commandResult.getStatus());
    assertTrue(resultAsString.contains("Index "+INDEX_NAME+" not found"));
  }

  @Test
  public void searchIndexShouldReturnCorrectKeys() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    Map<String,TestObject> entries=new HashMap<>();
    entries.put("A",new TestObject("value1 ","value2","value3"));
    entries.put("B",new TestObject("ABC","EFG","HIJ"));
    entries.put("C",new TestObject("value1","QWE","RTY"));
    entries.put("D",new TestObject("ABC","EFG","HIJ"));
    entries.put("E",new TestObject("value1","ABC","EFG"));
    entries.put("F",new TestObject("ABC","EFG","HIJ"));
    entries.put("G",new TestObject("value1","JKR","POW"));
    entries.put("H",new TestObject("ABC","EFG","H2J"));
    putEntries(vm1,entries,8);

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_SEARCH_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE__INDEX_NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE__REGION_PATH,REGION_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__QUERY_STRING,"value1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__DEFAULT_FIELD,"field1");
    csb.addOption(LuceneCliStrings.LUCENE_SEARCH_INDEX__KEYSONLY,"true");
    executeCommandAndLogResult(csb);

    TabularResultData data = (TabularResultData) executeCommandAndGetResult(csb).getResultData();
    assertEquals(4,data.retrieveAllValues("key").size());

  }

  private void createRegion() {
    getCache().createRegionFactory(RegionShortcut.PARTITION).create(REGION_NAME);
  }

  private String executeCommandAndLogResult(final CommandStringBuilder csb) {
    String commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Result String :\n ", resultAsString);
    assertEquals("Command failed\n" + resultAsString, Status.OK, commandResult.getStatus());
    return resultAsString;
  }

  private CommandResult executeCommandAndGetResult(final CommandStringBuilder csb) {
    String commandString = csb.toString();
    writeToLog("Command String :\n ", commandString);
    CommandResult commandResult = executeCommand(commandString);
    String resultAsString = commandResultToString(commandResult);
    writeToLog("Result String :\n ", resultAsString);
    assertEquals("Command failed\n" + resultAsString, Status.OK, commandResult.getStatus());
    return commandResult;
  }

  private void createIndex(final VM vm1) {
    vm1.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      Map<String, Analyzer> fieldAnalyzers = new HashMap();
      fieldAnalyzers.put("field1", new StandardAnalyzer());
      fieldAnalyzers.put("field2", new KeywordAnalyzer());
      fieldAnalyzers.put("field3", null);
      luceneService.createIndex(INDEX_NAME, REGION_NAME, fieldAnalyzers);
      createRegion();
    });
  }

  private void createIndexWithoutRegion(final VM vm1) {
    vm1.invoke(() -> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      Map<String, Analyzer> fieldAnalyzers = new HashMap();
      fieldAnalyzers.put("field1", new StandardAnalyzer());
      fieldAnalyzers.put("field2", new KeywordAnalyzer());
      fieldAnalyzers.put("field3", null);
      luceneService.createIndex(INDEX_NAME, REGION_NAME, fieldAnalyzers);
    });
  }

  private void writeToLog(String text, String resultAsString) {
    System.out.println(text + ": " + getTestMethodName() + " : ");
    System.out.println(text + ":" + resultAsString);
  }

  private void putEntries(final VM vm1, Map<String,TestObject> entries, int countOfDocuments) {
    Cache cache=getCache();
    vm1.invoke(()-> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      Region region=getCache().getRegion(REGION_NAME);
      region.putAll(entries);
      luceneService.getIndex(INDEX_NAME,REGION_NAME).waitUntilFlushed(60000);
      LuceneIndexImpl index=(LuceneIndexImpl) luceneService.getIndex(INDEX_NAME,REGION_NAME);
      Awaitility.await().atMost(65, TimeUnit.SECONDS).until(() ->
        assertEquals(countOfDocuments,index.getIndexStats().getDocuments()));

    });
  }

  private void queryAndVerify(VM vm1, String queryString, String defaultField, List<String> expectedKeys) {
    vm1.invoke(()-> {
      LuceneService luceneService = LuceneServiceProvider.get(getCache());
      final LuceneQuery<String, TestObject> query = luceneService.createLuceneQueryFactory().create(
        INDEX_NAME, REGION_NAME, queryString, defaultField);
      assertEquals(Collections.singletonList("A"),query.findKeys());
    });
  }

  protected class TestObject implements Serializable{
    private String field1;
    private String field2;
    private String field3;

    protected TestObject(String value1, String value2, String value3) {
      this.field1=value1;
      this.field2=value2;
      this.field3=value3;
    }

    public String toString() {
      return "field1="+field1+" field2="+field2+" field3="+field3;
    }
  }
}
