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
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.distributed.ConfigurationProperties;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.commands.CliCommandTestBase;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.test.dunit.*;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.sun.org.apache.regexp.internal.RE;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.gemstone.gemfire.cache.lucene.test.LuceneTestUtilities.*;
import static com.gemstone.gemfire.test.dunit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

@Category(DistributedTest.class)
public class LuceneIndexCommandsDUnitTest extends CliCommandTestBase {

  @Before
  public void createJMXManager() {
    disconnectAllFromDS();
    setUpJmxManagerOnVm0ThenConnect(null);
  }

  @Test
  public void listIndexShouldReturnExistingIndex() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);

    createIndex(vm1);
    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_LIST_INDEX);
    String resultAsString = executeCommandAndLogResult(csb);
    assertTrue(resultAsString.contains(INDEX_NAME));
  }

  @Test
  public void createIndexShouldCreateANewIndex() throws Exception {
    final VM vm1 = Host.getHost(0).getVM(1);
    vm1.invoke(() -> {getCache();});

    CommandManager.getInstance().add(LuceneIndexCommands.class.newInstance());

    CommandStringBuilder csb = new CommandStringBuilder(LuceneCliStrings.LUCENE_CREATE_INDEX);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__REGION,REGION_NAME);
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
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__REGION,REGION_NAME);
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
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__NAME,INDEX_NAME);
    csb.addOption(LuceneCliStrings.LUCENE_CREATE_INDEX__REGION,REGION_NAME);
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

  private void writeToLog(String text, String resultAsString) {
    System.out.println(text + ": " + getTestMethodName() + " : ");
    System.out.println(text + ":" + resultAsString);
  }
}
