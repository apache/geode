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

package org.apache.geode.cache.lucene.test;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneCreateIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneDescribeIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneDestroyIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneListIndexFunction;
import org.apache.geode.cache.lucene.internal.cli.functions.LuceneSearchIndexFunction;
import org.apache.geode.cache.lucene.internal.directory.DumpDirectoryFiles;
import org.apache.geode.cache.lucene.internal.distributed.LuceneQueryFunction;
import org.apache.geode.cache.lucene.internal.distributed.WaitUntilFlushedFunction;
import org.apache.geode.cache.lucene.internal.results.LuceneGetPageFunction;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({IntegrationTest.class, SecurityTest.class})
public class LuceneFunctionSecurityTest {
  private static final String RESULT_HEADER = "Function Execution Result";

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withSecurityManager(SimpleSecurityManager.class)
          .withRegion(RegionShortcut.PARTITION, "testRegion").withAutoStart();

  @Rule
  public GfshCommandRule gfsh =
      new GfshCommandRule(server::getJmxPort, GfshCommandRule.PortType.jmxManager);

  private static Map<Function, String> functionStringMap = new HashMap<>();

  @BeforeClass
  public static void setupClass() {
    functionStringMap.put(new LuceneCreateIndexFunction(), "CLUSTER:MANAGE:LUCENE");
    functionStringMap.put(new LuceneDescribeIndexFunction(), "CLUSTER:READ:LUCENE");
    functionStringMap.put(new LuceneDestroyIndexFunction(), "CLUSTER:MANAGE:LUCENE");
    functionStringMap.put(new LuceneListIndexFunction(), "CLUSTER:READ:LUCENE");
    functionStringMap.put(new LuceneSearchIndexFunction(), "DATA:READ:testRegion");
    functionStringMap.put(new LuceneQueryFunction(), "DATA:READ:testRegion");
    functionStringMap.put(new WaitUntilFlushedFunction(), "DATA:READ:testRegion");
    functionStringMap.put(new LuceneGetPageFunction(), "DATA:READ:testRegion");

    functionStringMap.keySet().forEach(FunctionService::registerFunction);
    FunctionService.registerFunction(new DumpDirectoryFiles());
  }

  @Test
  @ConnectionConfiguration(user = "user", password = "user")
  public void functionRequireExpectedPermission() throws Exception {
    functionStringMap.entrySet().stream().forEach(entry -> {
      Function function = entry.getKey();
      String permission = entry.getValue();
      gfsh.executeAndAssertThat("execute function --region=testRegion --id=" + function.getId())
          .tableHasRowCount(RESULT_HEADER, 1)
          .tableHasRowWithValues(RESULT_HEADER, "Exception: user not authorized for " + permission)
          .statusIsError();
    });
  }

  // use DumpDirectoryFile function to verify that all the permissions returned by the
  // getRequiredPermission are all enforced before trying to execute
  @Test
  @ConnectionConfiguration(user = "clusterManage", password = "clusterManage")
  public void dumpDirectoryFileRequiresBoth_AsClusterManage() {
    gfsh.executeAndAssertThat("execute function --region=testRegion --id=" + DumpDirectoryFiles.ID)
        .tableHasRowCount(RESULT_HEADER, 1).tableHasRowWithValues(RESULT_HEADER,
            "Exception: clusterManage not authorized for DATA:READ:testRegion")
        .statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void dumpDirectoryFileRequiresBoth_AsDataRead() {
    gfsh.executeAndAssertThat("execute function --region=testRegion --id=" + DumpDirectoryFiles.ID)
        .tableHasRowCount(RESULT_HEADER, 1).tableHasRowWithValues(RESULT_HEADER,
            "Exception: dataRead not authorized for CLUSTER:MANAGE")
        .statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "clusterManage,dataReadRegionB",
      password = "clusterManage,dataReadRegionB")
  public void dumpDirectoryFileRequiresBoth_dataReadAnotherRegion() {
    gfsh.executeAndAssertThat("execute function --region=testRegion --id=" + DumpDirectoryFiles.ID)
        .tableHasRowCount(RESULT_HEADER, 1)
        .tableHasRowWithValues(RESULT_HEADER,
            "Exception: clusterManage,dataReadRegionB not authorized for DATA:READ:testRegion")
        .statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "clusterManage,dataReadTestRegionA",
      password = "clusterManage,dataReadTestRegionA")
  public void dumpDirectoryFileRequiresBoth_dataReadInsufficient() {
    gfsh.executeAndAssertThat("execute function --region=testRegion --id=" + DumpDirectoryFiles.ID)
        .tableHasRowCount(RESULT_HEADER, 1)
        .tableHasRowWithValues(RESULT_HEADER,
            "Exception: clusterManage,dataReadTestRegionA not authorized for DATA:READ:testRegion")
        .statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "clusterManage,dataReadTestRegion",
      password = "clusterManage,dataReadTestRegion")
  public void dumpDirectoryFileRequiresBoth_validUser() {
    gfsh.executeAndAssertThat("execute function --region=testRegion --id=" + DumpDirectoryFiles.ID)
        .tableHasRowCount(RESULT_HEADER, 1).doesNotContainOutput("not authorized").statusIsError();
  }
}
