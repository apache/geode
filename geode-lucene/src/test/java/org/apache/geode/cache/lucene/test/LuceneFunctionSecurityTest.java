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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

import java.util.function.Predicate;

import org.apache.logging.log4j.Logger;
import org.assertj.core.api.Condition;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

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
import org.apache.geode.cache.lucene.internal.security.LucenePermission;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({IntegrationTest.class, SecurityTest.class})
public class LuceneFunctionSecurityTest {
  private static final Logger logger = LogService.getLogger();

  private static Function luceneCreateIndexFunction = Mockito.spy(new LuceneCreateIndexFunction());
  private static Function luceneDescribeIndexFunction =
      Mockito.spy(new LuceneDescribeIndexFunction());
  private static Function luceneDestroyIndexFunction =
      Mockito.spy(new LuceneDestroyIndexFunction());
  private static Function luceneListIndexFunction = Mockito.spy(new LuceneListIndexFunction());
  private static Function luceneSearchIndexFunction = Mockito.spy(new LuceneSearchIndexFunction());
  private static Function dumpDirectoryFiles = Mockito.spy(new DumpDirectoryFiles());
  private static Function luceneQueryFunction = Mockito.spy(new LuceneQueryFunction());
  private static Function waitUntilFlushedFunction = Mockito.spy(new WaitUntilFlushedFunction());
  private static Function luceneGetPageFunction = Mockito.spy(new LuceneGetPageFunction());

  static {
    Mockito.doAnswer(announceNoOp()).when(luceneCreateIndexFunction).execute(any());
    Mockito.doAnswer(announceNoOp()).when(luceneDescribeIndexFunction).execute(any());
    Mockito.doAnswer(announceNoOp()).when(luceneDestroyIndexFunction).execute(any());
    Mockito.doAnswer(announceNoOp()).when(luceneListIndexFunction).execute(any());
    Mockito.doAnswer(announceNoOp()).when(luceneSearchIndexFunction).execute(any());
    Mockito.doAnswer(announceNoOp()).when(dumpDirectoryFiles).execute(any());
    Mockito.doAnswer(announceNoOp()).when(luceneQueryFunction).execute(any());
    Mockito.doAnswer(announceNoOp()).when(waitUntilFlushedFunction).execute(any());
    Mockito.doAnswer(announceNoOp()).when(luceneGetPageFunction).execute(any());
  }

  private static String regionName = "luceneTestRegion";

  private static ResourcePermission clusterManage =
      new ResourcePermission(Resource.CLUSTER, Operation.MANAGE);

  private static ResourcePermission clusterManageLucene =
      new ResourcePermission(Resource.CLUSTER, Operation.MANAGE, LucenePermission.TARGET);

  private static ResourcePermission clusterReadLucene =
      new ResourcePermission(Resource.CLUSTER, Operation.READ, LucenePermission.TARGET);

  private static ResourcePermission dataRead =
      new ResourcePermission(Resource.DATA, Operation.READ);

  private static ResourcePermission dataReadRegion =
      new ResourcePermission(Resource.DATA, Operation.READ, regionName);

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withSecurityManager(SimpleSecurityManager.class)
          .withRegion(RegionShortcut.PARTITION, regionName).withAutoStart();

  @Rule
  public GfshCommandRule gfsh =
      new GfshCommandRule(server::getJmxPort, GfshCommandRule.PortType.jmxManager);

  @BeforeClass
  public static void setupClass() {
    FunctionService.registerFunction(luceneCreateIndexFunction);
    FunctionService.registerFunction(luceneDescribeIndexFunction);
    FunctionService.registerFunction(luceneDestroyIndexFunction);
    FunctionService.registerFunction(luceneListIndexFunction);
    FunctionService.registerFunction(luceneSearchIndexFunction);
    FunctionService.registerFunction(dumpDirectoryFiles);
    FunctionService.registerFunction(luceneQueryFunction);
    FunctionService.registerFunction(waitUntilFlushedFunction);
    FunctionService.registerFunction(luceneGetPageFunction);
  }

  /* Command authorized tests */
  @Test
  @ConnectionConfiguration(user = "clusterManageLucene", password = "clusterManageLucene")
  public void testValidPermissionsForLuceneCreateIndexFunction() throws Exception {
    Function thisFunction = luceneCreateIndexFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId()).statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "clusterReadLucene", password = "clusterReadLucene")
  public void testValidPermissionsForLuceneDescribeIndexFunction() throws Exception {
    Function thisFunction = luceneDescribeIndexFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId()).statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "clusterManageLucene", password = "clusterManageLucene")
  public void testValidPermissionsForLuceneDestroyIndexFunction() throws Exception {
    Function thisFunction = luceneDestroyIndexFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId()).statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "clusterReadLucene", password = "clusterReadLucene")
  public void testValidPermissionsForLuceneListIndexFunction() throws Exception {
    Function thisFunction = luceneListIndexFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId()).statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testValidPermissionsForDumpDirectoryFilesWithRegionParameter_withClusterManage()
      throws Exception {
    Function thisFunction = dumpDirectoryFiles;

    gfsh.executeAndAssertThat(
        "execute function  --region=" + regionName + " --id=" + thisFunction.getId())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testValidPermissionsForDumpDirectoryFilesWithoutRegionParameter_withClusterManage()
      throws Exception {
    Function thisFunction = dumpDirectoryFiles;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId()).statusIsSuccess();
  }


  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testValidPermissionsForLuceneSearchIndexFunctionWithoutRegionParameter()
      throws Exception {
    Function thisFunction = luceneSearchIndexFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId()).statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataReadRegion", password = "dataReadRegion")
  public void testValidPermissionsForLuceneSearchIndexFunctionWithRegionParameter()
      throws Exception {
    Function thisFunction = luceneSearchIndexFunction;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testValidPermissionsForLuceneQueryFunctionWithoutRegionParameter() throws Exception {
    Function thisFunction = luceneQueryFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId()).statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataReadRegion", password = "dataReadRegion")
  public void testValidPermissionsForLuceneQueryFunctionWithRegionParameter() throws Exception {
    Function thisFunction = luceneQueryFunction;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testValidPermissionsForWaitUntilFlushedFunctionWithoutRegionParameter()
      throws Exception {
    Function thisFunction = waitUntilFlushedFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId()).statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataReadRegion", password = "dataReadRegion")
  public void testValidPermissionsForWaitUntilFlushedFunctionWithRegionParameter()
      throws Exception {
    Function thisFunction = waitUntilFlushedFunction;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testValidPermissionsForLuceneGetPageFunctionWithoutRegionParameter()
      throws Exception {
    Function thisFunction = luceneGetPageFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId()).statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataReadRegion", password = "dataReadRegion")
  public void testValidPermissionsForLuceneGetPageFunctionWithRegionParameter() throws Exception {
    Function thisFunction = luceneGetPageFunction;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .statusIsSuccess();
  }


  /* Command refused tests */
  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneCreateIndexFunction() throws Exception {
    Function thisFunction = luceneCreateIndexFunction;
    ResourcePermission thisRequiredPermission = clusterManageLucene;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneDescribeIndexFunction() throws Exception {
    Function thisFunction = luceneDescribeIndexFunction;
    ResourcePermission thisRequiredPermission = clusterReadLucene;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneDestroyIndexFunction() throws Exception {
    Function thisFunction = luceneDestroyIndexFunction;
    ResourcePermission thisRequiredPermission = clusterManageLucene;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneListIndexFunction() throws Exception {
    Function thisFunction = luceneListIndexFunction;
    ResourcePermission thisRequiredPermission = clusterReadLucene;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForDumpDirectoryFilesWithoutRegionParameter_noPermission()
      throws Exception {
    Function thisFunction = dumpDirectoryFiles;

    Predicate<String> notAuthForDataRead =
        s -> s.contains("not authorized for " + dataRead.toString());
    Predicate<String> notAuthForClusterManage =
        s -> s.contains("not authorized for " + clusterManage.toString());
    Predicate<String> notAuthForSomePermission =
        s -> notAuthForDataRead.test(s) || notAuthForClusterManage.test(s);

    String output = gfsh.execute("execute function --id=" + thisFunction.getId());

    Condition<String> containsSomeAuthFailure = new Condition<>(notAuthForSomePermission,
        "not authorized for for [DATA:MANAGE|CLUSTER:MANAGE]", output);
    assertThat(output).has(containsSomeAuthFailure);
  }

  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testInvalidPermissionsForDumpDirectoryFilesWithoutRegionParameter_withDataRead()
      throws Exception {
    Function thisFunction = dumpDirectoryFiles;
    ResourcePermission thisMissingPermission = clusterManage;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisMissingPermission.toString()).statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "clusterManage", password = "clusterManage")
  public void testInvalidPermissionsForDumpDirectoryFilesWithoutRegionParameter_withClusterManage()
      throws Exception {
    Function thisFunction = dumpDirectoryFiles;
    ResourcePermission thisMissingPermission = dataRead;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisMissingPermission.toString()).statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForDumpDirectoryFilesWithRegionParameter_noPermission()
      throws Exception {
    Function thisFunction = dumpDirectoryFiles;

    Predicate<String> notAuthForDataReadRegion =
        s -> s.contains("not authorized for " + dataReadRegion.toString());
    Predicate<String> notAuthForClusterManage =
        s -> s.contains("not authorized for " + clusterManage.toString());
    Predicate<String> notAuthForSomePermission =
        s -> notAuthForDataReadRegion.test(s) || notAuthForClusterManage.test(s);

    String output =
        gfsh.execute("execute function --region=" + regionName + " --id=" + thisFunction.getId());

    Condition<String> containsSomeAuthFailure =
        new Condition<>(notAuthForSomePermission, "D:R or C:M:L auth failure", output);
    assertThat(output).has(containsSomeAuthFailure);
  }

  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testInvalidPermissionsForDumpDirectoryFilesWithRegionParameter_withDataRead()
      throws Exception {
    Function thisFunction = dumpDirectoryFiles;
    ResourcePermission thisMissingPermission = clusterManage;

    gfsh.executeAndAssertThat(
        "execute function  --region=" + regionName + " --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisMissingPermission.toString()).statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "clusterManage", password = "clusterManage")
  public void testInvalidPermissionsForDumpDirectoryFilesWithRegionParameter_withClusterManage()
      throws Exception {
    Function thisFunction = dumpDirectoryFiles;
    ResourcePermission thisMissingPermission = dataRead;

    gfsh.executeAndAssertThat(
        "execute function  --region=" + regionName + " --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisMissingPermission.toString()).statusIsSuccess();
  }


  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneSearchIndexFunctionWithoutRegionParameter()
      throws Exception {
    Function thisFunction = luceneSearchIndexFunction;
    ResourcePermission thisRequiredPermission = dataRead;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneSearchIndexFunctionWithRegionParameter()
      throws Exception {
    Function thisFunction = luceneSearchIndexFunction;
    ResourcePermission thisRequiredPermission = dataReadRegion;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneQueryFunctionWithoutRegionParameter()
      throws Exception {
    Function thisFunction = luceneQueryFunction;
    ResourcePermission thisRequiredPermission = dataRead;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneQueryFunctionWithRegionParameter() throws Exception {
    Function thisFunction = luceneQueryFunction;
    ResourcePermission thisRequiredPermission = dataReadRegion;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForWaitUntilFlushedFunctionWithoutRegionParameter()
      throws Exception {
    Function thisFunction = waitUntilFlushedFunction;
    ResourcePermission thisRequiredPermission = dataRead;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForWaitUntilFlushedFunctionWithRegionParameter()
      throws Exception {
    Function thisFunction = waitUntilFlushedFunction;
    ResourcePermission thisRequiredPermission = dataReadRegion;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneGetPageFunctionWithoutRegionParameter()
      throws Exception {
    Function thisFunction = luceneGetPageFunction;
    ResourcePermission thisRequiredPermission = dataRead;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString())
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneGetPageFunctionWithRegionParameter() throws Exception {
    Function thisFunction = luceneGetPageFunction;
    ResourcePermission thisRequiredPermission = dataReadRegion;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString())
        .statusIsSuccess();
  }

  private static Answer<Void> announceNoOp() {
    return invocation -> {
      String mockedName = invocation.getMethod().getName();
      logger.info("Performing no-op mock of " + mockedName);
      return null;
    };
  }
}
