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

import static org.apache.geode.management.internal.security.ResourcePermissions.CLUSTER_MANAGE;
import static org.apache.geode.management.internal.security.ResourcePermissions.DATA_READ;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

import java.util.function.Predicate;

import org.assertj.core.api.Condition;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
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
  // Note: this region name is embedded below in several @ConnectionConfiguration inputs,
  // which is itself case-sensitive in parsing.
  private static String regionName = "this_test_region";

  private static ResourcePermission CLUSTER_MANAGE_LUCENE =
      new ResourcePermission(Resource.CLUSTER, Operation.MANAGE, LucenePermission.TARGET);
  private static ResourcePermission CLUSTER_READ_LUCENE =
      new ResourcePermission(Resource.CLUSTER, Operation.READ, LucenePermission.TARGET);
  private static ResourcePermission DATA_READ_REGION =
      new ResourcePermission(Resource.DATA, Operation.READ, regionName);

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
    Mockito.doAnswer(callLastResult()).when(luceneCreateIndexFunction).execute(any());
    Mockito.doAnswer(callLastResult()).when(luceneDescribeIndexFunction).execute(any());
    Mockito.doAnswer(callLastResult()).when(luceneDestroyIndexFunction).execute(any());
    Mockito.doAnswer(callLastResult()).when(luceneListIndexFunction).execute(any());
    Mockito.doAnswer(callLastResult()).when(luceneSearchIndexFunction).execute(any());
    Mockito.doAnswer(callLastResult()).when(dumpDirectoryFiles).execute(any());
    Mockito.doAnswer(callLastResult()).when(luceneQueryFunction).execute(any());
    Mockito.doAnswer(callLastResult()).when(waitUntilFlushedFunction).execute(any());
    Mockito.doAnswer(callLastResult()).when(luceneGetPageFunction).execute(any());
  }

  // The FunctionService requires a lastResult to be produced
  private static Answer<Void> callLastResult() {
    return invocation -> {
      FunctionContext context = invocation.getArgument(0);
      context.getResultSender().lastResult(null);
      return null;
    };
  }

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
  public void testValidPermissionsForLuceneCreateIndexFunction() {
    Function thisFunction = luceneCreateIndexFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .doesNotContainOutput("not authorized for").statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "clusterReadLucene", password = "clusterReadLucene")
  public void testValidPermissionsForLuceneDescribeIndexFunction() {
    Function thisFunction = luceneDescribeIndexFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .doesNotContainOutput("not authorized for").statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "clusterManageLucene", password = "clusterManageLucene")
  public void testValidPermissionsForLuceneDestroyIndexFunction() {
    Function thisFunction = luceneDestroyIndexFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .doesNotContainOutput("not authorized for").statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "clusterReadLucene", password = "clusterReadLucene")
  public void testValidPermissionsForLuceneListIndexFunction() {
    Function thisFunction = luceneListIndexFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .doesNotContainOutput("not authorized for").statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataReadThis_test_region,clusterManage",
      password = "dataReadThis_test_region,clusterManage")
  public void testValidPermissionsForDumpDirectoryFilesWithRegionParameter() {
    Function thisFunction = dumpDirectoryFiles;

    gfsh.executeAndAssertThat(
        "execute function  --region=" + regionName + " --id=" + thisFunction.getId())
        .doesNotContainOutput("not authorized for").statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataRead,clusterManage", password = "dataRead,clusterManage")
  public void testValidPermissionsForDumpDirectoryFilesWithoutRegionParameter() {
    Function thisFunction = dumpDirectoryFiles;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .doesNotContainOutput("not authorized for").statusIsSuccess();
  }


  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testValidPermissionsForLuceneSearchIndexFunctionWithoutRegionParameter() {
    Function thisFunction = luceneSearchIndexFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .doesNotContainOutput("not authorized for").statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataReadThis_test_region", password = "dataReadThis_test_region")
  public void testValidPermissionsForLuceneSearchIndexFunctionWithRegionParameter() {
    Function thisFunction = luceneSearchIndexFunction;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .doesNotContainOutput("not authorized for").statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testValidPermissionsForLuceneQueryFunctionWithoutRegionParameter() {
    Function thisFunction = luceneQueryFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .doesNotContainOutput("not authorized for").statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataReadThis_test_region", password = "dataReadThis_test_region")
  public void testValidPermissionsForLuceneQueryFunctionWithRegionParameter() {
    Function thisFunction = luceneQueryFunction;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .doesNotContainOutput("not authorized for").statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testValidPermissionsForWaitUntilFlushedFunctionWithoutRegionParameter() {
    Function thisFunction = waitUntilFlushedFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .doesNotContainOutput("not authorized for").statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataReadThis_test_region", password = "dataReadThis_test_region")
  public void testValidPermissionsForWaitUntilFlushedFunctionWithRegionParameter() {
    Function thisFunction = waitUntilFlushedFunction;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .doesNotContainOutput("not authorized for").statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testValidPermissionsForLuceneGetPageFunctionWithoutRegionParameter() {
    Function thisFunction = luceneGetPageFunction;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .doesNotContainOutput("not authorized for").statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataReadThis_test_region", password = "dataReadThis_test_region")
  public void testValidPermissionsForLuceneGetPageFunctionWithRegionParameter() {
    Function thisFunction = luceneGetPageFunction;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .doesNotContainOutput("not authorized for").statusIsSuccess();
  }


  /* Command refused tests */
  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneCreateIndexFunction() {
    Function thisFunction = luceneCreateIndexFunction;
    ResourcePermission thisRequiredPermission = CLUSTER_MANAGE_LUCENE;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString()).statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneDescribeIndexFunction() {
    Function thisFunction = luceneDescribeIndexFunction;
    ResourcePermission thisRequiredPermission = CLUSTER_READ_LUCENE;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString()).statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneDestroyIndexFunction() {
    Function thisFunction = luceneDestroyIndexFunction;
    ResourcePermission thisRequiredPermission = CLUSTER_MANAGE_LUCENE;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString()).statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneListIndexFunction() {
    Function thisFunction = luceneListIndexFunction;
    ResourcePermission thisRequiredPermission = CLUSTER_READ_LUCENE;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString()).statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForDumpDirectoryFilesWithoutRegionParameter_noPermission()
      throws Exception {
    Function thisFunction = dumpDirectoryFiles;

    Predicate<String> notAuthForDataRead =
        s -> s.contains("not authorized for " + DATA_READ.toString());
    Predicate<String> notAuthForClusterManage =
        s -> s.contains("not authorized for " + CLUSTER_MANAGE.toString());
    Predicate<String> notAuthForSomePermission =
        s -> notAuthForDataRead.test(s) || notAuthForClusterManage.test(s);

    String output = gfsh.execute("execute function --id=" + thisFunction.getId());

    Condition<String> containsSomeAuthFailure = new Condition<>(notAuthForSomePermission,
        "not authorized for for [DATA:MANAGE|CLUSTER:MANAGE]", output);
    assertThat(output).has(containsSomeAuthFailure);
  }

  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testInvalidPermissionsForDumpDirectoryFilesWithoutRegionParameter_withDataRead() {
    Function thisFunction = dumpDirectoryFiles;
    ResourcePermission thisMissingPermission = CLUSTER_MANAGE;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisMissingPermission.toString()).statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "clusterManage", password = "clusterManage")
  public void testInvalidPermissionsForDumpDirectoryFilesWithoutRegionParameter_withClusterManage() {
    Function thisFunction = dumpDirectoryFiles;
    ResourcePermission thisMissingPermission = DATA_READ;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisMissingPermission.toString()).statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForDumpDirectoryFilesWithRegionParameter_noPermission()
      throws Exception {
    Function thisFunction = dumpDirectoryFiles;

    Predicate<String> notAuthForDataReadRegion =
        s -> s.contains("not authorized for " + DATA_READ_REGION.toString());
    Predicate<String> notAuthForClusterManage =
        s -> s.contains("not authorized for " + CLUSTER_MANAGE.toString());
    Predicate<String> notAuthForSomePermission =
        s -> notAuthForDataReadRegion.test(s) || notAuthForClusterManage.test(s);

    String output =
        gfsh.execute("execute function --region=" + regionName + " --id=" + thisFunction.getId());

    Condition<String> containsSomeAuthFailure =
        new Condition<>(notAuthForSomePermission, "D:R or C:M:L auth failure", output);
    assertThat(output).has(containsSomeAuthFailure);
  }

  @Test
  @ConnectionConfiguration(user = "dataReadThis_test_region", password = "dataReadThis_test_region")
  public void testInvalidPermissionsForDumpDirectoryFilesWithRegionParameter_withDataReadRegion() {
    Function thisFunction = dumpDirectoryFiles;
    ResourcePermission thisMissingPermission = CLUSTER_MANAGE;

    gfsh.executeAndAssertThat(
        "execute function  --region=" + regionName + " --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisMissingPermission.toString()).statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "clusterManage", password = "clusterManage")
  public void testInvalidPermissionsForDumpDirectoryFilesWithRegionParameter_withClusterManage() {
    Function thisFunction = dumpDirectoryFiles;
    ResourcePermission thisMissingPermission = DATA_READ;

    gfsh.executeAndAssertThat(
        "execute function  --region=" + regionName + " --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisMissingPermission.toString()).statusIsError();
  }


  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneSearchIndexFunctionWithoutRegionParameter() {
    Function thisFunction = luceneSearchIndexFunction;
    ResourcePermission thisRequiredPermission = DATA_READ;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString()).statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneSearchIndexFunctionWithRegionParameter() {
    Function thisFunction = luceneSearchIndexFunction;
    ResourcePermission thisRequiredPermission = DATA_READ_REGION;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString()).statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneQueryFunctionWithoutRegionParameter() {
    Function thisFunction = luceneQueryFunction;
    ResourcePermission thisRequiredPermission = DATA_READ;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString()).statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneQueryFunctionWithRegionParameter() {
    Function thisFunction = luceneQueryFunction;
    ResourcePermission thisRequiredPermission = DATA_READ_REGION;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString()).statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForWaitUntilFlushedFunctionWithoutRegionParameter() {
    Function thisFunction = waitUntilFlushedFunction;
    ResourcePermission thisRequiredPermission = DATA_READ;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString()).statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForWaitUntilFlushedFunctionWithRegionParameter() {
    Function thisFunction = waitUntilFlushedFunction;
    ResourcePermission thisRequiredPermission = DATA_READ_REGION;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString()).statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneGetPageFunctionWithoutRegionParameter() {
    Function thisFunction = luceneGetPageFunction;
    ResourcePermission thisRequiredPermission = DATA_READ;

    gfsh.executeAndAssertThat("execute function --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString()).statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "noPermissions", password = "noPermissions")
  public void testInvalidPermissionsForLuceneGetPageFunctionWithRegionParameter() {
    Function thisFunction = luceneGetPageFunction;
    ResourcePermission thisRequiredPermission = DATA_READ_REGION;

    gfsh.executeAndAssertThat(
        "execute function --region=" + regionName + " --id=" + thisFunction.getId())
        .containsOutput("not authorized for " + thisRequiredPermission.toString()).statusIsError();
  }
}
