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

import java.util.Collection;
import java.util.HashSet;

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
import org.apache.geode.cache.lucene.internal.distributed.IndexingInProgressFunction;
import org.apache.geode.cache.lucene.internal.distributed.LuceneQueryFunction;
import org.apache.geode.cache.lucene.internal.distributed.WaitUntilFlushedFunction;
import org.apache.geode.cache.lucene.internal.results.LuceneGetPageFunction;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.internal.security.ResourcePermissions;
import org.apache.geode.security.ResourcePermission;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.LuceneTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class, LuceneTest.class})
public class LuceneFunctionSecurityTest {
  private static final String RESULT_HEADER = "Message";
  private static final String REGION_NAME = "testRegion";

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withJMXManager()
          .withSecurityManager(SimpleSecurityManager.class)
          .withRegion(RegionShortcut.PARTITION, REGION_NAME).withAutoStart();

  @Rule
  public GfshCommandRule gfsh =
      new GfshCommandRule(server::getJmxPort,
          GfshCommandRule.PortType.jmxManager);

  private static HashSet<Function> functions = new HashSet<>();
  private static HashSet<Function> functionsWithDataRead = new HashSet<>();

  @BeforeClass
  public static void setupFunctions() {
    functions.add(new LuceneCreateIndexFunction());
    functions.add(new LuceneDescribeIndexFunction());
    functions.add(new LuceneDestroyIndexFunction());
    functions.add(new LuceneListIndexFunction());
    functions.add(new LuceneSearchIndexFunction());
    functions.add(new LuceneQueryFunction());
    functions.add(new WaitUntilFlushedFunction());
    functions.add(new LuceneGetPageFunction());
    functions.add(new IndexingInProgressFunction());

    functions.forEach(FunctionService::registerFunction);
    FunctionService.registerFunction(new DumpDirectoryFiles());

    for (Function function : functions) {
      Collection<ResourcePermission> permissions = function
          .getRequiredPermissions(REGION_NAME);
      for (ResourcePermission permission : permissions) {
        if (permission.getResource().equals(ResourcePermission.Resource.DATA)
            && permission.getOperation().equals(ResourcePermission.Operation.READ)) {
          functionsWithDataRead.add(function);
          break;
        }
      }
    }
  }

  @Test
  @ConnectionConfiguration(user = "user", password = "user")
  public void functionRequireExpectedPermission() throws Exception {
    functions.stream().forEach(function -> {
      Collection<ResourcePermission> permissions = function
          .getRequiredPermissions(REGION_NAME);
      ResourcePermission resourcePermission = (ResourcePermission) permissions
          .toArray()[0];
      String permission = resourcePermission.toString();

      gfsh.executeAndAssertThat(
          "execute function --region=" + REGION_NAME + " --id=" + function.getId())
          .tableHasRowCount(1)
          .tableHasRowWithValues(RESULT_HEADER, "Exception: user not authorized for " + permission)
          .statusIsError();
    });
  }

  @Test
  @ConnectionConfiguration(user = "DATAREAD", password = "DATAREAD")
  public void functionRequireExpectedReadPermissionToPass() throws Exception {
    IgnoredException.addIgnoredException("did not send last result");
    assertThat(functionsWithDataRead).isNotEmpty();

    functionsWithDataRead.stream().forEach(function -> {
      String permission = "*";
      Collection<ResourcePermission> permissions = function
          .getRequiredPermissions(REGION_NAME);
      for (ResourcePermission resourcePermission : permissions) {
        if (permission.equals(ResourcePermissions.DATA_READ)) {
          permission = resourcePermission.toString();
        }
      }
      gfsh.executeAndAssertThat(
          "execute function --region=" + REGION_NAME + " --id=" + function.getId())
          .doesNotContainOutput("Exception: user not authorized for ")
          .statusIsError();
    });
  }

  @Test
  @ConnectionConfiguration(user = "user", password = "user")
  public void functionWithoutExpectedPermissionWillFail() throws Exception {
    IgnoredException.addIgnoredException("did not send last result");
    assertThat(functionsWithDataRead).isNotEmpty();

    functionsWithDataRead.stream().forEach(function -> {
      Collection<ResourcePermission> permissions = function
          .getRequiredPermissions(REGION_NAME);
      ResourcePermission resourcePermission = (ResourcePermission) permissions
          .toArray()[0];
      String permission = resourcePermission.toString();
      gfsh.executeAndAssertThat(
          "execute function --region=" + REGION_NAME + " --id=" + function.getId())
          .statusIsError().hasTableSection().hasRowSize(1).hasRow(0).asList().last()
          .isEqualTo("Exception: user not authorized for " + permission);
    });
  }

  // use DumpDirectoryFile function to verify that all the permissions returned by the
  // getRequiredPermission are all enforced before trying to execute
  @Test
  @ConnectionConfiguration(user = "clusterManage", password = "clusterManage")
  public void dumpDirectoryFileRequiresAll_insufficientUser() {
    gfsh.executeAndAssertThat(
        "execute function --region=" + REGION_NAME + " --id=" + DumpDirectoryFiles.ID)
        .tableHasRowCount(1)
        .tableHasRowWithValues(RESULT_HEADER,
            "Exception: clusterManage not authorized for *")
        .statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "*", password = "*")
  public void dumpDirectoryFileRequiresAll_validUser() {
    gfsh.executeAndAssertThat(
        "execute function --region=" + REGION_NAME + " --id=" + DumpDirectoryFiles.ID)
        .tableHasRowCount(1).doesNotContainOutput("not authorized")
        .statusIsError();
  }
}
