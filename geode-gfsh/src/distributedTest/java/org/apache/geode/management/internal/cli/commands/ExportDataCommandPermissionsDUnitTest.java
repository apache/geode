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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.management.internal.cli.functions.ExportDataFunction.EXPORT_DATA_DIRS_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

/**
 * Tests which principals may run {@code export data} in a secured cluster.
 *
 * <p>
 * {@link SimpleSecurityManager} authorizes a user for exactly those permissions whose string form
 * starts with the user name, and treats a comma separated user name as a set of roles. So
 * "dataRead" holds DATA:READ alone, while "dataRead,clusterWrite" is the operator {@code
 * export data} requires.
 */
@Category(SecurityTest.class)
public class ExportDataCommandPermissionsDUnitTest implements Serializable {

  private static final String REGION_NAME = "testRegion";
  private static final String READ_ONLY_USER = "dataRead";
  private static final String EXPORT_OPERATOR = "dataRead,clusterWrite";

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static MemberVM locator;
  private static MemberVM server;

  /** The directory the server has been configured to permit exports into. */
  private Path permittedDir;

  /** Any other location on the server host. */
  private Path otherDir;

  @BeforeClass
  public static void beforeClass() {
    Properties locatorProps = new Properties();
    locatorProps.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    locator = cluster.startLocatorVM(0, locatorProps);

    Properties serverProps = new Properties();
    serverProps.setProperty(ResourceConstants.USER_NAME, "clusterManage");
    serverProps.setProperty(ResourceConstants.PASSWORD, "clusterManage");
    server = cluster.startServerVM(1, serverProps, locator.getPort());

    server.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME).put("key", "value");
    });
  }

  @Before
  public void configurePermittedExportDirectory() throws Exception {
    // Refusing an export is logged at error level on the member; that is the expected outcome of
    // most of these tests, not a symptom of one going wrong.
    IgnoredException.addIgnoredException("Cannot export to");

    permittedDir = temporaryFolder.newFolder("permitted").toPath();
    otherDir = temporaryFolder.newFolder("other").toPath();

    String permitted = permittedDir.toString();
    server.invoke(() -> System.setProperty(EXPORT_DATA_DIRS_PROPERTY, permitted));
  }

  @After
  public void clearPermittedExportDirectory() {
    server.invoke(() -> System.clearProperty(EXPORT_DATA_DIRS_PROPERTY));
  }

  private void connectAs(String user) throws Exception {
    gfsh.secureConnectAndVerify(locator.getPort(), GfshCommandRule.PortType.locator, user, user);
  }

  private String exportTo(String option, Path path) {
    return "export data --member=" + server.getName() + " --region=" + REGION_NAME + " --"
        + option + "=" + path;
  }

  /**
   * Read access to region data on its own does not permit an export.
   */
  @Test
  public void dataReadUserCannotExport() throws Exception {
    connectAs(READ_ONLY_USER);
    Path target = permittedDir.resolve("snapshot.gfd");

    gfsh.executeAndAssertThat(exportTo("file", target))
        .statusIsError()
        .containsOutput("not authorized for CLUSTER:WRITE");

    assertThat(target).doesNotExist();
  }

  /**
   * Permissions are checked before the path, so the target directory makes no difference.
   */
  @Test
  public void dataReadUserIsRefusedForAnyDirectory() throws Exception {
    connectAs(READ_ONLY_USER);
    Path target = otherDir.resolve("snapshot.gfd");

    gfsh.executeAndAssertThat(exportTo("file", target))
        .statusIsError()
        .containsOutput("not authorized for CLUSTER:WRITE");

    assertThat(target).doesNotExist();
  }

  /**
   * The command works for a principal holding both permissions.
   */
  @Test
  public void operatorWithClusterWriteCanExportIntoThePermittedDirectory() throws Exception {
    connectAs(EXPORT_OPERATOR);
    Path target = permittedDir.resolve("snapshot.gfd");

    gfsh.executeAndAssertThat(exportTo("file", target))
        .statusIsSuccess()
        .containsOutput("Data successfully exported");

    assertThat(target).exists();
  }

  /**
   * The directory restriction applies independently of the permission: even a permitted operator
   * cannot place the snapshot anywhere it likes.
   */
  @Test
  public void operatorCannotExportOutsideThePermittedDirectory() throws Exception {
    connectAs(EXPORT_OPERATOR);
    Path target = otherDir.resolve("snapshot.gfd");

    gfsh.executeAndAssertThat(exportTo("file", target)).statusIsError();

    assertThat(target).doesNotExist();
  }

  /**
   * Nor can the operator leave the permitted directory with "../".
   */
  @Test
  public void operatorCannotLeavePermittedDirectoryWithParentReference() throws Exception {
    connectAs(EXPORT_OPERATOR);
    Path withParentReference = permittedDir.resolve("..").resolve("other");

    gfsh.executeAndAssertThat(exportTo("dir", withParentReference)).statusIsError();

    assertThat(otherDir.resolve(REGION_NAME + ".gfd")).doesNotExist();
  }

  /**
   * An existing file outside the permitted directory survives an export aimed at it.
   */
  @Test
  public void existingFileOutsideThePermittedDirectoryIsNotOverwritten() throws Exception {
    connectAs(EXPORT_OPERATOR);
    Path existingFile = otherDir.resolve("existing.gfd");
    String originalContent = "existing content";
    Files.write(existingFile, originalContent.getBytes(StandardCharsets.UTF_8));

    gfsh.executeAndAssertThat(exportTo("file", existingFile)).statusIsError();

    assertThat(new String(Files.readAllBytes(existingFile), StandardCharsets.UTF_8))
        .isEqualTo(originalContent);
  }

  /**
   * Confirms the read only grant really is read only, so the refusals above are the permission
   * check taking effect rather than a misconfigured principal.
   */
  @Test
  public void readOnlyUserCanStillReadData() throws Exception {
    connectAs(READ_ONLY_USER);

    gfsh.executeAndAssertThat("get --region=" + REGION_NAME + " --key=key").statusIsSuccess();
    gfsh.executeAndAssertThat("put --region=" + REGION_NAME + " --key=k --value=v")
        .statusIsError()
        .containsOutput("dataRead not authorized for DATA:WRITE");
  }
}
