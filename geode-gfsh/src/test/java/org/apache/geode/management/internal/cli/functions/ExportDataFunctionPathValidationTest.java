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

package org.apache.geode.management.internal.cli.functions;

import static org.apache.geode.management.internal.cli.functions.ExportDataFunction.EXPORT_DATA_DIRS_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.management.internal.functions.CliFunctionResult;

/**
 * Tests the directories a member permits {@code export data} to write into.
 */
public class ExportDataFunctionPathValidationTest {

  private static final String REGION = "testRegion";

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private ExportDataFunction function;
  private RegionSnapshotService<Object, Object> snapshotService;
  private FunctionContext<String[]> context;

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    function = new ExportDataFunction();

    snapshotService = mock(RegionSnapshotService.class);
    Region<Object, Object> region = mock(Region.class);
    when(region.getSnapshotService()).thenReturn(snapshotService);

    InternalCacheForClientAccess clientCache = mock(InternalCacheForClientAccess.class);
    when(clientCache.getRegion(REGION)).thenReturn(region);

    InternalCache cache = mock(InternalCache.class);
    when(cache.getCacheForProcessingClientRequests()).thenReturn(clientCache);

    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(member.getHost()).thenReturn("localhost");
    InternalDistributedSystem system = mock(InternalDistributedSystem.class);
    when(system.getDistributedMember()).thenReturn(member);
    when(clientCache.getDistributedSystem()).thenReturn(system);

    context = mock(FunctionContext.class);
    when(context.getCache()).thenReturn(cache);
    when(context.getMemberName()).thenReturn("server1");
  }

  /** Permits exports into the temporary folder, in addition to the working directory. */
  private Path permitTemporaryFolder() {
    Path permitted = temporaryFolder.getRoot().toPath();
    System.setProperty(EXPORT_DATA_DIRS_PROPERTY, permitted.toString());
    return permitted;
  }

  private CliFunctionResult export(String requestedPath) throws Exception {
    when(context.getArguments())
        .thenReturn(new String[] {REGION, requestedPath, Boolean.toString(false)});
    return function.executeFunction(context);
  }

  private File captureExportFile() throws Exception {
    ArgumentCaptor<File> fileCaptor = ArgumentCaptor.forClass(File.class);
    verify(snapshotService).save(fileCaptor.capture(), eq(SnapshotFormat.GEODE));
    return fileCaptor.getValue();
  }

  private void verifyNothingWasWritten() throws Exception {
    verify(snapshotService, never()).save(any(File.class), eq(SnapshotFormat.GEODE));
  }

  /**
   * An export into a permitted directory works normally.
   */
  @Test
  public void exportIntoAPermittedDirectorySucceeds() throws Exception {
    Path permitted = permitTemporaryFolder();

    CliFunctionResult result = export(permitted.resolve("snapshot.gfd").toString());

    assertThat(result.isSuccessful()).isTrue();
    assertThat(captureExportFile().toPath())
        .isEqualTo(permitted.toRealPath().resolve("snapshot.gfd"));
  }

  /** Sub-directories of a permitted directory are permitted too. */
  @Test
  public void exportIntoASubdirectoryOfAPermittedDirectorySucceeds() throws Exception {
    Path permitted = permitTemporaryFolder();

    CliFunctionResult result = export(permitted.resolve("nested/snapshot.gfd").toString());

    assertThat(result.isSuccessful()).isTrue();
  }

  /**
   * An absolute path outside every permitted directory is refused.
   */
  @Test
  public void exportToAnAbsolutePathOutsideEveryPermittedDirectoryIsRefused() throws Exception {
    permitTemporaryFolder();

    assertThatThrownBy(() -> export("/var/tmp/snapshot.gfd"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not within the export directories configured for this member");

    verifyNothingWasWritten();
  }

  /**
   * A "../" element that climbs out of a permitted directory is refused: the path is canonicalized
   * before it is compared, so the comparison uses the location actually written to.
   */
  @Test
  public void parentReferenceOutOfAPermittedDirectoryIsRefused() throws Exception {
    Path permitted = permitTemporaryFolder();

    assertThatThrownBy(() -> export(permitted.resolve("../escaped.gfd").toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not within the export directories configured for this member");

    verifyNothingWasWritten();
  }

  /**
   * A "../" element that stays inside a permitted directory is honoured, and is resolved before
   * the write, so no "../" reaches the snapshot service.
   */
  @Test
  public void parentReferenceInsideAPermittedDirectoryIsResolvedBeforeTheWrite()
      throws Exception {
    Path permitted = permitTemporaryFolder();

    CliFunctionResult result =
        export(permitted.resolve("nested/../snapshot.gfd").toString());

    assertThat(result.isSuccessful()).isTrue();
    File exportFile = captureExportFile();
    assertThat(exportFile.getPath()).doesNotContain("..");
    assertThat(exportFile.toPath()).isEqualTo(permitted.toRealPath().resolve("snapshot.gfd"));
  }

  /**
   * With no configuration, the only permitted directory is the member's working directory - which
   * is where a relative export path lands.
   */
  @Test
  public void withoutConfigurationOnlyTheMemberWorkingDirectoryIsPermitted() throws Exception {
    System.clearProperty(EXPORT_DATA_DIRS_PROPERTY);
    Path workingDir = Paths.get(System.getProperty("user.dir")).toRealPath();

    assertThat(export(workingDir.resolve("snapshot.gfd").toString()).isSuccessful()).isTrue();

    assertThatThrownBy(() -> export(temporaryFolder.getRoot().toPath().resolve("x.gfd").toString()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * The member's working directory stays permitted when other directories are configured, so a
   * relative export path keeps working.
   */
  @Test
  public void theWorkingDirectoryRemainsPermittedWhenOtherDirectoriesAreConfigured()
      throws Exception {
    permitTemporaryFolder();

    assertThat(export("snapshot.gfd").isSuccessful()).isTrue();
    assertThat(captureExportFile().toPath())
        .isEqualTo(Paths.get(System.getProperty("user.dir")).toRealPath().resolve("snapshot.gfd"));
  }

  /**
   * More than one directory can be permitted, which is how a deployment that exports to a
   * dedicated backup location configures the member.
   */
  @Test
  public void severalDirectoriesCanBePermitted() throws Exception {
    File backup = temporaryFolder.newFolder("backup");
    File other = temporaryFolder.newFolder("other");
    System.setProperty(EXPORT_DATA_DIRS_PROPERTY,
        backup.getAbsolutePath() + File.pathSeparator + other.getAbsolutePath());

    assertThat(export(new File(backup, "snapshot.gfd").getPath()).isSuccessful()).isTrue();
    assertThat(export(new File(other, "snapshot.gfd").getPath()).isSuccessful()).isTrue();

    assertThatThrownBy(() -> export(temporaryFolder.getRoot().toPath().resolve("x.gfd").toString()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * A directory whose name merely starts with a permitted directory's name is not inside it - the
   * check compares path elements, not string prefixes.
   */
  @Test
  public void aSiblingDirectoryWithAMatchingNamePrefixIsNotPermitted() throws Exception {
    File permitted = temporaryFolder.newFolder("exports");
    File sibling = temporaryFolder.newFolder("exports-archive");
    System.setProperty(EXPORT_DATA_DIRS_PROPERTY, permitted.getAbsolutePath());

    assertThatThrownBy(() -> export(new File(sibling, "snapshot.gfd").getPath()))
        .isInstanceOf(IllegalArgumentException.class);

    verifyNothingWasWritten();
  }
}
