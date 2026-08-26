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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.snapshot.RegionSnapshotService;
import org.apache.geode.cache.snapshot.SnapshotOptions;
import org.apache.geode.cache.snapshot.SnapshotOptions.SnapshotFormat;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.snapshot.SnapshotOptionsImpl;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.util.internal.GeodeGlossary;

/***
 * Function which carries out the export of a region to a file on a member. Uses the
 * RegionSnapshotService to export the data
 *
 * <p>
 * Export destinations are resolved to their canonical form and must be within the export
 * directories configured for this member.
 */
public class ExportDataFunction extends CliFunction<String[]> {
  private static final long serialVersionUID = 1L;

  private static final String ID =
      "org.apache.geode.management.internal.cli.functions.ExportDataFunction";

  /**
   * System property naming additional directories this member writes {@code export data} snapshots
   * into. Several directories may be listed, separated by {@link File#pathSeparator}. Exports into
   * sub-directories of a configured directory are included.
   *
   * <p>
   * The member's working directory is always configured, since that is where a relative export
   * path resolves to, so when this property is not set it is the only export destination.
   */
  public static final String EXPORT_DATA_DIRS_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + "export.data.dirs";

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<String[]> context) throws Exception {
    final String[] args = context.getArguments();
    if (args.length < 3) {
      throw new IllegalStateException(
          "Arguments length does not match required length. Export command may have been sent from incompatible older version");
    }
    final String regionName = args[0];
    final String fileName = args[1];
    final boolean parallel = Boolean.parseBoolean(args[2]);
    CliFunctionResult result;

    Cache cache = ((InternalCache) context.getCache()).getCacheForProcessingClientRequests();
    Region<Object, Object> region = cache.getRegion(regionName);
    String hostName = cache.getDistributedSystem().getDistributedMember().getHost();
    if (region != null) {
      RegionSnapshotService<Object, Object> snapshotService = region.getSnapshotService();
      final File exportFile = resolveExportFile(fileName);
      if (parallel) {
        SnapshotOptions<Object, Object> options = new SnapshotOptionsImpl<>().setParallelMode(true);
        snapshotService.save(exportFile, SnapshotFormat.GEODE, options);
      } else {
        snapshotService.save(exportFile, SnapshotFormat.GEODE);
      }

      String successMessage = CliStrings.format(CliStrings.EXPORT_DATA__SUCCESS__MESSAGE,
          regionName, exportFile.getCanonicalPath(), hostName);
      result = new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.OK,
          successMessage);
    } else {
      result = new CliFunctionResult(context.getMemberName(), CliFunctionResult.StatusState.ERROR,
          CliStrings.format(CliStrings.REGION_NOT_FOUND, regionName));
    }

    return result;
  }

  /**
   * Resolves the requested export path against the export directories configured for this member.
   *
   * @param fileName the path requested by the caller, which may be relative or absolute
   * @return the canonical file to export to
   * @throws IllegalArgumentException if the path is not within a configured export directory
   */
  static File resolveExportFile(String fileName) throws IOException {
    File exportFile = new File(fileName).getCanonicalFile();
    List<File> exportDirs = configuredExportDirs();

    for (File exportDir : exportDirs) {
      if (exportFile.toPath().startsWith(exportDir.toPath())) {
        return exportFile;
      }
    }

    throw new IllegalArgumentException(String.format(
        "Cannot export to %s: the path is not within the export directories configured for this member (%s). Use the %s system property to configure additional directories.",
        exportFile, exportDirs, EXPORT_DATA_DIRS_PROPERTY));
  }

  private static List<File> configuredExportDirs() throws IOException {
    List<File> exportDirs = new ArrayList<>();
    exportDirs.add(new File(System.getProperty("user.dir")).getCanonicalFile());

    String configuredDirs = System.getProperty(EXPORT_DATA_DIRS_PROPERTY);
    if (configuredDirs != null) {
      for (String configuredDir : configuredDirs.split(File.pathSeparator)) {
        if (!configuredDir.trim().isEmpty()) {
          exportDirs.add(new File(configuredDir.trim()).getCanonicalFile());
        }
      }
    }

    return exportDirs;
  }
}
