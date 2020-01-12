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
package org.apache.geode.management.internal.cli.commands.lifecycle;

import static org.apache.geode.internal.Assert.assertState;
import static org.apache.geode.internal.process.ProcessStreamReader.waitAndCaptureProcessStandardErrorStream;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.internal.util.IOUtils;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.commands.OfflineGfshCommand;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;

public class StartVsdCommand extends OfflineGfshCommand {
  @CliCommand(value = CliStrings.START_VSD, help = CliStrings.START_VSD__HELP)
  @CliMetaData(shellOnly = true,
      relatedTopic = {CliStrings.TOPIC_GEODE_M_AND_M, CliStrings.TOPIC_GEODE_STATISTICS})
  public ResultModel startVsd(@CliOption(key = CliStrings.START_VSD__FILE,
      help = CliStrings.START_VSD__FILE__HELP) final String[] statisticsArchiveFilePathnames)
      throws Exception {

    String geodeHome = System.getenv("GEODE_HOME");

    assertState(org.apache.commons.lang3.StringUtils.isNotBlank(geodeHome),
        CliStrings.GEODE_HOME_NOT_FOUND_ERROR_MESSAGE);

    assertState(IOUtils.isExistingPathname(getPathToVsd()),
        String.format(CliStrings.START_VSD__NOT_FOUND_ERROR_MESSAGE, geodeHome));

    String[] vsdCommandLine = createdVsdCommandLine(statisticsArchiveFilePathnames);

    if (isDebugging()) {
      getGfsh().printAsInfo(
          String.format("GemFire VSD command-line (%1$s)", Arrays.toString(vsdCommandLine)));
    }

    Process vsdProcess = Runtime.getRuntime().exec(vsdCommandLine);

    getGfsh().printAsInfo(CliStrings.START_VSD__RUN);

    String vsdProcessOutput = waitAndCaptureProcessStandardErrorStream(vsdProcess);

    ResultModel result = new ResultModel();
    InfoResultModel info = result.addInfo("info");

    if (org.apache.commons.lang3.StringUtils.isNotBlank(vsdProcessOutput)) {
      info.addLine(vsdProcessOutput);
    }

    return result;
  }

  protected String[] createdVsdCommandLine(final String[] statisticsArchiveFilePathnames)
      throws FileNotFoundException {
    List<String> commandLine = new ArrayList<>();

    commandLine.add(getPathToVsd());
    commandLine.addAll(processStatisticsArchiveFiles(statisticsArchiveFilePathnames));

    return commandLine.toArray(new String[0]);
  }

  protected String getPathToVsd() {
    String vsdPathname =
        IOUtils.appendToPath(System.getenv("GEODE_HOME"), "tools", "vsd", "bin", "vsd");

    if (SystemUtils.isWindows()) {
      vsdPathname += ".bat";
    }

    return vsdPathname;
  }

  protected Set<String> processStatisticsArchiveFiles(final String[] statisticsArchiveFilePathnames)
      throws FileNotFoundException {
    Set<String> statisticsArchiveFiles = new TreeSet<>();

    if (statisticsArchiveFilePathnames != null) {
      for (String pathname : statisticsArchiveFilePathnames) {
        File path = new File(pathname);

        if (path.exists()) {
          if (path.isFile()) {
            if (StatisticsArchiveFileFilter.INSTANCE.accept(path)) {
              statisticsArchiveFiles.add(pathname);
            } else {
              throw new IllegalArgumentException(
                  "A Statistics Archive File must end with a .gfs file extension.");
            }
          } else { // the File (path) is a directory
            processStatisticsArchiveFiles(path, statisticsArchiveFiles);
          }
        } else {
          throw new FileNotFoundException(String.format(
              "The pathname (%1$s) does not exist.  Please check the path and try again.",
              path.getAbsolutePath()));
        }
      }
    }

    return statisticsArchiveFiles;
  }

  @SuppressWarnings("null")
  protected void processStatisticsArchiveFiles(final File path,
      final Set<String> statisticsArchiveFiles) {
    if (path != null && path.isDirectory()) {
      for (File file : path.listFiles(StatisticsArchiveFileAndDirectoryFilter.INSTANCE)) {
        if (file.isDirectory()) {
          processStatisticsArchiveFiles(file, statisticsArchiveFiles);
        } else if (StatisticsArchiveFileFilter.INSTANCE.accept(file)) {
          statisticsArchiveFiles.add(file.getAbsolutePath());
        }
      }
    }
  }


  protected static class StatisticsArchiveFileFilter implements FileFilter {

    @Immutable
    protected static final StatisticsArchiveFileFilter INSTANCE = new StatisticsArchiveFileFilter();

    @Override
    public boolean accept(final File pathname) {
      return (pathname.isFile() && pathname.getAbsolutePath().endsWith(".gfs"));
    }
  }

  protected static class StatisticsArchiveFileAndDirectoryFilter
      extends StatisticsArchiveFileFilter {

    @Immutable
    protected static final StatisticsArchiveFileAndDirectoryFilter INSTANCE =
        new StatisticsArchiveFileAndDirectoryFilter();

    @Override
    public boolean accept(final File pathname) {
      return (pathname.isDirectory() || super.accept(pathname));
    }
  }
}
