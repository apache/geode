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
package org.apache.geode.management.internal.cli.util;

import static java.util.stream.Collectors.toList;

import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class LogSizer {
  private static final Logger LOGGER = LogService.getLogger();

  private final LogFilter logFilter;
  private final File baseLogFile;
  private final File baseStatsFile;

  private long filteredSize;

  /**
   * @param logFilter the filter that's used to check if we need to accept the file or the logLine
   * @param baseLogFile if not null, we will export the logs in that directory
   * @param baseStatsFile if not null, we will export stats in that directory
   */
  public LogSizer(LogFilter logFilter, File baseLogFile, File baseStatsFile) throws ParseException {
    assert logFilter != null;
    this.logFilter = logFilter;
    this.baseLogFile = baseLogFile;
    this.baseStatsFile = baseStatsFile;
    filteredSize = 0;
  }

  /**
   * @return combined size of stat archives and filtered log files in bytes
   */
  public long getFilteredSize() throws IOException {

    if (baseLogFile != null) {
      for (Path logFile : findLogFiles(baseLogFile.toPath().getParent())) {
        filteredSize += filterAndSize(logFile);
      }
    }

    if (baseStatsFile != null) {
      for (Path statFile : findStatFiles(baseStatsFile.toPath().getParent())) {
        filteredSize += statFile.toFile().length();
      }
    }

    return filteredSize;
  }

  /**
   * @return size of file in bytes
   */
  protected long filterAndSize(Path originalLogFile) throws FileNotFoundException {
    long size = 0;
    Scanner in = new Scanner(originalLogFile.toFile());
    while (in.hasNextLine()) {
      String line = in.nextLine();

      LogFilter.LineFilterResult result = this.logFilter.acceptsLine(line);

      if (result == LogFilter.LineFilterResult.REMAINDER_OF_FILE_REJECTED) {
        break;
      }
      size += line.length() + File.separator.length();
    }
    return size;
  }

  protected List<Path> findLogFiles(Path workingDir) throws IOException {
    Predicate<Path> logFileSelector = (Path file) -> file.toString().toLowerCase().endsWith(".log");
    return findFiles(workingDir, logFileSelector);
  }


  protected List<Path> findStatFiles(Path workingDir) throws IOException {
    Predicate<Path> statFileSelector =
        (Path file) -> file.toString().toLowerCase().endsWith(".gfs");
    return findFiles(workingDir, statFileSelector);
  }

  private List<Path> findFiles(Path workingDir, Predicate<Path> fileSelector) throws IOException {
    Stream<Path> selectedFiles = null;
    if (!workingDir.toFile().isDirectory()) {
      return Collections.emptyList();
    }
    selectedFiles = Files.list(workingDir).filter(fileSelector).filter(this.logFilter::acceptsFile);

    return selectedFiles.collect(toList());
  }
}
