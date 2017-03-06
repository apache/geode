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
 *
 */

package org.apache.geode.management.internal.cli.util;

import static java.util.stream.Collectors.toList;

import org.apache.commons.io.FileUtils;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * this LogExporter only finds the .log and .gfs files under in the same directory of the base files
 * it doesn't use the base file's filename patterns to find the related logs/stats yet.
 */
public class LogExporter {
  private static final Logger LOGGER = LogService.getLogger();

  private final LogFilter logFilter;
  private final File baseLogFile;
  private final File baseStatsFile;

  /**
   * @param logFilter: the filter that's used to check if we need to accept the file or the logLine
   * @param baseLogFile: if not null, we will export the logs in that directory
   * @param baseStatsFile: if not null, we will export stats in that directory
   * @throws ParseException
   */
  public LogExporter(LogFilter logFilter, File baseLogFile, File baseStatsFile)
      throws ParseException {
    assert logFilter != null;
    this.logFilter = logFilter;
    this.baseLogFile = baseLogFile;
    this.baseStatsFile = baseStatsFile;
  }

  /**
   *
   * @return Path to the zip file that has all the filtered files, null if no files are selected to
   *         export.
   * @throws IOException
   */
  public Path export() throws IOException {
    Path tempDirectory = Files.createTempDirectory("exportLogs");

    if (baseLogFile != null) {
      for (Path logFile : findLogFiles(baseLogFile.toPath().getParent())) {
        Path filteredLogFile = tempDirectory.resolve(logFile.getFileName());
        writeFilteredLogFile(logFile, filteredLogFile);
      }
    }

    if (baseStatsFile != null) {
      for (Path statFile : findStatFiles(baseStatsFile.toPath().getParent())) {
        Files.copy(statFile, tempDirectory.resolve(statFile.getFileName()));
      }
    }

    Path zipFile = null;
    if (tempDirectory.toFile().listFiles().length > 0) {
      zipFile = Files.createTempFile("logExport", ".zip");
      ZipUtils.zipDirectory(tempDirectory, zipFile);
      LOGGER.info("Zipped files to: " + zipFile);
    }

    FileUtils.deleteDirectory(tempDirectory.toFile());

    return zipFile;
  }

  protected void writeFilteredLogFile(Path originalLogFile, Path filteredLogFile)
      throws IOException {
    this.logFilter.startNewFile();

    try (BufferedReader reader = new BufferedReader(new FileReader(originalLogFile.toFile()))) {
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(filteredLogFile.toFile()))) {

        String line;
        while ((line = reader.readLine()) != null) {
          LogFilter.LineFilterResult result = this.logFilter.acceptsLine(line);

          if (result == LogFilter.LineFilterResult.REMAINDER_OF_FILE_REJECTED) {
            break;
          }

          if (result == LogFilter.LineFilterResult.LINE_ACCEPTED) {
            writeLine(line, writer);
          }
        }
      }
    }
  }

  private void writeLine(String line, BufferedWriter writer) {
    try {
      writer.write(line);
      writer.newLine();
    } catch (IOException e) {
      throw new RuntimeException("Unable to write to log file", e);
    }
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
    Stream<Path> selectedFiles =
        Files.list(workingDir).filter(fileSelector).filter(this.logFilter::acceptsFile);

    return selectedFiles.collect(toList());
  }


}
