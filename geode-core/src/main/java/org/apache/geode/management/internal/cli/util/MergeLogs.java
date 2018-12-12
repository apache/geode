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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.MergeLogFiles;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/**
 * @since GemFire 7.0
 */

public class MergeLogs {
  private static final Logger logger = LogService.getLogger();

  public static void main(String[] args) {
    if (args.length < 1 || args.length > 1) {
      throw new IllegalArgumentException("Requires only 1  arguments : <targetDirName>");
    }
    try {
      String result = mergeLogFile(args[0]).getCanonicalPath();
      System.out.println("Merged logs to: " + result);
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  public static void mergeLogsInNewProcess(Path logDirectory) {
    // create a new process for merging
    logger.info("Exporting logs merging logs" + logDirectory);
    List<String> commandList = new ArrayList<String>();
    commandList.add(
        System.getProperty("java.home") + File.separatorChar + "bin" + File.separatorChar + "java");
    commandList.add("-classpath");
    commandList.add(System.getProperty("java.class.path", "."));
    commandList.add(MergeLogs.class.getName());

    commandList.add(logDirectory.toAbsolutePath().toString());

    ProcessBuilder procBuilder = new ProcessBuilder(commandList);
    StringBuilder output = new StringBuilder();
    try {
      logger.info("Exporting logs now merging logs");
      Process mergeProcess = procBuilder.redirectErrorStream(true).start();

      mergeProcess.waitFor();

      InputStream inputStream = mergeProcess.getInputStream();
      BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
      String line = null;

      while ((line = br.readLine()) != null) {
        output.append(line).append(GfshParser.LINE_SEPARATOR);
      }
      mergeProcess.destroy();
    } catch (Exception e) {
      logger.error(e.getMessage());
    }
    if (output.toString().contains("Merged logs to: ")) {
      logger.info("Exporting logs successfully merged logs");
    } else {
      logger.error("Could not merge");
    }
  }

  protected static List<File> findLogFilesToMerge(File dir) {
    return FileUtils.listFiles(dir, new String[] {"log"}, true).stream().collect(toList());
  }

  static File mergeLogFile(String dirName) throws Exception {
    Path dir = Paths.get(dirName);
    List<File> logsToMerge = findLogFilesToMerge(dir.toFile());
    Map<String, InputStream> logFiles = new HashMap<>();
    for (int i = 0; i < logsToMerge.size(); i++) {
      try {
        logFiles.put(dir.relativize(logsToMerge.get(i).toPath()).toString(),
            new FileInputStream(logsToMerge.get(i)));
      } catch (FileNotFoundException e) {
        throw new Exception(
            logsToMerge.get(i) + " " + CliStrings.EXPORT_LOGS__MSG__FILE_DOES_NOT_EXIST);
      }
    }

    PrintWriter mergedLog = null;
    File mergedLogFile = null;
    try {
      String mergeLog = dirName + File.separator + "merge_"
          + new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new java.util.Date()) + ".log";
      mergedLogFile = new File(mergeLog);
      mergedLog = new PrintWriter(mergedLogFile);
      MergeLogFiles.mergeLogFiles(logFiles, mergedLog);
    } catch (FileNotFoundException e) {
      throw new Exception(
          "FileNotFoundException in creating PrintWriter in MergeLogFiles" + e.getMessage());
    } catch (Exception e) {
      throw new Exception("Exception in creating PrintWriter in MergeLogFiles" + e.getMessage());
    }

    return mergedLogFile;
  }


}
