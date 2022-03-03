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
package org.apache.geode.session.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;

public class LogChecker {
  private static final List<String> suspectStrings;
  private static final List<String> excludeStrings;
  private static final Logger logger = LogService.getLogger();

  static {
    suspectStrings = new ArrayList<>();
    suspectStrings.add(java.lang.ClassCastException.class.getName());
    suspectStrings.add(java.lang.NullPointerException.class.getName());
    excludeStrings = new ArrayList<>();
    excludeStrings.add("[fine");
  }

  static void checkLogs(File dir) {
    List<File> logsToCheck = getLogs(dir);
    checkLogs(logsToCheck);
  }

  private static List<File> getLogs(File currentDir) {
    logger.info("Getting all logs visible from " + currentDir);
    List<File> logList = new ArrayList<>();
    getLogs(currentDir, logList);
    return logList;
  }

  private static void getLogs(File currentDir, List<File> logs) {
    File[] dirContents = currentDir.listFiles();
    if (dirContents == null) {
      return;
    }
    for (File aFile : dirContents) {
      if (aFile.isDirectory()) {
        getLogs(aFile, logs);
      } else {
        String fileName = aFile.getName();
        if (fileName.startsWith("container") && fileName.endsWith(".log")) {
          logs.add(aFile);
        } else if (fileName.startsWith("gemfire") && fileName.endsWith(".log")) {
          logs.add(aFile);
        }
      }
    }
  }

  private static void checkLogs(List<File> logsToCheck) {
    BufferedReader reader = null;
    String line;
    for (File aFile : logsToCheck) {
      logger.info("Checking " + aFile.getAbsolutePath());
      try {
        try {
          reader = new BufferedReader(new FileReader(aFile.getAbsoluteFile()));
        } catch (FileNotFoundException e) {
          throw new RuntimeException(e);
        }
        line = readNextLine(reader);
        while (line != null) {
          if (contains(suspectStrings, line) && !contains(excludeStrings, line)) {
            throw new RuntimeException(aFile.getAbsolutePath() + " contains " + line + "\n");
          }
          line = readNextLine(reader);
        }
      } finally {
        close(reader);
      }
    }
  }

  private static String readNextLine(BufferedReader reader) {
    String line;
    try {
      line = reader.readLine();
      return line;
    } catch (IOException e) {
      logger.info("Caught " + e);
      return null;
    }
  }

  private static boolean contains(List<String> targetStrs, String aStr) {
    for (String target : targetStrs) {
      if (aStr.contains(target)) {
        return true;
      }
    }
    return false;
  }

  private static void close(BufferedReader reader) {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        logger.info("Caught " + e + " while closing " + reader);
      }
    }
  }
}
