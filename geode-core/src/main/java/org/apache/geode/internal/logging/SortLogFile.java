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
package org.apache.geode.internal.logging;

import static org.apache.commons.lang.SystemUtils.LINE_SEPARATOR;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.geode.internal.ExitCode;

/**
 * This program sorts the entries in a GemFire log file (one written using a
 * {@link org.apache.geode.LogWriter}) by their timestamps. Note that in order to do so, we
 * have to read the entire file into memory.
 *
 * @see MergeLogFiles
 * @see LogFileParser
 *
 *
 * @since GemFire 3.0
 */
public class SortLogFile {
  private static PrintStream out = System.out;
  private static PrintStream err = System.err;

  /**
   * Parses a log file from a given source and writes the sorted entries to a given destination.
   */
  public static void sortLogFile(InputStream logFile, PrintWriter sortedFile) throws IOException {

    SortedSet sorted = new TreeSet(new Comparator() {
      public int compare(Object o1, Object o2) {
        LogFileParser.LogEntry entry1 = (LogFileParser.LogEntry) o1;
        LogFileParser.LogEntry entry2 = (LogFileParser.LogEntry) o2;
        String stamp1 = entry1.getTimestamp();
        String stamp2 = entry2.getTimestamp();

        if (stamp1.equals(stamp2)) {
          if (entry1.getContents().equals(entry2.getContents())) {
            // Timestamps and contents are both equal - compare hashCode()
            return Integer.valueOf(entry1.hashCode()).compareTo(Integer.valueOf(entry2.hashCode()));
          } else {
            return entry1.getContents().compareTo(entry2.getContents());
          }
        } else {
          return stamp1.compareTo(stamp2);
        }
      }
    });

    BufferedReader br = new BufferedReader(new InputStreamReader(logFile));
    LogFileParser parser = new LogFileParser(null, br);
    while (parser.hasMoreEntries()) {
      sorted.add(parser.getNextEntry());
    }

    for (Iterator iter = sorted.iterator(); iter.hasNext();) {
      LogFileParser.LogEntry entry = (LogFileParser.LogEntry) iter.next();
      entry.writeTo(sortedFile);
    }
  }

  //////////////////// Main Program ////////////////////

  /**
   * Prints usage information about this program
   */
  private static void usage(String s) {
    err.println(LINE_SEPARATOR + "** " + s + LINE_SEPARATOR);
    err.println(
        "Usage: java SortLogFile logFile");
    err.println("-sortedFile file "
        + "File in which to put sorted log");
    err.println("");
    err.println(
        "Sorts a GemFire log file by timestamp. The merged log file is written to System.out (or a file).");
    err.println("");
    ExitCode.FATAL.doSystemExit();
  }

  public static void main(String[] args) throws IOException {
    File logFile = null;
    File sortedFile = null;
    // int dirCount = 0;

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-sortedFile")) {
        if (++i >= args.length) {
          usage("Missing sorted file name");
        }

        sortedFile = new File(args[i]);

      } else if (logFile == null) {
        File file = new File(args[i]);
        if (!file.exists()) {
          usage(String.format("File %s does not exist", file));
        }

        logFile = file;

      } else {
        usage(String.format("Extraneous command line: %s", args[i]));
      }
    }

    if (logFile == null) {
      usage("Missing filename");
    }

    InputStream logFileStream = new FileInputStream(logFile);

    PrintStream ps;
    if (sortedFile != null) {
      ps = new PrintStream(new FileOutputStream(sortedFile), true);

    } else {
      ps = out;
    }

    PrintWriter pw = new PrintWriter(ps, true);

    sortLogFile(logFileStream, pw);

    ExitCode.NORMAL.doSystemExit();
  }

}
