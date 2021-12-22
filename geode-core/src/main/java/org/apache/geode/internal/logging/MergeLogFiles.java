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

import static java.lang.System.lineSeparator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.geode.LogWriter;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.logging.LogFileParser.LogEntry;

/**
 * This program merges entries from multiple GemFire log files (those written using a
 * {@link LogWriter} together, sorting them by their timestamp. Note that this program assumes that
 * the entries in the individual log files are themselves sorted by timestamp.
 *
 * <p>
 * MergeLogFiles has several command line options:<br>
 * <br>
 * <b>-pids</b> tells the program to look for hydra-style naming and pick out process identifiers,
 * then use these to distinguish log entries instead of full log file names.<br>
 * <br>
 * <b>-dirCount x</b> when <i>-pids</i> is not being used, or it is being used and a process ID
 * can't be found, the <i>-dirCount</i> option instructs the program as to how many parent directory
 * names should be included in the name of each log file.<br>
 * <br>
 * <b>-noblanks</b> tells the program to suppress blank lines found in log files<br>
 * <br>
 * <b>-align</b> tells the program to align log content not having timestamps with log content that
 * does have timestamps.<br>
 * <br>
 * <b>-threads</b> tells the program to attempt to make use of multiple CPUs
 *
 * <p>
 * The <i>-pids</i> option will cause a file nickname table to be built and emitted at the beginning
 * of the merged log files. Nicknames are of the form pid-x, where <i>pid</i> is the process ID that
 * emitted the log entry, and <i>x</i> is the position of this log file in the ordered set of log
 * files created by that process.
 *
 * <p>
 * Normally, one log file reader is created per log file, and they are iterated over repeatedly to
 * find and write the next earliest timestamped line to the output stream. Text without a timestamp
 * is associated with the prior timestamped line in the file.
 *
 * <p>
 * The <i>-threads</i> option will cause the program to also create threads for each reader that are
 * backed by bounded {@link BlockingQueue queues}, as outlined in the diagram below. This can
 * consume more memory, so it is wise to increase the Xmx of the java virtual machine if you are
 * going to use this option.
 *
 * @see SortLogFile
 * @see LogFileParser
 *
 * @since GemFire 2.0 (-pids, -threads, -align, and -noblanks added in 5.1)
 */
public class MergeLogFiles {

  @Immutable
  private static final PrintStream out = System.out;
  @Immutable
  private static final PrintStream err = System.err;

  /**
   * Merges the log files from a given set of {@code InputStream}s into a
   * {@code PrintWriter}.
   *
   * @param logFiles The log files to be merged
   * @param mergedFile Where the merged logs are printed to
   *
   * @return Whether or not problems occurred while merging the log files.
   *
   * @throws IllegalArgumentException If the length of {@code logFiles} is not the same as the
   *         length of {@code logFileNames}
   */
  public static boolean mergeLogFiles(final Map<String, InputStream> logFiles,
      final PrintWriter mergedFile) {
    Map<String, DisplayNameAndFileStream> newMap = new HashMap<>();
    for (Map.Entry<String, InputStream> entry : logFiles.entrySet()) {
      newMap.put(entry.getKey(), new DisplayNameAndFileStream(entry.getKey(), entry.getValue()));
    }
    return mergeLogFiles(newMap, mergedFile, false, false, false, new LinkedList<>());
  }

  /**
   * Merges the log files from a given set of {@code InputStream}s into a
   * {@code PrintWriter}.
   *
   * @param logFiles The log files to be merged
   * @param mergedFile Where the merged logs are printed to
   * @param tabOut Whether to align non-timestamped lines with timestamped lines
   * @param suppressBlanks Whether to omit blank lines
   * @param patterns Regular expression patterns that lines must match to be included
   * @return Whether or not problems occurred while merging the log files.
   *
   * @throws IllegalArgumentException If the length of {@code logFiles} is not the same as the
   *         length of {@code logFileNames}
   */
  public static boolean mergeLogFiles(final Map<String, DisplayNameAndFileStream> logFiles,
      final PrintWriter mergedFile, final boolean tabOut, final boolean suppressBlanks,
      final boolean multithreaded, final List<String> patterns) {
    return Sorter.mergeLogFiles(logFiles, mergedFile, tabOut, suppressBlanks, multithreaded,
        patterns);
  }

  /**
   * Prints usage information about this program
   */
  private static void usage(final String s) {
    // note that we don't document the -pids switch because it is tailored
    // to how hydra works and would not be useful for customers
    err.println(lineSeparator() + "** " + s + lineSeparator());
    err.println("Usage"
        + ": java MergeLogFiles [(directory | logFile)]+");
    err.println("-dirCount n      "
        + "Number of parent dirs to print");
    err.println("-mergeFile file  "
        + "File in which to put merged logs");
    err.println("-pids            "
        + "Search for PIDs in file names and use them to identify files");
    err.println(
        "-align           " + "Align non-timestamped lines with others");
    err.println("-noblanks        "
        + "Suppress output of blank lines");
    err.println("-threaded        "
        + "Use multithreading to take advantage of multiple CPUs");
    err.println();
    err.println(
        "Merges multiple GemFire log files and sorts them by timestamp.");
    err.println(
        "The merged log file is written to System.out (or a file).");
    err.println();
    err.println(
        "If a directory is specified, all .log files in that directory are merged.");
    err.println();
    ExitCode.FATAL.doSystemExit();
  }

  /**
   * Find all of the .log files in the given directory
   *
   * @param dirName directory to search
   * @return all of the .log files found (Files)
   */
  static List<File> getLogFiles(final String dirName) {
    List<File> result = new ArrayList<>();

    File dir = new File(dirName);
    File[] names = dir.listFiles();
    if (names != null) {
      for (final File name : names) {
        String path = name.getAbsolutePath();
        if (path.endsWith(".log") || path.endsWith(".log.gz")) {
          result.add(name);
        }
      }
    }
    return result;
  }

  public static void main(final String... args) throws IOException {
    File mergeFile = null;
    List<File> files = new ArrayList<>();
    int dirCount = 0;
    boolean findPIDs = false;
    boolean tabOut = false;
    boolean suppressBlanks = false;
    boolean multithreaded = false;
    List<String> patterns = new LinkedList<>();

    // Parse command line
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "-align":
          tabOut = true;
          break;
        case "-noblanks":
          suppressBlanks = true;
          break;
        case "-pids":
          findPIDs = true;
          break;
        case "-threaded":
          multithreaded = true;
          break;
        case "-regex":
          if (i + 1 >= args.length) {
            usage("missing pattern for -regex option");
          }
          patterns.add(args[i + 1]);
          i++;
          break;
        case "-dirCount":
          if (++i >= args.length) {
            usage("Missing number of parent directories");
          }

          try {
            dirCount = Integer.parseInt(args[i]);

          } catch (NumberFormatException ex) {
            usage(String.format("Not a number: %s", args[i]));
          }

          break;
        case "-mergeFile":
          if (++i >= args.length) {
            usage("Missing merge file name");
          }

          mergeFile = new File(args[i]);

          break;
        default:
          File file = new File(args[i]);
          if (!file.exists()) {
            usage(String.format("File %s does not exist", file));
          }

          files.add(file.getAbsoluteFile());
          break;
      }
    } // for
    if (files.isEmpty()) {
      usage("Missing filename");
    }

    // Expand directory names found in list
    List<File> expandedFiles = new ArrayList<>();
    for (File file : files) {
      String path = file.getAbsolutePath();
      if (!file.exists()) {
        usage(String.format("File %s does not exist", path));
      }
      if (file.isFile()) {
        expandedFiles.add(file);
        continue;
      }
      if (file.isDirectory()) {
        List<File> moreFiles = getLogFiles(path);
        expandedFiles.addAll(moreFiles);
        continue;
      }
      usage(String.format("File '%s' is neither a file nor a directory.", path));
    }
    Collections.sort(expandedFiles);
    files = expandedFiles;

    // Create output stream
    PrintStream ps;
    if (mergeFile != null) {
      FileOutputStream fileOutputStream = new FileOutputStream(mergeFile);
      try {
        ps = new PrintStream(fileOutputStream, true);
      } catch (Exception ex) {
        fileOutputStream.close();
        throw ex;
      }
    } else {
      ps = out;
    }

    PrintWriter mergedFile = new PrintWriter(ps, true);

    ps.println("Merged files (count = " + expandedFiles.size() + ") input list:");
    for (File expandedFile : expandedFiles) {
      ps.println("  " + expandedFile);
    }
    ps.println();

    List nickNames = null;
    if (findPIDs) {
      nickNames = findPIDs(files, mergedFile);
    }

    Map<String, DisplayNameAndFileStream> logFiles =
        getStringDisplayNameAndFileStreamMap(files, dirCount, findPIDs, nickNames);

    mergeLogFiles(logFiles, mergedFile, tabOut, suppressBlanks, multithreaded,
        patterns);

    ExitCode.NORMAL.doSystemExit();
  }

  static Map<String, DisplayNameAndFileStream> getStringDisplayNameAndFileStreamMap(
      List<File> files, int dirCount, boolean findPIDs, List nickNames) throws IOException {
    Map<String, DisplayNameAndFileStream> logFiles = new HashMap<>();
    for (int i = 0; i < files.size(); i++) {
      File file = files.get(i);

      String logFileName;
      if (findPIDs && nickNames.get(i) != null) {
        if (file.getCanonicalPath().toLowerCase().endsWith("gz")) {
          logFileName = nickNames.get(i) + ".gz";
        } else {
          logFileName = (String) nickNames.get(i);
        }
      } else {
        StringBuilder sb = new StringBuilder();
        File parent = file.getParentFile();
        for (int j = 0; j < dirCount && parent != null; j++) {
          String parentName = parent.getName() + "/";
          // don't add dot-slash
          if (parentName.equals("./")) {
            parent = null;
          } else {
            sb.insert(0, parentName);
            parent = parent.getParentFile();
          }
        }
        sb.append(file.getName());

        logFileName = sb.toString();
      }
      logFiles.put(file.getPath(),
          new DisplayNameAndFileStream(logFileName, new FileInputStream(file)));
    }
    return logFiles;
  }

  /**
   * hydra log files usually have the process's PID in their path name. This method extracts the PID
   * number and assigns the corresponding File a nickname using the PID and the position of the File
   * in the list of those also having this PID.
   * <p>
   * e.g., bgexec32414_1043.log --> 1043-1<br>
   * gemfire_1043/system.log --> 1043-2<br>
   * gemfire_1043/system_01_00.log --> 1043-3<br>
   */
  private static List<String> findPIDs(final Collection<File> files, final PrintWriter output) {
    int[] pidTable = new int[files.size()];
    int[] pidTableCounter = new int[pidTable.length];
    List<String> nickNames = new ArrayList<>();
    char fileSeparatorChar = File.separatorChar;

    for (File file : files) {
      String name = file.getPath();

      String slashdotslash = fileSeparatorChar + "." + fileSeparatorChar;
      int startIdx = name.lastIndexOf(slashdotslash);

      // get rid of the parent directories and any /./ in the path
      if (startIdx > 0) {
        name = name.substring(startIdx + slashdotslash.length());
      }

      startIdx = name.lastIndexOf(fileSeparatorChar);

      // first see if there's a number at the end of the file's directory name
      if (startIdx > 0) {
        startIdx--;
        char c = name.charAt(startIdx);
        if (!('0' <= c && c <= '9')) {
          startIdx = 0;
        } else {
          // see if this is a hydra-generated test directory name, like parReg-0504-161349
          int testIdx = startIdx - 1;
          while (testIdx > 0 && '0' <= name.charAt(testIdx) && name.charAt(testIdx) <= '9') {
            testIdx--;
          }
          if (testIdx < 1 || name.charAt(testIdx) == '-') {
            startIdx = 0;
          }
        }
      }

      // if there's no number in the directory name, use the file name
      if (startIdx <= 0) {
        startIdx = name.length() - 1;
        if (startIdx > 6 && name.charAt(startIdx) == 'z' && name.charAt(startIdx - 1) == 'g'
            && name.charAt(startIdx - 2) == '.' && name.charAt(startIdx - 3) == 'g'
            && name.charAt(startIdx - 4) == 'o' && name.charAt(startIdx - 5) == 'l'
            && name.charAt(startIdx - 6) == '.') {
          startIdx -= 7;
        } else if (startIdx > 3 && name.charAt(startIdx) == 'g' && name.charAt(startIdx - 1) == 'o'
            && name.charAt(startIdx - 2) == 'l' && name.charAt(startIdx - 3) == '.') {
          startIdx -= 4;
        }
      }

      // find the string of numbers at the end of the test area and use it as a PID
      for (int i = startIdx; i >= 0; i--) {
        char c = name.charAt(i);
        if (!('0' <= c && c <= '9')) {
          if (i < name.length() - 1) { // have a number
            // there's a number - assume it's a PID if it's not zero
            String PID = name.substring(i + 1, startIdx + 1);
            try {
              int iPID = Integer.parseInt(PID);
              if (iPID > 0) {
                int p = 0;
                // find the PID in the table of those seen so far, or assign it
                // a new slot. increment the number of files for this PID and
                // assign a nickname for the file
                for (; p < pidTable.length; p++) {
                  if (pidTable[p] == 0) {
                    pidTable[p] = iPID;
                    pidTableCounter[p] = 1;
                    break;
                  }
                  if (pidTable[p] == iPID) {
                    pidTableCounter[p]++;
                    break;
                  }
                }
                Assert.assertTrue(p < pidTableCounter.length);
                nickNames.add(iPID + "-" + pidTableCounter[p]);
                output.println("nickname " + iPID + "-" + pidTableCounter[p] + ": " + name);
              } else {
                nickNames.add(null);
              }
            } catch (NumberFormatException nfe) {
              nickNames.add(null);
            }
          } else {
            nickNames.add(null);
          }
          break;
        }
      }
    }
    return nickNames;
  }

  /** interface for threaded and non-threaded reader classes */
  interface Reader {

    LogEntry peek();

    LogEntry poll();

    String getFileName();

    void setUniqueId(int id);

    int getUniqueId();
  }

  /**
   * Thread that reads an entry from a GemFire log file and adds it a bounded queue. The entries are
   * consumed by a {@link MergeLogFiles.Sorter}.
   */
  static class NonThreadedReader implements Reader {

    /** The log file */
    private BufferedReader logFile;

    /** The name of the log file */
    private final String logFileName;

    private final LogFileParser parser;

    private LogEntry nextEntry;

    private final List<Pattern> patterns;

    /**
     * Creates a new {@code Reader} that reads from the given log file with the given name.
     * Invoking this constructor will start this reader thread.
     *
     * @param patterns java regular expressions that an entry must match one or more of
     */
    public NonThreadedReader(final InputStream logFile, final String logFileName,
        final ThreadGroup group, final boolean tabOut, final boolean suppressBlanks,
        final List<Pattern> patterns) {
      if (logFileName.endsWith(".gz")) {
        try {
          this.logFile = new BufferedReader(new InputStreamReader(new GZIPInputStream(logFile)));
        } catch (IOException e) {
          System.err.println(logFileName + " does not appear to be in gzip format");
          this.logFile = new BufferedReader(new InputStreamReader(logFile));
        }
      } else {
        this.logFile = new BufferedReader(new InputStreamReader(logFile));
      }
      this.logFileName = logFileName;
      this.patterns = patterns;
      parser = new LogFileParser(this.logFileName, this.logFile, tabOut, suppressBlanks);
    }

    /** returns the file name being read */
    @Override
    public String getFileName() {
      return logFileName;
    }

    /** unique identifier, used for sorting instead of file name */
    private int uniqueId;

    /** set the unique identifier for this reader */
    @Override
    public void setUniqueId(final int id) {
      uniqueId = id;
    }

    /** retrieve the unique identifier for this reader */
    @Override
    public int getUniqueId() {
      return uniqueId;
    }

    /**
     * Peeks at the oldest log entry read from the log file, waits for a log entry to become
     * available.
     *
     * @return {@code null} if interrupted while waiting
     */
    @Override
    public synchronized LogEntry peek() {
      while (nextEntry == null) {
        try {
          nextEntry = parser.getNextEntry();
          if (nextEntry == null) {
            return null;
          }
          if (!nextEntry.isLast() && !patternMatch(nextEntry)) {
            continue;
          }
        } catch (IOException ioe) {
          ioe.printStackTrace(System.err);
        }
      }
      return nextEntry;
    }


    /** return true if the entry matches one or more regex patterns */
    private boolean patternMatch(final LogEntry entry) {
      if (patterns == null || patterns.isEmpty()) {
        return true;
      }
      for (Pattern p : patterns) {
        if (p.matcher(entry.getContents()).matches()) {
          return true;
        }
      }
      return false;
    }

    /**
     * Removes the old log entry read from the log file
     */
    @Override
    public LogEntry poll() {
      LogEntry returnValue = null;
      if (nextEntry != null) {
        returnValue = nextEntry;
        nextEntry = null;
      } else {
        while (returnValue == null) {
          try {
            returnValue = parser.getNextEntry();
            if (!returnValue.isLast() && !patternMatch(returnValue)) {
              returnValue = null;
              continue;
            }
          } catch (IOException ioe) {
            ioe.printStackTrace(System.err);
            break;
          }
        }
      }
      return returnValue;
    }
  }

  /**
   * Thread that reads an entry from a GemFire log file and adds it a bounded queue. The entries are
   * consumed by a {@link MergeLogFiles.Sorter}.
   */
  static class ThreadedReader extends Thread implements Reader {

    /** The maximum size of the entry queue */
    private static final int QUEUE_CAPACITY = 1000;

    /** The log file */
    private BufferedReader logFile;

    /** The name of the log file */
    private final String logFileName;

    /** The queue containing log entries */
    private final BlockingQueue<LogEntry> queue;

    /** whether to suppress blank lines */
    private final boolean suppressBlanks;

    /** whether to align non-timestamped lines with timestamped lines */
    private final boolean tabOut;

    private final List<Pattern> patterns;

    /**
     * Creates a new {@code Reader} that reads from the given log file with the given name.
     * Invoking this constructor will start this reader thread. The InputStream is closed at the
     * end of processing.
     */
    public ThreadedReader(final InputStream logFile, final String logFileName,
        final ThreadGroup group, final boolean tabOut, final boolean suppressBlanks,
        final List<Pattern> patterns) {
      super(group, "Log File Reader");
      if (logFileName.endsWith(".gz")) {
        try {
          this.logFile = new BufferedReader(new InputStreamReader(new GZIPInputStream(logFile)));
        } catch (IOException e) {
          System.err.println(logFileName + " does not appear to be in gzip format");
          this.logFile = new BufferedReader(new InputStreamReader(logFile));
        }
      } else {
        this.logFile = new BufferedReader(new InputStreamReader(logFile));
      }
      this.logFileName = logFileName;
      queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
      this.suppressBlanks = suppressBlanks;
      this.tabOut = tabOut;
      this.patterns = patterns;
      start();
    }

    /** returns the file name being read */
    @Override
    public String getFileName() {
      return logFileName;
    }

    /** unique identifier, used for sorting instead of file name */
    private int uniqueId;

    /** set the unique identifier for this reader */
    @Override
    public void setUniqueId(final int id) {
      uniqueId = id;
    }

    /** retrieve the unique identifier for this reader */
    @Override
    public int getUniqueId() {
      return uniqueId;
    }

    /**
     * Reads the log file and places {@link LogEntry} objects into the queue. When it
     * is finished, it places a {@code LogEntry} that whose
     * {@link LogEntry#isLast isLast} method will return {@code true}.
     */
    @Override
    public void run() {
      LogFileParser parser =
          new LogFileParser(logFileName, logFile, tabOut, suppressBlanks);

      try {
        while (true) {
          SystemFailure.checkFailure();
          LogEntry entry = parser.getNextEntry();
          if (entry.isLast() || patternMatch(entry)) {
            queue.put(entry);

            synchronized (this) {
              notifyAll();
            }
          }
          if (entry.isLast()) {
            break;
          }
        }

      } catch (IOException ex) {
        ex.printStackTrace(System.err);

      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      } finally {
        try {
          logFile.close();
        } catch (IOException e) {
          e.printStackTrace(System.err);
        }
      }
    }

    /** return true if the entry matches one or more regex patterns */
    private boolean patternMatch(final LogEntry entry) {
      if (patterns == null || patterns.isEmpty()) {
        return true;
      }
      for (Pattern p : patterns) {
        if (p.matcher(entry.getContents()).matches()) {
          return true;
        }
      }
      return false;
    }

    /**
     * Peeks at the oldest log entry read from the log file, waits for a log entry to become
     * available.
     *
     * @return {@code null} if interrupted while waiting
     */
    @Override
    public LogEntry peek() {
      LogEntry entry = queue.peek();
      if (entry == null) {
        synchronized (this) {
          entry = queue.peek();
          while (entry == null) {
            boolean interrupted = Thread.interrupted();
            try {
              wait();
              entry = queue.peek();
            } catch (InterruptedException e) {
              interrupted = true;
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          }
        }
      }
      return entry;
    }

    /**
     * Removes the old log entry read from the log file
     */
    @Override
    public LogEntry poll() {
      return queue.poll();
    }
  }

  /**
   * A thread group that contains the reader threads and logs uncaught exceptions to standard error.
   */
  static class ReaderGroup extends ThreadGroup {

    /** Did an uncaught exception occur? */
    private boolean exceptionOccurred;

    ReaderGroup(final String groupName) {
      super(groupName);
      exceptionOccurred = false;
    }

    @Override
    public void uncaughtException(final Thread t, final Throwable e) {
      if (e instanceof VirtualMachineError) {
        SystemFailure.setFailure((Error) e); // don't throw
      }
      exceptionOccurred = true;
      System.err.println(String.format("Exception in %s", t));
      e.printStackTrace(System.err);
    }

    /**
     * Returns whether or not an uncaught exception occurred in one of the threads in this group.
     */
    public boolean exceptionOccurred() {
      return exceptionOccurred;
    }
  }

  /**
   * Examines the {@link MergeLogFiles.Reader#peek oldest entry} in each log file and writes it to a
   * merged log file.
   */
  static class Sorter {

    /**
     * Merges the log files from a given set of {@code InputStream}s into a
     * {@code PrintWriter}.
     *
     * @param logFiles The log files to be merged
     * @param mergedFile Where the merged logs are printed to
     * @param tabOut Whether to align non-timestamped lines with others
     * @param suppressBlanks Whether to suppress output of blank lines
     *
     * @return Whether or not problems occurred while merging the log files.
     *
     * @throws IllegalArgumentException If the length of {@code logFiles} is not the same as
     *         the length of {@code logFileNames}
     */
    public static boolean mergeLogFiles(final Map<String, DisplayNameAndFileStream> logFiles,
        final PrintWriter mergedFile, final boolean tabOut, final boolean suppressBlanks,
        final boolean multithreaded, final Iterable<String> patterns) {
      List<Pattern> compiledPatterns = new LinkedList<>();
      for (String pattern : patterns) {
        compiledPatterns.add(Pattern.compile(pattern, Pattern.CASE_INSENSITIVE));
      }

      // First start the Reader threads
      ReaderGroup group =
          new ReaderGroup("Reader threads");
      Collection<Reader> readers = new ArrayList<>(logFiles.size());
      for (DisplayNameAndFileStream nameAndFileStream : logFiles.values()) {
        if (multithreaded) {
          readers.add(new ThreadedReader(nameAndFileStream.getInputStream(),
              nameAndFileStream.getDisplayName(), group, tabOut,
              suppressBlanks, compiledPatterns));
        } else {
          readers.add(new NonThreadedReader(nameAndFileStream.getInputStream(),
              nameAndFileStream.getDisplayName(), group, tabOut,
              suppressBlanks, compiledPatterns));
        }
      }

      // Merge the log files together
      Reader lastOldest = null;

      // sort readers by their next time-stamp
      Set<Reader> sorted = sortReaders(readers);

      while (!readers.isEmpty()) {
        Iterator sortedIt = sorted.iterator();
        if (!sortedIt.hasNext()) {
          break;
        }
        Reader oldest = (Reader) sortedIt.next();
        sortedIt.remove();

        String nextReaderTimestamp = null;
        if (sortedIt.hasNext()) {
          Reader nextInLine = (Reader) sortedIt.next();
          nextReaderTimestamp = nextInLine.peek().getTimestamp();
        }

        // if we've switched to a different reader, emit a blank line
        // for readability
        if (oldest != lastOldest) {
          mergedFile.println();
          lastOldest = oldest;
        }

        LogEntry entry;
        // write until we hit the next file's time-stamp
        do {
          entry = oldest.peek();
          String timestamp = entry.getTimestamp();

          if (nextReaderTimestamp != null) {
            if (nextReaderTimestamp.compareTo(timestamp) < 0) {
              sorted.add(oldest);
              entry = null;
              break;
            }
          }

          entry = oldest.poll();
          entry.writeTo(mergedFile);

        } while (!entry.isLast());

        if (entry != null && entry.isLast()) {
          readers.remove(oldest);
        }
      }

      return group.exceptionOccurred();
    }

    private static Set<Reader> sortReaders(final Iterable<Reader> readers) {
      Set<Reader> sorted = new TreeSet<>(new ReaderComparator());
      int uniqueId = 1;
      for (Reader reader : readers) {
        if (reader == null) {
          continue;
        }
        reader.setUniqueId(uniqueId++);
        sorted.add(reader);
      }
      return sorted;
    }
  }

  @SuppressWarnings("serial")
  protected static class ReaderComparator implements Comparator<Reader>, Serializable {

    @Override
    public int compare(final Reader reader1, final Reader reader2) {
      int id1 = reader1.getUniqueId();
      int id2 = reader2.getUniqueId();
      LogEntry entry1 = reader1.peek();
      LogEntry entry2 = reader2.peek();
      if (entry1 == null) {
        if (entry2 == null) {
          return Integer.compare(id1, id2);
        }
        // sort readers with no entries before readers with entries so they'll
        // be removed quickly
        return -1;
      }
      if (entry2 == null) {
        return 1;
      }
      String timestamp1 = entry1.getTimestamp();
      String timestamp2 = entry2.getTimestamp();
      int compare = timestamp1.compareTo(timestamp2);
      if (compare == 0) {
        if (id1 < id2) {
          return -1;
        }
        return 1;
      }
      return compare;
    }
  }

  static class DisplayNameAndFileStream {
    private final String displayName;
    private final InputStream inputStream;

    public String getDisplayName() {
      return displayName;
    }

    public InputStream getInputStream() {
      return inputStream;
    }

    DisplayNameAndFileStream(String displayName, InputStream inputStream) {
      this.displayName = displayName;
      this.inputStream = inputStream;
    }
  }
}
