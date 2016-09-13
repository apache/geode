/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.logging;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.geode.SystemFailure;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.FileUtil;
import org.apache.geode.internal.i18n.LocalizedStrings;

/**
 * This program merges entries from multiple GemFire log files (those
 * written using a {@link org.apache.geode.i18n.LogWriterI18n} together,
 * sorting them by their timestamp.  Note that this program assumes
 * that the entries in the individual log files are themselves sorted
 * by timestamp.<p>
 *
 * MergeLogFiles has several command line options:<br>
 * <br>
 * <b>-pids</b> tells the program to look for hydra-style naming and pick out
 * process identifiers, then use these to distinguish log entries instead of
 * full log file names.<br>
 * <br>
 * <b>-dirCount x</b> when <i>-pids</i> is not being used, or it is being used
 * and a process ID can't be found, the <i>-dirCount</i> option instructs the
 * program as to how many parent directory names should be included in the name
 * of each log file.<br>
 * <br>
 * <b>-noblanks</b> tells the program to suppress blank lines found in log files<br>
 * <br>
 * <b>-align</b> tells the program to align log content not having timestamps with
 * log content that does have timestamps.<br>
 * <br>
 * <b>-threads</b> tells the program to attempt to make use of multiple CPUs<p>
 *
 * The <i>-pids</i> option will cause a file nickname table to be built and
 * emitted at the beginning of the merged log files.  Nicknames are of the
 * form pid-x, where <i>pid</i> is the process ID that emitted the log entry, and
 * <i>x</i> is the position of this log file in the ordered set of log files
 * created by that process.<p>
 * 
 * Normally, one log file reader is created per log file, and they are iterated
 * over repeatedly to find and write the next earliest timestamped line to
 * the output stream.  Text without a timestamp is associated with the prior
 * timestamped line in the file.<p>
 * 
 * The <i>-threads</i> option will cause the program to also create threads
 * for each reader that are backed by bounded {@link BlockingQueue queues},
 * as outlined in the diagram below.  This can consume more memory, so it is
 * wise to increase the Xmx of the java virtual machine if you are going to
 * use this option.<p>
 *
 * <CENTER>
 * <IMG SRC="{@docRoot}/javadoc-images/merge-log-files.gif"
 *      WIDTH="353" HEIGHT="246"/>
 * </CENTER>
 *
 * @see SortLogFile
 * @see LogFileParser
 *
 *
 *
 * @since GemFire 2.0 (-pids, -threads, -align, and -noblanks added in 5.1)
 */
public class MergeLogFiles {
  private static PrintStream out = System.out;
  private static PrintStream err = System.err;

  /**
   * Merges the log files from a given set of
   * <code>InputStream</code>s into a <code>PrinWriter</code>.
   *
   * @param logFiles
   *        The log files to be merged
   * @param logFileNames
   *        The names of the log files to be printed in the merged log
   * @param mergedFile
   *        Where the merged logs are printed to
   * 
   * @return Whether or not problems occurred while merging the log
   *         files. 
   *
   * @throws IllegalArgumentException
   *         If the length of <code>logFiles</code> is not the same
   *         as the length of <code>logFileNames</code>
   */
  public static boolean mergeLogFiles(InputStream[] logFiles,
      String[] logFileNames, PrintWriter mergedFile) {
    return mergeLogFiles(logFiles, logFileNames, mergedFile, false, false, false, new LinkedList());
  }
  
  /**
   * Merges the log files from a given set of
   * <code>InputStream</code>s into a <code>PrinWriter</code>.
   *
   * @param logFiles
   *        The log files to be merged
   * @param logFileNames
   *        The names of the log files to be printed in the merged log
   * @param mergedFile
   *        Where the merged logs are printed to
   * @param tabOut
   *        Whether to align non-timestamped lines with timestamped lines
   * @param suppressBlanks
   *        Whether to omit blank lines
   * @param patterns
   *        Regular expression patterns that lines must match to be included
   * @return Whether or not problems occurred while merging the log
   *         files. 
   *
   * @throws IllegalArgumentException
   *         If the length of <code>logFiles</code> is not the same
   *         as the length of <code>logFileNames</code>
   */
  public static boolean mergeLogFiles(InputStream[] logFiles,
      String[] logFileNames, PrintWriter mergedFile,
      boolean tabOut, boolean suppressBlanks, boolean multithreaded, List<String> patterns) {
    return Sorter.mergeLogFiles(logFiles, logFileNames, mergedFile,
        tabOut, suppressBlanks, multithreaded, patterns);
  }

  
  // ////////////////// Main Program ////////////////////

  /**
   * Prints usage information about this program
   */
  private static void usage(String s) {
    // note that we don't document the -pids switch because it is tailored
    // to how hydra works and would not be useful for customers
    err.println("\n** " + s + "\n");
    err.println(LocalizedStrings.MergeLogFiles_USAGE.toLocalizedString() + ": java MergeLogFiles [(directory | logFile)]+");
    err.println("-dirCount n      " + LocalizedStrings.MergeLogFiles_NUMBER_OF_PARENT_DIRS_TO_PRINT.toLocalizedString());
    err.println("-mergeFile file  " + LocalizedStrings.MergeLogFiles_FILE_IN_WHICH_TO_PUT_MERGED_LOGS.toLocalizedString());
    err.println("-pids            " + LocalizedStrings.MergeLogFiles_SEARCH_FOR_PIDS_IN_FILE_NAMES_AND_USE_THEM_TO_IDENTIFY_FILES.toLocalizedString());
    err.println("-align           " + LocalizedStrings.MergeLogFiles_ALIGN_NONTIMESTAMPED_LINES_WITH_OTHERS.toLocalizedString());
    err.println("-noblanks        " + LocalizedStrings.MergeLogFiles_SUPPRESS_OUTPUT_OF_BLANK_LINES.toLocalizedString());
    err.println("-threaded        " + LocalizedStrings.MergeLogFiles_USE_MULTITHREADING_TO_TAKE_ADVANTAGE_OF_MULTIPLE_CPUS.toLocalizedString());
//    err.println("-regex  pattern   Case-insensitive search for a regular expression.");
//    err.println("                 May be used multiple times.  Use Java regular ");
//    err.println("                 expression syntax (see java.util.regex.Pattern).");
    err.println("");
    err.println(LocalizedStrings.MergeLogFiles_MERGES_MULTIPLE_GEMFIRE_LOG_FILES_AND_SORTS_THEM_BY_TIMESTAMP.toLocalizedString());
    err.println(LocalizedStrings.MergeLogFiles_THE_MERGED_LOG_FILE_IS_WRITTEN_TO_SYSTEM_OUT_OR_A_FILE.toLocalizedString());
    err.println("");
    err.println(LocalizedStrings.MergeLogFiles_IF_A_DIRECTORY_IS_SPECIFIED_ALL_LOG_FILES_IN_THAT_DIRECTORY_ARE_MERGED.toLocalizedString());
    err.println("");
    System.exit(1);
  }

  /**
   * Find all of the .log files in the given directory
   * @param dirName directory to search
   * @return all of the .log files found (Files)
   */
  static ArrayList getLogFiles(String dirName) {
    ArrayList result = new ArrayList();
    
    File dir = new File(dirName);
    File names[] = FileUtil.listFiles(dir);
    for (int i = 0; i < names.length; i ++) {
      String n = names[i].getAbsolutePath();
      if (n.endsWith(".log") || n.endsWith(".log.gz")) {
        result.add(names[i]);
      }
    } // for
    return result;
  }
  
  public static void main(String[] args) throws IOException {
    File mergeFile = null;
    ArrayList files = new ArrayList();
    List nickNames = null;
    int dirCount = 0;
    boolean findPIDs = false;
    boolean tabOut = false;
    boolean suppressBlanks = false;
    boolean multithreaded = false;
    List<String> patterns = new LinkedList();
    
    // Parse command line
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-align")) {
        tabOut = true;
      }
      else if (args[i].equals("-noblanks")) {
        suppressBlanks = true;
      }
      else if (args[i].equals("-pids")) {
        findPIDs = true;
      }
      else if (args[i].equals("-threaded")) {
        multithreaded = true;
      }
      else if (args[i].equals("-regex")) {
        if (i+1 >= args.length) {
          usage("missing pattern for -regex option");
        }
        patterns.add(args[i+1]);
        i++;
      }
      else if (args[i].equals("-dirCount")) {
        if (++i >= args.length) {
          usage(LocalizedStrings.MergeLogFiles_MISSING_NUMBER_OF_PARENT_DIRECTORIES.toLocalizedString());
        }

        try {
          dirCount = Integer.parseInt(args[i]);

        } catch (NumberFormatException ex) {
          usage(LocalizedStrings.MergeLogFiles_NOT_A_NUMBER_0.toLocalizedString(args[i]));
        }

      } else if (args[i].equals("-mergeFile")) {
        if (++i >= args.length) {
          usage(LocalizedStrings.MergeLogFiles_MISSING_MERGE_FILE_NAME.toLocalizedString());
        }

        mergeFile = new File(args[i]);

      } else {
        File file = new File(args[i]);
        if (!file.exists()) {
          usage(LocalizedStrings.MergeLogFiles_FILE_0_DOES_NOT_EXIST.toLocalizedString(file));
        }

        files.add(file.getAbsoluteFile());
      }
    } // for
    if ( files.isEmpty() ) {
      usage(LocalizedStrings.MergeLogFiles_MISSING_FILENAME.toLocalizedString());
    }

    // Expand directory names found in list
    ArrayList expandedFiles = new ArrayList();
    for (int i = 0; i <  files.size(); i ++) {
      File f = (File)files.get(i);
      String n = f.getAbsolutePath();
      if (!f.exists()) {
        usage(LocalizedStrings.MergeLogFiles_FILE_0_DOES_NOT_EXIST.toLocalizedString(n));
      }
      if (f.isFile()) {
        expandedFiles.add(f);
        continue;
      }
      if (f.isDirectory()) {
       ArrayList moreFiles = getLogFiles(n);
       expandedFiles.addAll(moreFiles);
       continue; 
      }
      usage(LocalizedStrings.MergeLogFiles_FILE_0_IS_NEITHER_A_FILE_NOR_A_DIRECTORY.toLocalizedString(n));
    }
    Collections.sort(expandedFiles);
    files = expandedFiles;
    
    // Create output stream
    PrintStream ps;
    if (mergeFile != null) {
      ps = new PrintStream(new FileOutputStream(mergeFile), true);

    } else {
      ps = out;
    }
    PrintWriter mergedFile = new PrintWriter(ps, true);

    ps.println("Merged files (count = " + expandedFiles.size() + ") input list:");
    for (int i = 0; i < expandedFiles.size(); i ++) {
      ps.println("  " + expandedFiles.get(i));
    }
    ps.println("");
    
    if (findPIDs) {
      nickNames = findPIDs(files, mergedFile);
    }

    InputStream[] logFiles = new InputStream[files.size()];
    String[] logFileNames = new String[files.size()];
    for (int i = 0; i < files.size(); i++) {
      File file = (File) files.get(i);
      logFiles[i] = new FileInputStream(file);

      if (findPIDs  &&  (nickNames.get(i) != null)) {
        if (file.getCanonicalPath().toLowerCase().endsWith("gz")) {
          logFileNames[i] = (String)nickNames.get(i)+".gz";
        } else {
          logFileNames[i] = (String)nickNames.get(i);
        }
      }
      else {
        StringBuffer sb = new StringBuffer();
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
  
        logFileNames[i] = sb.toString();
      }
    }
    
    mergeLogFiles(logFiles, logFileNames, mergedFile, tabOut, suppressBlanks,
        multithreaded, patterns);

    System.exit(0);
  }

  //////////////////////  Inner Classes  //////////////////////

  /**
   * hydra log files usually have the process's PID in their path name.
   * This method extracts the PID number and assigns the corresponding File
   * a nickname using the PID and the position of the File in the list of those
   * also having this PID.<p>
   * e.g., bgexec32414_1043.log          --> 1043-1<br>
   *       gemfire_1043/system.log       --> 1043-2<br>
   *       gemfire_1043/system_01_00.log --> 1043-3<br>
   */
  private static ArrayList findPIDs(ArrayList files, PrintWriter output) {
    int pidTable[] = new int[files.size()];
    int pidTableCounter[] = new int[pidTable.length];
    ArrayList nickNames = new ArrayList();
    char sep = File.separatorChar;

//    System.out.println("findPids() invoked");
    
    for (Iterator it=files.iterator(); it.hasNext(); ) {
      File f = (File)it.next();
      String name = f.getPath();
//      System.out.println("considering " + name);
      
      String slashdotslash = "" + sep + "." + sep;
      int startIdx = name.lastIndexOf(slashdotslash);
      
      // get rid of the parent directories and any /./ in the path
      if (startIdx > 0) {
        name = name.substring(startIdx + slashdotslash.length());
      }

      startIdx = name.lastIndexOf(sep);
      
      // first see if there's a number at the end of the file's directory name
      if (startIdx > 0) {
        startIdx--;
        char c = name.charAt(startIdx);
        if ( ! ('0' <= c  &&  c <= '9') ) {
//          System.out.println("no number found in directory name");
          startIdx = 0;
        }
        else {
          // see if this is a hydra-generated test directory name, like
          // parReg-0504-161349
          int testIdx = startIdx-1;
          while (testIdx > 0 && '0' <= name.charAt(testIdx) && name.charAt(testIdx) <= '9') {
            testIdx--;
          }
          if (testIdx < 1 || name.charAt(testIdx) == '-') {
            startIdx = 0;
          }
//          System.out.println("using directory name: '" + name.substring(0, startIdx+1) + "'");
        }
      }
      
      // if there's no number in the directory name, use the file name
      if (startIdx <= 0) {
        startIdx = name.length() - 1;
        if (startIdx > 6
            && name.charAt(startIdx) == 'z'
            && name.charAt(startIdx-1) == 'g'
            && name.charAt(startIdx-2) == '.'
            && name.charAt(startIdx-3) == 'g'
            && name.charAt(startIdx-4) == 'o'
            && name.charAt(startIdx-5) == 'l'
            && name.charAt(startIdx-6) == '.') {
          startIdx -= 7;
        }
        else if (startIdx > 3
            && name.charAt(startIdx) == 'g'
            && name.charAt(startIdx-1) == 'o'
            && name.charAt(startIdx-2) == 'l'
            && name.charAt(startIdx-3) == '.') {
          startIdx -= 4;
//          System.out.println("using file name: '" + name.substring(0,startIdx+1) + "'");
        }
//        else {
//          System.out.println("could not find a PID");
//        }
      }
      
      // find the string of numbers at the end of the test area and use it
      // as a PID
      String PID = null;
      for (int i=startIdx; i>=0; i--) {
        char c = name.charAt(i);
//        System.out.println("charAt("+i+")="+c);
        if ( ! ('0' <= c  &&  c <= '9') ) {
          if (i < (name.length()-1)) { // have a number
            // there's a number - assume it's a PID if it's not zero
            PID = name.substring(i+1, startIdx+1);
//            System.out.println("parsing '" + PID + "'");
            try {
              int iPID = Integer.valueOf(PID).intValue();
              if (iPID > 0) {
//                System.out.println("Found PID " + iPID);
                int p = 0;
                // find the PID in the table of those seen so far, or assign it
                // a new slot.  increment the number of files for this PID and
                // assign a nickname for the file
                for ( ; p<pidTable.length; p++) {
                  if (pidTable[p] == 0) {
                    pidTable[p] = iPID;
                    pidTableCounter[p] = 1;
                    break;
                  }
                  else if (pidTable[p] == iPID) {
                    pidTableCounter[p]++;
                    break;
                  }
                }
                Assert.assertTrue(p < pidTableCounter.length);
                nickNames.add(""+iPID+"-"+pidTableCounter[p]);
                output.println("nickname " + iPID + "-" + pidTableCounter[p] + ": " + name); 
              }
              else {
                nickNames.add(null);
              }
            }
            catch (NumberFormatException nfe) {
              nickNames.add(null);
            }
          } // have a number
          else {
            nickNames.add(null);
          }
          break;
        } // not a digit
      } // for(i)
    }
    return nickNames;
  }

  /** interface for threaded and non-threaded reader classes */
  static interface Reader {
    public LogFileParser.LogEntry peek();
    public LogFileParser.LogEntry poll();
    public String getFileName();
    public void setUniqueId(int id);
    public int getUniqueId();
  }
  
  
  /**
   * Thread that reads an entry from a GemFire log file and adds it a
   * bounded queue.  The entries are consumed by a {@link
   * MergeLogFiles.Sorter}.
   */
  static class NonThreadedReader implements Reader {

    /** The maximum size of the entry queue */
//    private static int QUEUE_CAPACITY = 1000;

    ////////////////////  Instance Methods  ////////////////////

    /** The log file */
    private BufferedReader logFile;

    /** The name of the log file */
    private String logFileName;

    /** whether to suppress blank lines */
//    private boolean suppressBlanks;
    
    /** whether to align non-timestamped lines with timestamped lines */
//    private boolean tabOut;

    private LogFileParser parser;
    
    private LogFileParser.LogEntry nextEntry;

    private List<Pattern> patterns;

    ////////////////////  Constructors  ////////////////////

    /**
     * Creates a new <code>Reader</code> that reads from the given log
     * file with the given name.  Invoking this constructor will start
     * this reader thread.
     * @param patterns java regular expressions that an entry must match one or more of
     */
    public NonThreadedReader(InputStream logFile, String logFileName,
                  ThreadGroup group, boolean tabOut, boolean suppressBlanks, List<Pattern> patterns) {
//         super(group, "Reader for " + ((logFileName != null) ? logFileName : logFile.toString()));
      if (logFileName.endsWith(".gz")) {
        try {
          this.logFile = new BufferedReader(new InputStreamReader(new GZIPInputStream(logFile)));
        } catch (IOException e) {
          System.err.println(logFileName + " does not appear to be in gzip format");
          this.logFile = new BufferedReader(new InputStreamReader(logFile));
        }
      } else {
        this.logFile =
          new BufferedReader(new InputStreamReader(logFile));
      }
      this.logFileName = logFileName;
      this.patterns = patterns;
//      this.suppressBlanks = suppressBlanks;
//      this.tabOut = tabOut;
      this.parser = new LogFileParser(this.logFileName, this.logFile, tabOut, suppressBlanks);
    }
                
    /** returns the file name being read */
    public String getFileName() {
      return this.logFileName;
    }
    
    /** unique identifier, used for sorting instead of file name */
    private int uniqueId;
    /** set the unique identifier for this reader */
    public void setUniqueId(int id) {
      uniqueId = id;
    }
    /** retrieve the unique identifier for this reader */
    public int getUniqueId() {
      return uniqueId;
    }
                
    /**
     * Peeks at the oldest log entry read from the log file, waits for
     * a log entry to become available.
     *
     * @return <code>null</code> if interrupted while waiting
     */
    public synchronized LogFileParser.LogEntry peek() {
      while (this.nextEntry == null) {
        try {
          nextEntry = parser.getNextEntry();
          if (nextEntry == null) {
            return null;
          }
          if (!nextEntry.isLast() && !patternMatch(nextEntry)) {
            continue;
          }
        }
        catch (IOException ioe) {
          ioe.printStackTrace(System.err);
        }
      }
      return nextEntry;
    }
    

    /** return true if the entry matches one or more regex patterns */
    private boolean patternMatch(LogFileParser.LogEntry entry) {
      if (this.patterns == null || this.patterns.isEmpty()) {
        return true;
      }
      for (Pattern p: patterns) {
        if (p.matcher(entry.getContents()).matches()) {
          return true;
        }
      }
      return false;
    }

    /**
     * Removes the old log entry read from the log file
     */
    public LogFileParser.LogEntry poll() {
      LogFileParser.LogEntry returnValue = null;
      if (this.nextEntry != null) {
        returnValue = this.nextEntry;
        nextEntry = null;
      }
      else {
        while (returnValue == null) {
          try {
            returnValue = parser.getNextEntry();
            if (!returnValue.isLast() && !patternMatch(returnValue)) {
              returnValue = null;
              continue;
            }
          }
          catch (IOException ioe) {
            ioe.printStackTrace(System.err);
            break;
          }
        }
      }
      return returnValue;
    }
  }

  /**
   * Thread that reads an entry from a GemFire log file and adds it a
   * bounded queue.  The entries are consumed by a {@link
   * MergeLogFiles.Sorter}.
   */
  static class ThreadedReader extends Thread implements Reader {

    /** The maximum size of the entry queue */
    private static int QUEUE_CAPACITY = 1000;

    ////////////////////  Instance Methods  ////////////////////

    /** The log file */
    private BufferedReader logFile;

    /** The name of the log file */
    private String logFileName;

    /** The queue containing log entries */
    private BlockingQueue queue;
    
    /** whether to suppress blank lines */
    private boolean suppressBlanks;
    
    /** whether to align non-timestamped lines with timestamped lines */
    private boolean tabOut;

    private List<Pattern> patterns;

    ////////////////////  Constructors  ////////////////////

    /**
     * Creates a new <code>Reader</code> that reads from the given log
     * file with the given name.  Invoking this constructor will start
     * this reader thread.
     * @param patterns TODO
     *
     * @see #run
     */
    public ThreadedReader(InputStream logFile, String logFileName,
                  ThreadGroup group, boolean tabOut, boolean suppressBlanks, List<Pattern> patterns) {
//       super(group, "Reader for " + ((logFileName != null) ? logFileName : logFile.toString()));
      super(group, LocalizedStrings.MergeLogFiles_LOG_FILE_READER.toLocalizedString());
      if (logFileName.endsWith(".gz")) {
        try {
          this.logFile = new BufferedReader(new InputStreamReader(new GZIPInputStream(logFile)));
        } catch (IOException e) {
          System.err.println(logFileName + " does not appear to be in gzip format");
          this.logFile = new BufferedReader(new InputStreamReader(logFile));
        }
      } else {
        this.logFile =
          new BufferedReader(new InputStreamReader(logFile));
      }
      this.logFileName = logFileName;
      this.queue = new LinkedBlockingQueue(QUEUE_CAPACITY);
//        new UnsharedMessageQueue(QUEUE_CAPACITY,
//                                 (75 * QUEUE_CAPACITY) / 100);
      this.suppressBlanks = suppressBlanks;
      this.tabOut = tabOut;
      this.patterns = patterns;
      this.start();
    }
    
    /** returns the file name being read */
    public String getFileName() {
      return this.logFileName;
    }
                
    /** unique identifier, used for sorting instead of file name */
    private int uniqueId;

    /** set the unique identifier for this reader */
    public void setUniqueId(int id) {
      uniqueId = id;
    }
    
    /** retrieve the unique identifier for this reader */
    public int getUniqueId() {
      return uniqueId;
    }
                
    /**
     * Reads the log file and places {@link LogFileParser.LogEntry}
     * objects into the queue.  When it is finished, it places a
     * <code>LogEntry</code> that whose {@link
     * LogFileParser.LogEntry#isLast isLast} method will return
     * <code>true</code>.
     */ 
    @Override
    public void run() {
      LogFileParser parser =
        new LogFileParser(this.logFileName, this.logFile, tabOut, suppressBlanks);

      try {
        while (true) {
          SystemFailure.checkFailure();
          LogFileParser.LogEntry entry = parser.getNextEntry();
          if (entry.isLast() || patternMatch(entry)) {
            this.queue.put(entry);

            synchronized (this) {
              this.notify();
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
      }
    }

    /** return true if the entry matches one or more regex patterns */
    private boolean patternMatch(LogFileParser.LogEntry entry) {
      if (this.patterns == null || this.patterns.isEmpty()) {
        return true;
      }
      for (Pattern p: patterns) {
        if (p.matcher(entry.getContents()).matches()) {
          return true;
        }
      }
      return false;
    }

    /**
     * Peeks at the oldest log entry read from the log file, waits for
     * a log entry to become available.
     *
     * @return <code>null</code> if interrupted while waiting
     */
    public LogFileParser.LogEntry peek() {
//       out.println(this.getName() + " size " + this.queue.size());
      LogFileParser.LogEntry entry =
        (LogFileParser.LogEntry) this.queue.peek();
      if (entry == null) {
        synchronized (this) {
          entry = (LogFileParser.LogEntry) this.queue.peek();
          while (entry == null) {
            boolean interrupted = Thread.interrupted();
            try {
              this.wait();
              entry = (LogFileParser.LogEntry) this.queue.peek();
            }
            catch (InterruptedException e) {
              interrupted = true;
            }
            finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          } // while
        } // synchronized
      }
      return entry;
    }

    /**
     * Removes the old log entry read from the log file
     */
    public LogFileParser.LogEntry poll() {
      return (LogFileParser.LogEntry) this.queue.poll();
    }
  }

  /**
   * A thread group that contains the reader threads and logs uncaught
   * exceptions to standard error.
   */
  static class ReaderGroup extends ThreadGroup {
    /** Did an uncaught exception occur? */
    private boolean exceptionOccurred;

    ReaderGroup(String groupName) {
      super(groupName);
      this.exceptionOccurred = false;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
      if (e instanceof VirtualMachineError) {
        SystemFailure.setFailure((VirtualMachineError)e); // don't throw
      }
      this.exceptionOccurred = true;
      System.err.println(LocalizedStrings.MergeLogFiles_EXCEPTION_IN_0.toLocalizedString(t));
      e.printStackTrace(System.err);
    }

    /**
     * Returns whether or not an uncaught exception occurred in one of
     * the threads in this group.
     */
    public boolean exceptionOccurred() {
      return this.exceptionOccurred;
    }
  }

  /**
   * Examines the {@link MergeLogFiles.Reader#peek oldest entry} in
   * each log file and writes it to a merged log file.
   */
  static class Sorter {

    /**
     * Merges the log files from a given set of
     * <code>InputStream</code>s into a <code>PrintWriter</code>.
     *
     * @param logFiles
     *        The log files to be merged
     * @param logFileNames
     *        The names of the log files to be printed in the merged log
     * @param mergedFile
     *        Where the merged logs are printed to
     * @param tabOut
     *        Whether to align non-timestamped lines with others
     * @param suppressBlanks
     *        Whether to suppress output of blank lines
     * @param patterns TODO
     * @return Whether or not problems occurred while merging the log
     *         files. 
     *
     * @throws IllegalArgumentException
     *         If the length of <code>logFiles</code> is not the same
     *         as the length of <code>logFileNames</code>
     */
    public static boolean mergeLogFiles(InputStream[] logFiles,
        String[] logFileNames, PrintWriter mergedFile,
        boolean tabOut, boolean suppressBlanks, boolean multithreaded, List<String> patterns) {
      if (logFiles.length != logFileNames.length) {
        throw new IllegalArgumentException(LocalizedStrings.MergeLogFiles_NUMBER_OF_LOG_FILES_0_IS_NOT_THE_SAME_AS_THE_NUMBER_OF_LOG_FILE_NAMES_1.toLocalizedString(new Object[] {Integer.valueOf(logFiles.length), Integer.valueOf(logFileNames.length)}));
      }
      
      List<Pattern> compiledPatterns = new LinkedList<Pattern>();
      for (String pattern: patterns) {
        compiledPatterns.add(Pattern.compile(pattern, Pattern.CASE_INSENSITIVE));
      }

      // First start the Reader threads
      ReaderGroup group = new ReaderGroup(LocalizedStrings.MergeLogFiles_READER_THREADS.toLocalizedString());
      Collection readers = new ArrayList(logFiles.length);
      for (int i = 0; i < logFiles.length; i++) {
        if (multithreaded) {
          readers.add(new ThreadedReader(logFiles[i], logFileNames[i], group,
              tabOut, suppressBlanks, compiledPatterns));
        }
        else {
          readers.add(new NonThreadedReader(logFiles[i], logFileNames[i], group,
              tabOut, suppressBlanks, compiledPatterns));
        }
      }

      // Merge the log files together
      Reader lastOldest = null;
      
      // sort readers by their next time-stamp
      Set sorted = sortReaders(readers);

      while (!readers.isEmpty()) {

        Reader oldest = null;
        Iterator sortedIt = sorted.iterator();
        if (!sortedIt.hasNext()) {
          break;
        }
        oldest = (Reader)sortedIt.next();
        sortedIt.remove();

        String nextReaderTimestamp = null;
        Reader nextInLine = null;
        if (sortedIt.hasNext()) {
          nextInLine = (Reader)sortedIt.next();
          nextReaderTimestamp = nextInLine.peek().getTimestamp();
        }

        
        // if we've switched to a different reader, emit a blank line
        // for readability
        if (oldest != lastOldest) {
          mergedFile.println();
          lastOldest = oldest;
        }

        LogFileParser.LogEntry entry = null;
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
    
    private static Set sortReaders(Collection readers) {
      Set sorted = new TreeSet(new ReaderComparator());
      int uniqueId = 1;
      for (Iterator iter = readers.iterator(); iter.hasNext(); ) {
        Reader reader = (Reader)iter.next();
        if (reader == null) {
          continue;
        }
        reader.setUniqueId(uniqueId++);
        sorted.add(reader);
      }
      return sorted;
    }

  }
  
  protected static class ReaderComparator implements Comparator {

    public int compare(Object o1, Object o2) {
      Reader reader1 = (Reader)o1;
      int id1 = reader1.getUniqueId();
      Reader reader2 = (Reader)o2;
      int id2 = reader2.getUniqueId();
      LogFileParser.LogEntry entry1 = reader1.peek();
      LogFileParser.LogEntry entry2 = reader2.peek();
      if (entry1 == null) {
        if (entry2 == null) {
          if (id1 < id2) {
            return -1;
          }
          // IDs are unique, so no need for == test
          return 1;
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
  
}


