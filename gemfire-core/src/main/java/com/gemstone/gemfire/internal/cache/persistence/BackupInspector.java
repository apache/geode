/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Inspects a completed backup and parses the operation log file data from the restore script produced by a previous backup.
 * @author rholmes
 */
public abstract class BackupInspector {
  /**
   * Maps operation log file names to script lines that copy previously backed up operation log files.
   * These lines will be added to future restore scripts if the operation logs are still relevant to the member.
   */
  protected Map<String,String> oplogLineMap =  new HashMap<String,String>();
  
  /**
   * Contains the unique set of operation log file names contained in the restore script.
   */
  protected Set<String> oplogFileNames =  new HashSet<String>();
  
  /**
   * Root directory for a member's backup.
   */
  protected File backupDir = null;
  
  /**
   * Returns a BackupInspector for a member's backup directory.
   * @param backupDir a member's backup directory.
   * @return a new BackupInspector.
   * @throws IOException the backup directory was malformed.
   */
  public static BackupInspector createInspector(File backupDir) 
  throws IOException {
    if(isWindows()) {
      return new WindowsBackupInspector(backupDir);
    }
    
    return new UnixBackupInspector(backupDir);
  }
  
  /**
   * Creates a new BackupInspector.
   * @param backupDir a a previous backup for a member.
   * @throws IOException an error occurred while parsing the restore script.
   */
  public BackupInspector(File backupDir) 
  throws IOException {
    this.backupDir = backupDir;

    if(!backupDir.exists()) {
      throw new IOException("Backup directory " + backupDir.getAbsolutePath() + " does not exist.");
    }

    File restoreFile = getRestoreFile(backupDir);
    
    if(!restoreFile.exists()) {
      throw new IOException("Restore file " + restoreFile.getName() + " does not exist.");
    }
    
    BufferedReader reader = null;
    
    try {      
      reader = new BufferedReader(new FileReader(restoreFile));
      parseRestoreFile(reader);      
    } finally {
      if(null != reader) {
        reader.close();
      }
    }
  }
  
  /**
   * Searches for the incremental backup marker.
   * @param reader restore file reader.
   * @throws IOException
   */
  private void parseRestoreFile(BufferedReader reader) 
  throws IOException {    
    boolean markerFound = false;
    String line = null;
    String incrementalMarker = getIncrementalMarker();
    
    while(!markerFound && (null != (line = reader.readLine()))) {
      markerFound = line.startsWith(incrementalMarker);
    }
    
    if(markerFound) {
      parseOplogLines(reader);
    }
  }
  
  /**
   * @return true if the host operating system is windows.
   */
  public static boolean isWindows() {
    return (System.getProperty("os.name").indexOf("Windows") != -1);
  }
    
  /**
   * Returns true if the restore script is incremental.
   */
  public boolean isIncremental() {
    return !this.oplogFileNames.isEmpty();
  }
  
  /**
   * @return the backup directory being inspected.
   */
  public File getBackupDir() {
    return this.backupDir;
  }
  
  /**
   * Returns the restore script line that restores a particular operation log file.
   * @param oplogFileName an operation log file.
   */
  public String getScriptLineForOplogFile(String oplogFileName) {
    return this.oplogLineMap.get(oplogFileName);
  }
  
  /**
   * Returns the set of operation log files copied in the incremental backup section
   * of the restore script. 
   */
  public Set<String> getIncrementalOplogFileNames() {
    return Collections.unmodifiableSet(this.oplogFileNames);
  }
  
  /**
   * @return the incremental marke contained in the backup restore script.
   */
  protected abstract String getIncrementalMarker();
  
  /**
   * Returns the restore script for the backup.
   * @param backupDir a member's backup directory.
   */
  protected abstract File getRestoreFile(final File backupDir);
  
  /**
   * Returns the copyTo operation log file path for an operation log file name.
   * @param oplogFileName an operation log file.
   */
  public abstract String getCopyToForOplogFile(String oplogFileName);
  
  /**
   * Returns the copy from operation log file path for an operation log file name.
   * @param oplogFileName an operation log file.
   */
  public abstract String getCopyFromForOplogFile(String oplogFileName);

  /**
   * Parses out operation log data from the incremental backup portion
   * of the restore script.
   * @param reader restore file reader.
   * @throws IOException
   */
  protected abstract void parseOplogLines(BufferedReader reader) throws IOException ;
}

/**
 * A BackupInspector for the Windows platform(s).
 * @author rholmes
 *
 */
class WindowsBackupInspector extends BackupInspector {
  /**
   * When found indicates that the restore script was produced from an incremental backup. 
   */
  private static final String INCREMENTAL_MARKER = "rem Incremental backup";

  /**
   * Restore file for windows platform.
   */
  static final String RESTORE_FILE = "restore.bat";
  
  WindowsBackupInspector(File backupDir) 
  throws IOException {
    super(backupDir);
  }

  @Override
  public String getCopyFromForOplogFile(String oplogFileName) {
    String copyFrom = null;
    String line = this.oplogLineMap.get(oplogFileName);
    
    if(null != line) {
      String[]  parts = line.split("\\s");
      copyFrom = parts[1].substring(1, parts[1].length() - 1) + File.separator + parts[3];
    }
    
    return copyFrom;
  }

  @Override
  public String getCopyToForOplogFile(String oplogFileName) {
    String copyTo = null;
    String line = this.oplogLineMap.get(oplogFileName);
    
    if(null != line) {
      String[]  parts = line.split("\\s");
      copyTo = parts[2].substring(1, parts[2].length() - 1) + File.separator + parts[3];
    }
    
    return copyTo;
  }

  @Override
  /**
   * Parses out operation log data from the incremental backup portion
   * of the restore script.
   * @param reader restore file reader.
   * @throws IOException
   */
  protected void parseOplogLines(BufferedReader reader) throws IOException {
    String line = null;

    int beginIndex, endIndex;
    String oplogName = "";
    while(null != (line = reader.readLine())) {

      if (line.startsWith("robocopy")) {
        beginIndex = line.lastIndexOf("\"") + 2;
        endIndex = line.indexOf("/njh", beginIndex) - 1;
        oplogName = line.substring(beginIndex, endIndex);
        this.oplogFileNames.add(oplogName);
        this.oplogLineMap.put(oplogName, line);
      } else if (line.startsWith("IF")) {
        continue;
      } else if (line.contains(RestoreScript.EXIT_MARKER)) {
        break;
      }
    }
  }

  @Override
  protected String getIncrementalMarker() {
    return INCREMENTAL_MARKER;
  }

  @Override
  protected File getRestoreFile(final File backupDir) {
    return new File(backupDir,RESTORE_FILE);
  }
}

/**
 * A BackupInspector for Unix platforms.
 * @author rholmes
 */
class UnixBackupInspector extends BackupInspector {
  /**
   * When found indicates that the restore script was produced from an incremental backup. 
   */
  private static final String INCREMENTAL_MARKER = "#Incremental backup";

  /**
   * Restore file for windows platform.
   */
  static final String RESTORE_FILE = "restore.sh";
  
  UnixBackupInspector(File backupDir) 
  throws IOException {
    super(backupDir);
  }

  @Override
  public String getCopyFromForOplogFile(String oplogFileName) {
    String copyFrom = null;
    String line = this.oplogLineMap.get(oplogFileName);
    
    if(null != line) {
      String[]  parts = line.split("\\s");        
      copyFrom = parts[2].substring(1, parts[2].length() - 1);
    }
    
    return copyFrom;
  }

  @Override
  public String getCopyToForOplogFile(String oplogFileName) {
    String copyTo = null;
    String line = this.oplogLineMap.get(oplogFileName);
    
    if(null != line) {
      String[]  parts = line.split("\\s");        
      copyTo = parts[3].substring(1, parts[3].length() - 1);
    }
    
    return copyTo;
  }

  @Override
  /**
   * Parses out operation log data from the incremental backup portion
   * of the restore script.
   * @param reader restore file reader.
   * @throws IOException
   */
  protected void parseOplogLines(BufferedReader reader) throws IOException {
    String line = null;

    while(null != (line = reader.readLine())) {
      int beginIndex = line.lastIndexOf(File.separator) + 1;
      int endIndex = line.length() - 1;
      String oplogName = line.substring(beginIndex, endIndex);
      this.oplogFileNames.add(oplogName);
      this.oplogLineMap.put(oplogName, line);
    }
  }

  @Override
  protected String getIncrementalMarker() {
    return INCREMENTAL_MARKER;
  }

  @Override
  protected File getRestoreFile(final File backupDir) {
    return new File(backupDir,RESTORE_FILE);
  }
}
