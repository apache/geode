/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.internal.admin.GfManagerAgent;
import com.gemstone.gemfire.internal.admin.GemFireVM;
import com.gemstone.gemfire.internal.admin.ApplicationVM;
import com.gemstone.gemfire.internal.logging.MergeLogFiles;

import java.io.ByteArrayInputStream;
import java.io.InputStream;  
import java.io.PrintWriter;  
import java.io.StringWriter;  
import java.util.ArrayList;
import java.util.List;

public class LogCollator {
  
  private GfManagerAgent system;
  private List logTails;
    
  public LogCollator() {
  }
  
  public String collateLogs(GfManagerAgent system) {
    try {
      if (system == null) {
        return "";
      }
      this.system = system;
      this.logTails = new ArrayList();
      gatherActiveLogs();
      gatherInactiveLogs();
      return mergeLogs();
    }
    finally {
      this.system = null;
      this.logTails = null;
    }
  }

  // -------------------------------------------------------------------------
  
  private String mergeLogs() {
    // combine logs...
    InputStream[] logFiles = new InputStream[this.logTails.size()];
    String[] logFileNames = new String[logFiles.length];
    for (int i = 0; i < this.logTails.size(); i++) {
      Loglet loglet = (Loglet) this.logTails.get(i);
      logFiles[i] = new ByteArrayInputStream(loglet.tail.getBytes());
      logFileNames[i] = loglet.name;
    }
    
    // delegate to MergeLogFiles...
    StringWriter writer = new StringWriter();
    PrintWriter mergedLog = new PrintWriter(writer);
    if (!MergeLogFiles.mergeLogFiles(logFiles, logFileNames, mergedLog)) {
      return writer.toString();
    } 
    else {
      return "";
    }
  }

  private void gatherActiveLogs() {
    ApplicationVM[] runningsApps = this.system.listApplications();
    for (int i = 0; i < runningsApps.length; i++) {
      addLogFrom(runningsApps[i]);
    }
  }
  
  private void gatherInactiveLogs() {
    /* not yet supported....
    if (useStopped) {
      LogViewHelper helper = new LogViewHelper();
      for (Iterator iter = stoppedNodes.iterator(); iter.hasNext(); ) {
        Object adminEntity = iter.next();
        helper.setAdminEntity(adminEntity);
        try {
          if (helper.logViewAvailable()) {
            String[] logs = helper.getSystemLogs();
            addTail(allTails, logs, adminEntity.toString());
          }
        } catch (Exception e) {
          Service.getService().reportSystemError(e);
        }
      }
    }
    */
  }
  
  private void addLogFrom(GemFireVM vm) {
    String name = null;
    name = vm.toString();
    String[] logs = vm.getSystemLogs();
    addTail(name, logs);
  }

  private void addTail(String logName, String[] logs) {
    if (logs.length > 0) {
      String tail = (logs.length > 1) ? logs[1] : logs[0];      
      this.logTails.add(new Loglet(logName, tail));
    }
  }

  /*
  public void setUseStoppedManagers(boolean useStopped) {
    this.useStopped = useStopped;
  }
  */

  private static class Loglet {
    String name;
    String tail;
    Loglet(String name, String tail) {
      this.name = name;
      this.tail = tail;
    }
  }
  
}

