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

package org.apache.geode.admin.internal;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.internal.admin.ApplicationVM;
import org.apache.geode.internal.admin.GemFireVM;
import org.apache.geode.internal.admin.GfManagerAgent;
import org.apache.geode.internal.logging.MergeLogFiles;

public class LogCollator {

  private GfManagerAgent system;
  private List logTails;

  public LogCollator() {}

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
    } finally {
      this.system = null;
      this.logTails = null;
    }
  }

  // -------------------------------------------------------------------------

  private String mergeLogs() {
    // combine logs...
    Map<String, InputStream> logFiles = new HashMap<>();
    for (int i = 0; i < this.logTails.size(); i++) {
      Loglet loglet = (Loglet) this.logTails.get(i);
      logFiles.put(loglet.name, new ByteArrayInputStream(loglet.tail.getBytes()));
    }

    // delegate to MergeLogFiles...
    StringWriter writer = new StringWriter();
    PrintWriter mergedLog = new PrintWriter(writer);
    if (!MergeLogFiles.mergeLogFiles(logFiles, mergedLog)) {
      return writer.toString();
    } else {
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
    /*
     * not yet supported.... if (useStopped) { LogViewHelper helper = new LogViewHelper(); for
     * (Iterator iter = stoppedNodes.iterator(); iter.hasNext(); ) { Object adminEntity =
     * iter.next(); helper.setAdminEntity(adminEntity); try { if (helper.logViewAvailable()) {
     * String[] logs = helper.getSystemLogs(); addTail(allTails, logs, adminEntity.toString()); } }
     * catch (Exception e) { Service.getService().reportSystemError(e); } } }
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
   * public void setUseStoppedManagers(boolean useStopped) { this.useStopped = useStopped; }
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
