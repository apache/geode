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
package org.apache.geode.distributed.internal;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;

import org.apache.geode.GemFireIOException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.logging.PureLogWriter;

/**
 * A log intended for recording product-use.
 *
 * This class wraps a {@link InternalLogWriter} which it uses to record messages. It wipes the log
 * when it gets too large. The size of the log file is limited to 5mb by default and can be adjusted
 * with the system property <b>gemfire.max_product_use_file_size</b>, though the size is not allowed
 * to be less than 1mb.
 *
 * @since GemFire 2013
 */
public class ProductUseLog implements MembershipListener {
  protected static long MAX_PRODUCT_USE_FILE_SIZE = Long.getLong("max_view_log_size", 5000000);
  private final int logLevel;
  private final File productUseLogFile;
  private PureLogWriter logWriter;
  private InternalDistributedSystem system;

  static {
    // don't allow malicious setting of the file size to something too small
    if (MAX_PRODUCT_USE_FILE_SIZE < 1000000) {
      MAX_PRODUCT_USE_FILE_SIZE = 1000000;
    }
  }

  public ProductUseLog(File productUseLogFile) {
    // GEODE-4180, use absolute paths
    this.productUseLogFile = productUseLogFile.getAbsoluteFile();
    this.logLevel = InternalLogWriter.INFO_LEVEL;
    createLogWriter();
  }

  /**
   * adds the log as a membership listener to the given system and logs the view when members join
   */
  public void monitorUse(InternalDistributedSystem system) {
    this.system = system;
    DistributionManager dmgr = system.getDistributionManager();
    dmgr.addMembershipListener(this);
    MembershipManager mmgr = dmgr.getMembershipManager();
    if (mmgr != null) {
      log("Log opened with new distributed system connection.  "
          + system.getDM().getMembershipManager().getView());
    } else { // membership manager not initialized?
      log("Log opened with new distributed system connection.  Membership view not yet available in this VM.");
    }
  }

  public synchronized void log(String logMessage) {
    if (!this.logWriter.isClosed()) {
      if (this.productUseLogFile.length() + logMessage.length() + 100 > MAX_PRODUCT_USE_FILE_SIZE) {
        clearLog();
      }
      this.logWriter.info(logMessage);
    }
  }

  /**
   * Closes the log. It may be reopened with reopen(). This does not remove the log from any
   * distributed systems it is monitoring.
   */
  public synchronized void close() {
    if (!this.logWriter.isClosed()) {
      this.logWriter.close();
    }
  }

  /**
   * returns true if the log has been closed
   */
  public synchronized boolean isClosed() {
    return this.logWriter.isClosed();
  }

  /** reopens a closed log */
  public synchronized void reopen() {
    if (this.logWriter.isClosed()) {
      createLogWriter();
    }
  }

  private synchronized void clearLog() {
    this.logWriter.close();
    this.productUseLogFile.delete();
    createLogWriter();
  }

  private synchronized void createLogWriter() {
    FileOutputStream fos;
    try {
      fos = new FileOutputStream(productUseLogFile, true);
    } catch (FileNotFoundException ex) {
      String s = String.format("Could not open log file %s.",
          productUseLogFile.getAbsolutePath());
      throw new GemFireIOException(s, ex);
    }
    PrintStream out = new PrintStream(fos);
    this.logWriter = new PureLogWriter(this.logLevel, out);
  }

  @Override
  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {
    log("A new member joined: " + id + ".  " + system.getDM().getMembershipManager().getView());
  }

  @Override
  public void memberDeparted(DistributionManager distributionManager, InternalDistributedMember id,
      boolean crashed) {}

  @Override
  public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {}

  @Override
  public void quorumLost(DistributionManager distributionManager,
      Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}

}
