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
package com.gemstone.gemfire.internal.logging;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import junit.framework.Assert;

import com.gemstone.gemfire.GemFireIOException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.Banner;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.process.ProcessLauncherContext;
import com.gemstone.gemfire.internal.util.LogFileUtils;

/**
 * Creates LogWriter instances for testing.
 * 
 */
public class TestLogWriterFactory extends Assert {

  public static LogWriter createLogWriter(final boolean appendToFile,
                                          final boolean isLoner,
                                          final boolean isSecurityLog,
                                          final DistributionConfig config,
                                          final boolean logConfig, 
                                          final FileOutputStream[] FOSHolder) {
    
    assertFalse(isSecurityLog);
    LogWriter logger = null;
    File logFile = config.getLogFile();
    assertNotNull(logFile);
    PrintStream out;
    String firstMsg = null;
    boolean firstMsgWarning = false;

    LogWriter mlw = null;
    
    if (logFile == null || logFile.equals(new File(""))) {
      out = System.out;
    } else {
      if (logFile.exists()) {
        boolean useChildLogging = config.getLogFile() != null && !config.getLogFile().equals(new File("")) && config.getLogFileSizeLimit() != 0;
        boolean statArchivesRolling = config.getStatisticArchiveFile() != null && !config.getStatisticArchiveFile().equals(new File("")) && config.getArchiveFileSizeLimit() != 0 && config.getStatisticSamplingEnabled();
        if (!appendToFile || useChildLogging || statArchivesRolling) { // check useChildLogging for bug 50659
          File oldMain = ManagerLogWriter.getLogNameForOldMainLog(logFile, isSecurityLog || useChildLogging || statArchivesRolling);
          boolean succeeded = LogFileUtils.renameAggressively(logFile,oldMain);
          if(succeeded) {
            firstMsg = LocalizedStrings.InternalDistributedSystem_RENAMED_OLD_LOG_FILE_TO_0.toLocalizedString(oldMain);
          } else {
            firstMsgWarning = true;
            firstMsg = LocalizedStrings.InternalDistributedSystem_COULD_NOT_RENAME_0_TO_1.toLocalizedString(new Object[] {logFile, oldMain});
          }
        }
      }
      
      FileOutputStream fos;
      try {
        fos = new FileOutputStream(logFile, true);
      } catch (FileNotFoundException ex) {
        String s = LocalizedStrings.InternalDistributedSystem_COULD_NOT_OPEN_LOG_FILE_0.toLocalizedString(logFile);
        throw new GemFireIOException(s, ex);
      }
      out = new PrintStream(fos);
      if (FOSHolder != null) {
        FOSHolder[0] = fos;
      }

      if (isSecurityLog) {
        mlw = new SecurityManagerLogWriter(config.getSecurityLogLevel(), out, config.getName());
      } else {
        mlw = new ManagerLogWriter(config.getLogLevel(), out, config.getName());
      }
      ((ManagerLogWriter)mlw).setConfig(config);
    }
    
    if (mlw.infoEnabled()) {
      if (!isLoner || /* do this on a loner to fix bug 35602 */
          !Boolean.getBoolean(InternalLocator.INHIBIT_DM_BANNER)) {
        mlw.info(Banner.getString(null));
      }
    }
    logger = mlw;

    if (firstMsg != null) {
      if (firstMsgWarning) {
        logger.warning(firstMsg);
      } else {
        logger.info(firstMsg);
      }
    }
    if (logConfig && logger.configEnabled()) {
      logger.convertToLogWriterI18n().config(LocalizedStrings.InternalDistributedSystem_STARTUP_CONFIGURATIONN_0, config.toLoggerString());
    }
    if (isLoner) {
      logger.convertToLogWriterI18n().info(LocalizedStrings.InternalDistributedSystem_RUNNING_IN_LOCAL_MODE_SINCE_MCASTPORT_WAS_0_AND_LOCATORS_WAS_EMPTY);
    }
    
    // fix #46493 by moving redirectOutput invocation here
    if (ProcessLauncherContext.isRedirectingOutput()) {
      try {
        OSProcess.redirectOutput(config.getLogFile());
      } catch (IOException e) {
        logger.error(e);
        //throw new GemFireIOException("Unable to redirect output to " + config.getLogFile(), e);
      }
    }

    return logger;
  }
  
}
