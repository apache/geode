/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.gopivotal.com/patents.
 *========================================================================
 */

package hydra;

import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.LogWriter;

/**
 * Provides version-dependent support for logging changes.
 */
public class LogVersionHelper {

  protected static String levelToString(int level) {
    return LogWriterImpl.levelToString(level);
  }

  protected static int levelNameToCode(String level) {
    return LogWriterImpl.levelNameToCode(level);
  }

  protected static ThreadGroup getLoggingThreadGroup(String group, LogWriter logger) {
    return LoggingThreadGroup.createThreadGroup(group, (InternalLogWriter)logger);
  }

  protected static String getMergeLogFilesClassName() {
    return "com.gemstone.gemfire.internal.logging.MergeLogFiles";
  }
}
