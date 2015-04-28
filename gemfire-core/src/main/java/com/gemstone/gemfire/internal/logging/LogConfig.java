package com.gemstone.gemfire.internal.logging;

import java.io.File;

public interface LogConfig {
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#log-level">"log-level"</a> property
   *
   * @see com.gemstone.gemfire.internal.logging.LogWriterImpl
   */
  public int getLogLevel();
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#log-file">"log-file"</a> property
   *
   * @return <code>null</code> if logging information goes to standard
   *         out
   */
  public File getLogFile();
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#log-file-size-limit">"log-file-size-limit"</a>
   * property
   */
  public int getLogFileSizeLimit();
  /**
   * Returns the value of the <a
   * href="../DistributedSystem.html#log-disk-space-limit">"log-disk-space-limit"</a>
   * property
   */
  public int getLogDiskSpaceLimit();
  
  public String getName();
  
  public String toLoggerString();
}
