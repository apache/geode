/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.internal.process;

import com.gemstone.gemfire.GemFireException;

/**
 * The ProcessTerminatedAbnormallyException class is a GemFireException (or RuntimeException) indicating that a process
 * terminated abnormally, and it's exit code is captured along with this RuntimeException.
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.GemFireException
 * @since 7.0
 */
public final class ProcessTerminatedAbnormallyException extends GemFireException {
  private static final long serialVersionUID = -1181367425266595492L;
  private final int exitValue;

  /**
   * Constructs an instance of the ProcessTerminatedAbnormallyException class with the given exit value of the process.
   * </p>
   * @param exitValue an integer value indicating the exit value of the terminated process.
   */
  public ProcessTerminatedAbnormallyException(final int exitValue) {
    this.exitValue = exitValue;
  }

  /**
   * Constructs an instance of the ProcessTerminatedAbnormallyException class with the given exit value of the process
   * and a message indicating the reason of the abnormal termination.
   * </p>
   * @param exitValue an integer value indicating the exit value of the terminated process.
   * @param message a String indicating the reason the process terminated abnormally.
   */
  public ProcessTerminatedAbnormallyException(final int exitValue, final String message) {
    super(message);
    this.exitValue = exitValue;
  }

  /**
   * Constructs an instance of the ProcessTerminatedAbnormallyException class with the given exit value of the process
   * and a Throwable representing the underlying cause of the process termination.
   * </p>
   * @param exitValue an integer value indicating the exit value of the terminated process.
   * @param cause a Throwable encapsulating the undelrying cause of the process termination.
   */
  public ProcessTerminatedAbnormallyException(final int exitValue, final Throwable cause) {
    super(cause);
    this.exitValue = exitValue;
  }

  /**
   * Constructs an instance of the ProcessTerminatedAbnormallyException class with the given exit value of the process
   * as well as a message indicating the reason of the abnormal termination along with a Throwable representing the
   * underlying cause of the process termination.
   * </p>
   * @param exitValue an integer value indicating the exit value of the terminated process.
   * @param message a String indicating the reason the process terminated abnormally.
   * @param cause a Throwable encapsulating the undelrying cause of the process termination.
   */
  public ProcessTerminatedAbnormallyException(final int exitValue, final String message, final Throwable cause) {
    super(message, cause);
    this.exitValue = exitValue;
  }

  /**
   * Gets the exit value returned by the process when it terminated.
   * </p>
   * @return an integer value indicating the exit value of the terminated process.
   */
  public int getExitValue() {
    return exitValue;
  }

}
