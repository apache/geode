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

package com.gemstone.gemfire.internal.process;

import com.gemstone.gemfire.GemFireException;

/**
 * The ProcessTerminatedAbnormallyException class is a GemFireException (or RuntimeException) indicating that a process
 * terminated abnormally, and it's exit code is captured along with this RuntimeException.
 * </p>
 * @see com.gemstone.gemfire.GemFireException
 * @since GemFire 7.0
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
