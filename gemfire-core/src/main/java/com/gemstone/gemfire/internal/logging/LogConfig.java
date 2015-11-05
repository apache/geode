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
