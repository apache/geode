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
package org.apache.geode.internal.logging;

import java.io.File;

import org.apache.geode.distributed.internal.DistributionConfig;

public interface LogConfig {

  /**
   * Returns the value of the <a href="../DistributedSystem.html#log-level">"log-level"</a> property
   *
   * @see LogWriterImpl
   */
  int getLogLevel();

  /**
   * Returns the value of the <a href="../DistributedSystem.html#log-file">"log-file"</a> property
   *
   * @return <code>null</code> if logging information goes to standard out
   */
  File getLogFile();

  /**
   * Returns the value of the
   * <a href="../DistributedSystem.html#log-file-size-limit">"log-file-size-limit"</a> property
   */
  int getLogFileSizeLimit();

  /**
   * Returns the value of the
   * <a href="../DistributedSystem.html#log-disk-space-limit">"log-disk-space-limit"</a> property
   */
  int getLogDiskSpaceLimit();

  /**
   * Returns the value of the <a href="../DistributedSystem.html#name">"name"</a> property Gets the
   * member's name. A name is optional and by default empty. If set it must be unique in the ds.
   * When set its used by tools to help identify the member.
   *
   * <p>
   * The default value is:
   * {@link DistributionConfig#DEFAULT_NAME}.
   *
   * @return the system's name.
   */
  String getName();

  String toLoggerString();
}
