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
package org.apache.geode.internal.statistics.platform;

import org.apache.geode.Statistics;

/**
 * Abstracts the process statistics that are common on all platforms. This is necessary for
 * monitoring the health of GemFire components.
 *
 *
 * @since GemFire 3.5
 */
public abstract class ProcessStats {

  /** The underlying statistics */
  private final Statistics stats;

  /**
   * Creates a new <code>ProcessStats</code> that wraps the given <code>Statistics</code>.
   */
  ProcessStats(Statistics stats) {
    this.stats = stats;
  }

  /**
   * Closes these process stats
   *
   * @see Statistics#close
   */
  public void close() {
    stats.close();
  }

  public Statistics getStatistics() {
    return stats;
  }

  /**
   * Returns the size of this process (resident set on UNIX or working set on Windows) in megabytes
   */
  public abstract long getProcessSize();

}
