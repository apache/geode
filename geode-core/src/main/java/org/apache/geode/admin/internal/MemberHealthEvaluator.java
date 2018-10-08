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

import java.util.List;

import org.apache.geode.CancelException;
import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.admin.MemberHealthConfig;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.statistics.GemFireStatSampler;
import org.apache.geode.internal.statistics.platform.ProcessStats;

/**
 * Contains the logic for evaluating the health of a GemFire distributed system member according to
 * the thresholds provided in a {@link MemberHealthConfig}.
 *
 * @since GemFire 3.5
 */
class MemberHealthEvaluator extends AbstractHealthEvaluator {

  /** The config from which we get the evaluation criteria */
  private final MemberHealthConfig config;

  /** The description of the member being evaluated */
  private final String description;

  /** Statistics about this process (may be null) */
  private ProcessStats processStats;

  /** Statistics about the distribution manager */
  private final DMStats dmStats;

  /** The previous value of the reply timeouts stat */
  private long prevReplyTimeouts;

  /**
   * Creates a new <code>MemberHealthEvaluator</code>
   */
  MemberHealthEvaluator(GemFireHealthConfig config, DistributionManager dm) {
    super(config, dm);

    this.config = config;
    InternalDistributedSystem system = dm.getSystem();

    GemFireStatSampler sampler = system.getStatSampler();
    if (sampler != null) {
      // Sampling is enabled
      this.processStats = sampler.getProcessStats();
    }

    this.dmStats = dm.getStats();

    StringBuilder sb = new StringBuilder();
    sb.append("Application VM member ");
    sb.append(dm.getId());
    int pid = OSProcess.getId();
    if (pid != 0) {
      sb.append(" with pid ");
      sb.append(pid);
    }
    this.description = sb.toString();
  }

  @Override
  protected String getDescription() {
    return this.description;
  }

  /**
   * Checks to make sure that the {@linkplain ProcessStats#getProcessSize VM's process size} is less
   * than the {@linkplain MemberHealthConfig#getMaxVMProcessSize threshold}. If not, the status is
   * "okay" health.
   */
  void checkVMProcessSize(List status) {
    // There is no need to check isFirstEvaluation()
    if (this.processStats == null) {
      return;
    }

    long vmSize = this.processStats.getProcessSize();
    long threshold = this.config.getMaxVMProcessSize();
    if (vmSize > threshold) {
      String s =
          String.format("The size of this VM (%s megabytes) exceeds the threshold (%s megabytes)",
              vmSize, threshold);
      status.add(okayHealth(s));
    }
  }

  /**
   * Checks to make sure that the size of the distribution manager's
   * {@linkplain DMStats#getOverflowQueueSize() overflow} message queue does not exceed the
   * {@linkplain MemberHealthConfig#getMaxMessageQueueSize threshold}. If not, the status is "okay"
   * health.
   */
  private void checkMessageQueueSize(List status) {
    long threshold = this.config.getMaxMessageQueueSize();
    long overflowSize = this.dmStats.getOverflowQueueSize();
    if (overflowSize > threshold) {
      String s =
          String.format("The size of the overflow queue (%s) exceeds the threshold (%s).",
              overflowSize, threshold);
      status.add(okayHealth(s));
    }
  }

  /**
   * Checks to make sure that the number of {@linkplain DMStats#getReplyTimeouts reply timeouts}
   * does not exceed the {@linkplain MemberHealthConfig#getMaxReplyTimeouts threshold}. If not, the
   * status is "okay" health.
   */
  private void checkReplyTimeouts(List status) {
    if (isFirstEvaluation()) {
      return;
    }

    long threshold = this.config.getMaxReplyTimeouts();
    long deltaReplyTimeouts = this.dmStats.getReplyTimeouts() - prevReplyTimeouts;
    if (deltaReplyTimeouts > threshold) {
      String s =
          String.format("The number of message reply timeouts (%s) exceeds the threshold (%s)",
              deltaReplyTimeouts, threshold);
      status.add(okayHealth(s));
    }
  }

  /**
   * The function keeps updating the health of the cache based on roles required by the regions and
   * their reliability policies.
   */
  private void checkCacheRequiredRolesMeet(List status) {
    // will have to call here okayHealth() or poorHealth()

    try {
      InternalCache cache = (InternalCache) CacheFactory.getAnyInstance();
      CachePerfStats cPStats = cache.getCachePerfStats();

      if (cPStats.getReliableRegionsMissingFullAccess() > 0) {
        // health is okay.
        int numRegions = cPStats.getReliableRegionsMissingFullAccess();
        status.add(okayHealth(
            String.format(
                "There are %s regions missing required roles; however, they are configured for full access.",
                numRegions)));
      } else if (cPStats.getReliableRegionsMissingLimitedAccess() > 0) {
        // health is poor
        int numRegions = cPStats.getReliableRegionsMissingLimitedAccess();
        status.add(poorHealth(
            String.format(
                "There are %s regions missing required roles and configured with limited access.",
                numRegions)));
      } else if (cPStats.getReliableRegionsMissingNoAccess() > 0) {
        // health is poor
        int numRegions = cPStats.getReliableRegionsMissingNoAccess();
        status.add(poorHealth(
            String.format(
                "There are %s regions missing required roles and configured without access.",
                numRegions)));
      }
    } catch (CancelException ignore) {
    }
  }

  /**
   * Updates the previous values of statistics
   */
  private void updatePrevious() {
    this.prevReplyTimeouts = this.dmStats.getReplyTimeouts();
  }

  @Override
  protected void check(List status) {
    checkVMProcessSize(status);
    checkMessageQueueSize(status);
    checkReplyTimeouts(status);
    // will have to add another call to check for roles
    // missing and reliability attributed.
    checkCacheRequiredRolesMeet(status);

    updatePrevious();
  }

  @Override
  void close() {
    // nothing
  }
}
