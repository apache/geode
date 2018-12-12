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
import java.util.Set;

import org.apache.geode.admin.DistributedSystemHealthConfig;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * Contains the logic for evaluating the health of an entire GemFire distributed system according to
 * the thresholds provided in a {@link DistributedSystemHealthConfig}.
 *
 * <P>
 *
 * Note that unlike other evaluators, the <code>DistributedSystemHealthEvaluator</code> resides in
 * the "administrator" VM and not in the member VMs. This is because there only needs to be one
 * <code>DistributedSystemHealthEvaluator</code> per distributed system.
 *
 *
 * @since GemFire 3.5
 */
class DistributedSystemHealthEvaluator extends AbstractHealthEvaluator
    implements MembershipListener {

  /** The config from which we get the evaluation criteria */
  private DistributedSystemHealthConfig config;

  /**
   * The distribution manager with which this MembershipListener is registered
   */
  private DistributionManager dm;

  /** The description of the distributed system being evaluated */
  private String description;

  /**
   * The number of application members that have unexpectedly left since the previous evaluation
   */
  private int crashedApplications;

  /////////////////////// Constructors ///////////////////////

  /**
   * Creates a new <code>DistributedSystemHealthEvaluator</code>
   */
  DistributedSystemHealthEvaluator(DistributedSystemHealthConfig config, DistributionManager dm) {
    super(null, dm);

    this.config = config;
    this.dm = dm;
    this.dm.addMembershipListener(this);

    StringBuffer sb = new StringBuffer();
    sb.append("Distributed System ");

    String desc = null;
    if (dm instanceof ClusterDistributionManager) {
      desc = ((ClusterDistributionManager) dm).getDistributionConfigDescription();
    }

    if (desc != null) {
      sb.append(desc);

    } else {
      DistributionConfig dsc = dm.getSystem().getConfig();
      String locators = dsc.getLocators();
      if (locators == null || locators.equals("")) {
        sb.append("using multicast ");
        sb.append(dsc.getMcastAddress());
        sb.append(":");
        sb.append(dsc.getMcastPort());

      } else {
        sb.append("using locators ");
        sb.append(locators);
      }
    }

    this.description = sb.toString();
  }

  //////////////////// Instance Methods ////////////////////

  @Override
  protected String getDescription() {
    return this.description;
  }

  /**
   * Checks to make sure that the number of application members of the distributed system that have
   * left unexpected since the last evaluation is less than the
   * {@linkplain DistributedSystemHealthConfig#getMaxDepartedApplications threshold}. If not, the
   * status is "poor" health.
   */
  void checkDepartedApplications(List status) {
    synchronized (this) {
      long threshold = this.config.getMaxDepartedApplications();
      if (this.crashedApplications > threshold) {
        String s =
            String.format(
                "The number of applications that have left the distributed system (%s) exceeds the threshold (%s)",

                new Object[] {Long.valueOf(this.crashedApplications), Long.valueOf(threshold)});
        status.add(poorHealth(s));
      }
      this.crashedApplications = 0;
    }
  }

  @Override
  protected void check(List status) {
    checkDepartedApplications(status);
  }

  @Override
  void close() {
    this.dm.removeMembershipListener(this);
  }

  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {

  }

  /**
   * Keeps track of which members depart unexpectedly
   */
  @Override
  public void memberDeparted(DistributionManager distributionManager, InternalDistributedMember id,
      boolean crashed) {
    if (!crashed)
      return;
    synchronized (this) {
      int kind = id.getVmKind();
      switch (kind) {
        case ClusterDistributionManager.LOCATOR_DM_TYPE:
        case ClusterDistributionManager.NORMAL_DM_TYPE:
          this.crashedApplications++;
          break;
        default:
          break;
      }
    } // synchronized
  }

  public void quorumLost(DistributionManager distributionManager,
      Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}

  public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {}

}
