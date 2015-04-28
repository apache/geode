/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.admin.DistributedSystemHealthConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.MembershipListener;
import java.util.*;

import com.gemstone.gemfire.distributed.internal.membership.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Contains the logic for evaluating the health of an entire GemFire
 * distributed system according to the thresholds provided in a {@link
 * DistributedSystemHealthConfig}.
 *
 * <P>
 *
 * Note that unlike other evaluators, the
 * <code>DistributedSystemHealthEvaluator</code> resides in the
 * "administrator" VM and not in the member VMs.  This is because
 * there only needs to be one
 * <code>DistributedSystemHealthEvaluator</code> per distributed
 * system.
 *
 * @author David Whitlock
 *
 * @since 3.5
 * */
class DistributedSystemHealthEvaluator
  extends AbstractHealthEvaluator implements MembershipListener {

  /** The config from which we get the evaluation criteria */
  private DistributedSystemHealthConfig config;

  /** The distribution manager with which this MembershipListener is
   * registered */
  private DM dm;

  /** The description of the distributed system being evaluated */
  private String description;

  /** The number of application members that have unexpectedly left
   * since the previous evaluation */
  private int crashedApplications;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>DistributedSystemHealthEvaluator</code>
   */
  DistributedSystemHealthEvaluator(DistributedSystemHealthConfig config,
                                   DM dm) {
    super(null, dm);

    this.config = config;
    this.dm = dm;
    this.dm.addMembershipListener(this);

    StringBuffer sb = new StringBuffer();
    sb.append("Distributed System ");

    String desc = null;
    if (dm instanceof DistributionManager) {
      desc = 
        ((DistributionManager) dm).getDistributionConfigDescription();
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

  ////////////////////  Instance Methods  ////////////////////

  @Override
  protected String getDescription() {
    return this.description;
  }

  /**  
   * Checks to make sure that the number of application members of
   * the distributed system that have left unexpected since the last
   * evaluation is less than the {@linkplain
   * DistributedSystemHealthConfig#getMaxDepartedApplications
   * threshold}.  If not, the status is "poor" health.
   */
  void checkDepartedApplications(List status) {
    synchronized (this) {
      long threshold = this.config.getMaxDepartedApplications();
      if (this.crashedApplications > threshold) {
        String s = LocalizedStrings.DistributedSystemHealth_THE_NUMBER_OF_APPLICATIONS_THAT_HAVE_LEFT_THE_DISTRIBUTED_SYSTEM_0_EXCEEDS_THE_THRESHOLD_1.toLocalizedString(new Object[] { Long.valueOf(this.crashedApplications), Long.valueOf(threshold)});
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

  public void memberJoined(InternalDistributedMember id) {

  }

  /**
   * Keeps track of which members depart unexpectedly
   */
  public void memberDeparted(InternalDistributedMember id, boolean crashed) {
    if (!crashed)
      return;
    synchronized (this) {
        int kind = id.getVmKind();
        switch (kind) {
        case DistributionManager.LOCATOR_DM_TYPE:
        case DistributionManager.NORMAL_DM_TYPE:
          this.crashedApplications++;
          break;
        default:
          break;
        }
    } // synchronized
  }

  public void quorumLost(Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
  }

  public void memberSuspect(InternalDistributedMember id,
      InternalDistributedMember whoSuspected) {
  }
  
}
