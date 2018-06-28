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
package org.apache.geode.distributed.internal.membership.gms;

import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;

import org.apache.geode.CancelException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.MembershipManager;
import org.apache.geode.distributed.internal.membership.gms.interfaces.Manager;
import org.apache.geode.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;

/**
 * This helper class provides access to membership manager information that is not otherwise public
 *
 * @since GemFire 5.5
 */
public class MembershipManagerHelper {

  /** returns the JGroupMembershipManager for the given distributed system */
  public static MembershipManager getMembershipManager(DistributedSystem sys) {
    InternalDistributedSystem isys = (InternalDistributedSystem) sys;
    ClusterDistributionManager dm = (ClusterDistributionManager) isys.getDM();
    return dm.getMembershipManager();
  }

  /**
   * act sick. don't accept new connections and don't process ordered messages. Use
   * beHealthyMember() to reverse the effects.
   * <p>
   * Note that part of beSickMember's processing is to interrupt and stop any reader threads. A slow
   * listener in a reader thread should eat this interrupt.
   *
   */
  public static void beSickMember(DistributedSystem sys) {
    ((Manager) getMembershipManager(sys)).beSick();
  }

  /**
   * inhibit failure detection responses. This can be used in conjunction with beSickMember
   */
  public static void playDead(DistributedSystem sys) {
    try {
      ((Manager) getMembershipManager(sys)).playDead();
    } catch (CancelException e) {
      // really dead is as good as playing dead
    }
  }

  /** returns the current coordinator address */
  public static DistributedMember getCoordinator(DistributedSystem sys) {
    return ((Manager) getMembershipManager(sys)).getCoordinator();
  }

  /** returns the current lead member address */
  public static DistributedMember getLeadMember(DistributedSystem sys) {
    return ((Manager) getMembershipManager(sys)).getLeadMember();
  }

  /** register a test hook with the manager */
  public static void addTestHook(DistributedSystem sys,
      org.apache.geode.distributed.internal.membership.MembershipTestHook hook) {
    getMembershipManager(sys).registerTestHook(hook);
  }

  /** remove a registered test hook */
  public static void removeTestHook(DistributedSystem sys,
      org.apache.geode.distributed.internal.membership.MembershipTestHook hook) {
    getMembershipManager(sys).unregisterTestHook(hook);
  }

  /**
   * add a member id to the surprise members set, with the given millisecond clock birth time
   */
  public static void addSurpriseMember(DistributedSystem sys, DistributedMember mbr,
      long birthTime) {
    ((Manager) getMembershipManager(sys)).addSurpriseMemberForTesting(mbr, birthTime);
  }

  /**
   * inhibits/enables logging of forced-disconnect messages. For quorum-lost messages this adds
   * expected-exception annotations before and after the messages to make them invisible to greplogs
   */
  public static void inhibitForcedDisconnectLogging(boolean b) {
    GMSMembershipManager.inhibitForcedDisconnectLogging(b);
  }

  /**
   * wait for a member to leave the view. Throws an assertionerror if the timeout period elapses
   * before the member leaves
   */
  public static void waitForMemberDeparture(final DistributedSystem sys,
      final DistributedMember member, final long timeout) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return !getMembershipManager(sys).getView().contains(member);
      }

      public String description() {
        return "Waited over " + timeout + " ms for " + member + " to depart, but it didn't";
      }
    };
    Wait.waitForCriterion(ev, timeout, 200, true);
  }

  public static void crashDistributedSystem(final DistributedSystem msys) {
    msys.getLogWriter().info("crashing distributed system: " + msys);
    GMSMembershipManager mgr = ((GMSMembershipManager) getMembershipManager(msys));
    mgr.saveCacheXmlForReconnect(false);
    MembershipManagerHelper.inhibitForcedDisconnectLogging(true);
    MembershipManagerHelper.beSickMember(msys);
    MembershipManagerHelper.playDead(msys);
    mgr.forceDisconnect("for testing");
    // wait at most 10 seconds for system to be disconnected
    Awaitility.await().pollInterval(1, TimeUnit.SECONDS).until(() -> !msys.isConnected());
    MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
  }

}
