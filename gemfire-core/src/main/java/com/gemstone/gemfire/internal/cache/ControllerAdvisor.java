/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ServerLocator;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;


/**
 * Used to give advise to a connection controller.
 * Bridge server currently need to know about controller's
 * @author darrel
 *
 */
public class ControllerAdvisor extends GridAdvisor {
  
  /** Creates a new instance of ControllerAdvisor */
  private ControllerAdvisor(DistributionAdvisee server) {
    super(server);
  }
  
  public static ControllerAdvisor createControllerAdvisor(DistributionAdvisee server) {
    ControllerAdvisor advisor = new ControllerAdvisor(server);
    advisor.initialize();
    return advisor;
  }

  /**
   * Tell everyone else who we are and find out who they are.
   */
  @Override
  public final void handshake() {
    super.handshake();
    // also inform local SQLFabric advisor if any
    final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    final DistributionAdvisee sqlfAdvisee;
    final DistributionAdvisor sqlfAdvisor;
    GridProfile profile = (GridProfile) createProfile();
    if (cache != null && (sqlfAdvisee = cache.getSqlfAdvisee()) != null
        && (sqlfAdvisor = sqlfAdvisee.getDistributionAdvisor()) != null) {
      sqlfAdvisor.putProfile(profile);
    }
  }

  @Override
  protected void profileCreated(Profile profile) {
    super.profileCreated(profile);
    ((ServerLocator) getAdvisee()).profileCreated(profile);
  }
  
  @Override
  protected void profileRemoved(Profile profile) {
    super.profileRemoved(profile);
    ((ServerLocator) getAdvisee()).profileRemoved(profile);
  }

  @Override
  protected void profileUpdated(Profile profile) {
    super.profileUpdated(profile);
    ((ServerLocator) getAdvisee()).profileUpdated(profile);
  }



  @Override
  protected void profilesChanged() {
    if (pollIsInitialized()) {
      super.profilesChanged();
      ServerLocator sl = (ServerLocator)getAdvisee();
      sl.setLocatorCount(getControllerCount());
      sl.setServerCount(getBridgeServerCount());
    }
  }

  @Override
  public String toString() {
    return "ControllerAdvisor for " + getAdvisee().getFullPath();
  }

  /** Instantiate new distribution profile for this member */
  @Override
  protected Profile instantiateProfile(
      InternalDistributedMember memberId, int version) {
    return new ControllerProfile(memberId, version);
  }
  
  /**
   * Describes a bridge server for distribution purposes.
   */
  public static class ControllerProfile extends GridAdvisor.GridProfile {

    /** for internal use, required for DataSerializer.readObject */
    public ControllerProfile() {
    }

    public ControllerProfile(InternalDistributedMember memberId, int version) {
      super(memberId, version);
    }

    public ControllerProfile(ControllerProfile toCopy) {
      super(toCopy);
    }

    /**
     * Used to process an incoming connection controller profile. Any controller
     * or bridge server in this vm needs to be told about this incoming new
     * controller. The reply needs to contain any controller(s) that exist in
     * this vm and any bridge servers that exist in this vm.
     * 
     * @since 5.7
     */
    @Override
    public void processIncoming(DistributionManager dm, String adviseePath,
        boolean removeProfile, boolean exchangeProfiles,
        final List<Profile> replyProfiles) {
      // tell local controllers about this remote controller
      tellLocalControllers(removeProfile, exchangeProfiles, replyProfiles);
      // tell local bridge servers about this remote controller
      tellLocalBridgeServers(removeProfile, exchangeProfiles, replyProfiles);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
    }

    @Override
    public int getDSFID() {
      return CONTROLLER_PROFILE;
    }

    @Override
    public StringBuilder getToStringHeader() {
      return new StringBuilder("ControllerProfile");
    }

    @Override
    public void fillInToString(StringBuilder sb) {
      super.fillInToString(sb);
    }
  }
}
