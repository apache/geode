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

package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.ServerLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;


/**
 * Used to give advise to a connection controller. cache server currently need to know about
 * controller's
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
      ServerLocator sl = (ServerLocator) getAdvisee();
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
  protected Profile instantiateProfile(InternalDistributedMember memberId, int version) {
    return new ControllerProfile(memberId, version);
  }

  /**
   * Describes a cache server for distribution purposes.
   */
  public static class ControllerProfile extends GridAdvisor.GridProfile {

    /** for internal use, required for DataSerializer.readObject */
    public ControllerProfile() {}

    public ControllerProfile(InternalDistributedMember memberId, int version) {
      super(memberId, version);
    }

    public ControllerProfile(ControllerProfile toCopy) {
      super(toCopy);
    }

    /**
     * Used to process an incoming connection controller profile. Any controller or cache server in
     * this vm needs to be told about this incoming new controller. The reply needs to contain any
     * controller(s) that exist in this vm and any cache servers that exist in this vm.
     *
     * @since GemFire 5.7
     */
    @Override
    public void processIncoming(ClusterDistributionManager dm, String adviseePath,
        boolean removeProfile, boolean exchangeProfiles, final List<Profile> replyProfiles) {
      // tell local controllers about this remote controller
      tellLocalControllers(removeProfile, exchangeProfiles, replyProfiles);
      // tell local cache servers about this remote controller
      tellLocalBridgeServers(dm.getCache(), removeProfile, exchangeProfiles, replyProfiles);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
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
