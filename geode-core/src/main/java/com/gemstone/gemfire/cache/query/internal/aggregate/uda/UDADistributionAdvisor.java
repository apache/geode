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
package com.gemstone.gemfire.cache.query.internal.aggregate.uda;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/**
 * Handles the exchange of UDAProfile with the peers. 
 * It is used for exchanging information about the UDAs existing in the system.
 * @author ashahid
 * @since 9.0
 */
public class UDADistributionAdvisor extends DistributionAdvisor {

  private UDADistributionAdvisor(DistributionAdvisee sender) {
    super(sender);

  }

  public static UDADistributionAdvisor createUDAAdvisor(DistributionAdvisee sender) {
    UDADistributionAdvisor advisor = new UDADistributionAdvisor(sender);
    advisor.initialize();
    return advisor;
  }

  /** Instantiate new Sender profile for this member */
  @Override
  protected Profile instantiateProfile(InternalDistributedMember memberId, int version) {
    return new UDAProfile(memberId, version);
  }

  @Override
  public Set<InternalDistributedMember> adviseProfileExchange() {
    InternalDistributedSystem ids = InternalDistributedSystem.getConnectedInstance();
    GemFireCacheImpl gfc = GemFireCacheImpl.getExisting();
    DM dm = ids.getDistributionManager();
    InternalDistributedMember elder = dm.getElderId();
    Set<InternalDistributedMember> locators = dm.getAllHostedLocators().keySet();
    if (elder == null || elder.equals(dm.getId()) || locators.contains(elder)) {
      elder = null;
      Set<InternalDistributedMember> allMembers = gfc.getDistributionAdvisor().adviseGeneric();
      Iterator<InternalDistributedMember> iter = allMembers.iterator();
      while(iter.hasNext()) {
        InternalDistributedMember temp = iter.next();
        if(!locators.contains(temp)) {
          elder = temp;
          break;
        }
      }
    }
    if (elder != null) {
      return Collections.singleton(elder);
    } else {
      return Collections.emptySet();
    }
  }

  @Override
  public Set<InternalDistributedMember> adviseProfileUpdate() {
    InternalDistributedSystem ids = InternalDistributedSystem.getConnectedInstance();   
    DM dm = ids.getDistributionManager();
    
    Set<InternalDistributedMember> locators = dm.getAllHostedLocators().keySet();
    GemFireCacheImpl gfc = GemFireCacheImpl.getExisting(); 
    Set<InternalDistributedMember> all = gfc.getDistributionAdvisor().adviseGeneric();
    all.removeAll(locators);
    return all;
  }

  /**
   * Create or update a profile for a remote counterpart.
   * 
   * @param profile
   *          the profile, referenced by this advisor after this method returns.
   */
  @Override
  public boolean putProfile(Profile profile) {
    UDAProfile udap = (UDAProfile) profile;
    GemFireCacheImpl.getExisting().getUDAManager().addUDAs(udap.udas, udap.getCheckforLocalOnlyUDAs());
    return true;
  }

  @Override
  public boolean removeProfile(Profile profile, boolean destroyed) {
    return true;
  }
  
  @Override
  public boolean initializationGate() {    
    return false;
  }

  /**
   * Profile information for a remote counterpart.
   */
  public static final class UDAProfile extends DistributionAdvisor.Profile {

    private HashMap<String, String> udas = new HashMap<String, String>();
    private boolean checkForLocalOnlyUDAs = false;
    public UDAProfile(InternalDistributedMember memberId, int version) {
      super(memberId, version);
    }

    public UDAProfile() {
    }
    
    void setCheckForLocalOnlyUDAsAsTrue() {
      this.checkForLocalOnlyUDAs = true;
    }
    
    boolean getCheckforLocalOnlyUDAs() {
      return this.checkForLocalOnlyUDAs;
    }
    
    void putInProfile(String udaName, String udaClass) {
      this.udas.put(udaName, udaClass);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.udas = DataSerializer.readHashMap(in);
      this.checkForLocalOnlyUDAs = DataSerializer.readPrimitiveBoolean(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeHashMap(this.udas, out);
      DataSerializer.writePrimitiveBoolean(this.checkForLocalOnlyUDAs, out);
    }

    @Override
    public Version[] getSerializationVersions() {
      return null;
    }

    @Override
    public int getDSFID() {
      return UDA_PROFILE;
    }

    @Override
    public void processIncoming(DistributionManager dm, String adviseePath, boolean removeProfile, boolean exchangeProfiles, final List<Profile> replyProfiles) {
      handleDistributionAdvisee(GemFireCacheImpl.getExisting().getUDAManager(), removeProfile, exchangeProfiles, replyProfiles);
    }

    @Override
    public void collectProfile(DistributionManager dm, String adviseePath, final List<Profile> replyProfiles) {
      UDAProfile udap = (UDAProfile)GemFireCacheImpl.getExisting().getUDAManager().getProfile();
      udap.setCheckForLocalOnlyUDAsAsTrue();
      replyProfiles.add(udap);

    }
  }
}
