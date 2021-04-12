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
import java.util.Arrays;
import java.util.List;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.server.ServerLoad;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;


/**
 * Used to give advice to a cache server. Cache server currently need to know about controller's
 *
 */
public class CacheServerAdvisor extends GridAdvisor {

  private CacheServerAdvisor(DistributionAdvisee server) {
    super(server);
  }

  public static CacheServerAdvisor createCacheServerAdvisor(DistributionAdvisee server) {
    CacheServerAdvisor advisor = new CacheServerAdvisor(server);
    advisor.initialize();
    return advisor;
  }

  @Override
  public String toString() {
    return "CacheServerAdvisor for " + getAdvisee().getFullPath();
  }

  /** Instantiate new distribution profile for this member */
  @Override
  protected Profile instantiateProfile(InternalDistributedMember memberId, int version) {
    return new CacheServerProfile(memberId, version);
  }

  /**
   * Describes a cache server for distribution purposes.
   */
  public static class CacheServerProfile extends GridAdvisor.GridProfile {
    private String[] groups;
    private int maxConnections;
    private ServerLoad initialLoad;
    private long loadPollInterval;

    /** for internal use, required for DataSerializer.readObject */
    public CacheServerProfile() {}

    public CacheServerProfile(InternalDistributedMember memberId, int version) {
      super(memberId, version);
    }

    public CacheServerProfile(CacheServerProfile toCopy) {
      super(toCopy);
      this.groups = toCopy.groups;
    }

    /** don't modify the returned array! */
    public String[] getGroups() {
      return this.groups;
    }

    public void setGroups(String[] groups) {
      this.groups = groups;
    }

    public ServerLoad getInitialLoad() {
      return initialLoad;
    }

    public int getMaxConnections() {
      return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
      this.maxConnections = maxConnections;
    }

    public void setInitialLoad(ServerLoad initialLoad) {
      this.initialLoad = initialLoad;
    }

    public long getLoadPollInterval() {
      return this.loadPollInterval;
    }

    public void setLoadPollInterval(long v) {
      this.loadPollInterval = v;
    }

    /**
     * Used to process an incoming cache server profile. Any controller in this vm needs to be told
     * about this incoming new cache server. The reply needs to contain any controller(s) that exist
     * in this vm.
     *
     * @since GemFire 5.7
     */
    @Override
    public void processIncoming(ClusterDistributionManager dm, String adviseePath,
        boolean removeProfile, boolean exchangeProfiles, final List<Profile> replyProfiles) {
      // tell local controllers about this cache server
      tellLocalControllers(removeProfile, exchangeProfiles, replyProfiles);
      // for QRM messaging we need cache servers to know about each other
      tellLocalBridgeServers(dm.getCache(), removeProfile, exchangeProfiles, replyProfiles);
    }

    @Override
    public int getDSFID() {
      return CACHE_SERVER_PROFILE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeStringArray(this.groups, out);
      out.writeInt(maxConnections);
      context.getSerializer().invokeToData(initialLoad, out);
      out.writeLong(getLoadPollInterval());
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.groups = DataSerializer.readStringArray(in);
      this.maxConnections = in.readInt();
      this.initialLoad = new ServerLoad();
      context.getDeserializer().invokeFromData(initialLoad, in);
      setLoadPollInterval(in.readLong());
    }

    @Override
    public StringBuilder getToStringHeader() {
      return new StringBuilder("BridgeServerProfile");
    }

    @Override
    public void fillInToString(StringBuilder sb) {
      super.fillInToString(sb);
      if (this.groups != null) {
        sb.append("; groups=" + Arrays.asList(this.groups));
        sb.append("; maxConnections=" + maxConnections);
        sb.append("; initialLoad=" + initialLoad);
        sb.append("; loadPollInterval=" + getLoadPollInterval());
      }
    }
  }
}
