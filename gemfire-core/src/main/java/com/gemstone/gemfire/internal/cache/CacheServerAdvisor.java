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
import java.util.Arrays;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.server.ServerLoad;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.InternalDataSerializer;


/**
 * Used to give advice to a cache server.
 * Cache server currently need to know about controller's
 * @author darrel
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
  protected Profile instantiateProfile(
      InternalDistributedMember memberId, int version) {
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
    public CacheServerProfile() {
    }

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
     * Used to process an incoming cache server profile. Any controller in this
     * vm needs to be told about this incoming new cache server. The reply
     * needs to contain any controller(s) that exist in this vm.
     * 
     * @since 5.7
     */
    @Override
    public void processIncoming(DistributionManager dm, String adviseePath,
        boolean removeProfile, boolean exchangeProfiles,
        final List<Profile> replyProfiles) {
      // tell local controllers about this cache server
      tellLocalControllers(removeProfile, exchangeProfiles, replyProfiles);
      // for QRM messaging we need cache servers to know about each other
      tellLocalBridgeServers(removeProfile, exchangeProfiles, replyProfiles);
    }

    @Override
    public int getDSFID() {
      return CACHE_SERVER_PROFILE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeStringArray(this.groups, out);
      out.writeInt(maxConnections);
      InternalDataSerializer.invokeToData(initialLoad, out);
      out.writeLong(getLoadPollInterval());
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.groups = DataSerializer.readStringArray(in);
      this.maxConnections = in.readInt();
      this.initialLoad = new ServerLoad();
      InternalDataSerializer.invokeFromData(initialLoad, in);
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
