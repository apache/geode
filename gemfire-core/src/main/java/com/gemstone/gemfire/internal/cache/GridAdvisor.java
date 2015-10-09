/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.util.*;
import java.io.*;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.distributed.internal.membership.*;


/**
 * Used to share code with BridgeServerAdvisor and ControllerAdvisor
 * @author darrel
 *
 */
public abstract class GridAdvisor extends DistributionAdvisor {
  
  /** Creates a new instance of GridAdvisor */
  protected GridAdvisor(DistributionAdvisee server) {
    super(server);
  }

  private final Object cacheLock = new Object();
  private volatile List/*<BridgeServerProfile>*/ cachedBridgeServerProfiles;
  private volatile List/*<ControllerProfile>*/ cachedControllerProfiles;
  private volatile Set/*<DistributedMember>*/ cachedBridgeServerAdvise;
  private volatile Set/*<DistributedMember>*/ cachedControllerAdvise;

  private static final Filter CONTROLLER_FILTER = new Filter() {
      public boolean include(Profile profile) {
        return profile instanceof ControllerAdvisor.ControllerProfile;
      }
    };

  private static final Filter BRIDGE_SERVER_FILTER = new Filter() {
      public boolean include(Profile profile) {
        return profile instanceof CacheServerAdvisor.CacheServerProfile;
      }
    };
  
  /**
   * Return an unmodifiable Set<DistributedMember> of the cnx controllers
   * in this system.
   */
  public Set adviseControllers() {
    Set/*<DistributedMember>*/ result = this.cachedControllerAdvise;
    if (result == null) {
      synchronized (this.cacheLock) {
        result = this.cachedControllerAdvise;
        if (result == null) {
          result = Collections.unmodifiableSet(adviseFilter(CONTROLLER_FILTER));
          this.cachedControllerAdvise = result;
        }
      }
    }
    return result;
  }

  /**
   * Return an unmodifiable Set<DistributedMember> of the bridge servers
   * in this system.
   */
  public Set adviseBridgeServers() {
    Set/*<DistributedMember>*/ result = this.cachedBridgeServerAdvise;
    if (result == null) {
      synchronized (this.cacheLock) {
        result = this.cachedBridgeServerAdvise;
        if (result == null) {
          result = Collections.unmodifiableSet(adviseFilter(BRIDGE_SERVER_FILTER));
          this.cachedBridgeServerAdvise = result;
        }
      }
    }
    return result;
  }

  /**
   * Returns an unmodifiable <code>List</code> of the
   * <code>BridgeServerProfile</code>s for all known bridge servers.
   */
  public List/*<BridgeServerProfile>*/ fetchBridgeServers() {
    List/*<BridgeServerProfile>*/ result = null; //this.cachedBridgeServerProfiles;
    if (result == null) {
      synchronized (this.cacheLock) {
//        result = this.cachedBridgeServerProfiles;
        if (result == null) {
          result = fetchProfiles(BRIDGE_SERVER_FILTER);
          this.cachedBridgeServerProfiles = result;
        }
      }
    }
    return result;
  }
  /**
   * Returns an unmodifiable <code>List</code> of the
   * <code>ControllerProfile</code>s for all known cnx controllers.
   */
  public List/*<ControllerProfile>*/ fetchControllers() {
    List/*<ControllerProfile>*/ result = this.cachedControllerProfiles;
    if (result == null) {
      synchronized (this.cacheLock) {
        result = this.cachedControllerProfiles;
        if (result == null) {
          result = fetchProfiles(CONTROLLER_FILTER);
          this.cachedControllerProfiles = result;
        }
      }
    }
    return result;
  }

  public int getBridgeServerCount() {
    List/*<BridgeServerProfile>*/ l = this.cachedBridgeServerProfiles;
    if (l == null) {
      l = fetchProfiles(BRIDGE_SERVER_FILTER);
    }
    return l.size();
  }

  public int getControllerCount() {
    List/*<ControllerProfile>*/ l = this.cachedControllerProfiles;
    if (l == null) {
      l = fetchProfiles(CONTROLLER_FILTER);
    }
    return l.size();
  }

  /**
   * Need ALL others (both normal members and admin members).
   */
  @Override
  public boolean useAdminMembersForDefault() {
    return true;
  }

  /**
   * Just always return true.
   * This method could use getAllOtherMembers but it would cause
   * us to make lots of copies of the member set.
   * The problem with our super.isCurrentMember is it ignores admin members.
   */
  @Override
  protected boolean isCurrentMember(Profile p) {
    return true;
  }

  @Override
  protected void profileCreated(Profile profile) {
    profilesChanged();
  }
  @Override
  protected void profileUpdated(Profile profile) {
    profilesChanged();
  }
  @Override
  protected void profileRemoved(Profile profile) {
    profilesChanged();
  }
  /**
   * Used to drop any cached profile views we have since the master list
   * of profiles changed. The next time someone asks for a view it will
   * be recomputed.
   */
  protected void profilesChanged() {
    if (pollIsInitialized()) {
      // no need to synchronize here since all cached* fields are volatile
      this.cachedBridgeServerProfiles = null;
      this.cachedControllerProfiles = null;
      this.cachedBridgeServerAdvise = null;
      this.cachedControllerAdvise = null;
    }
  }

  /**
   * Tell everyone else who we are and find out who they are.
   */
  public void handshake() {
    if(initializationGate()) {
      //Exchange with any local servers or controllers.
      List<Profile> otherProfiles = new ArrayList<Profile>();
      GridProfile profile = (GridProfile) createProfile();
      profile.tellLocalBridgeServers(false, true, otherProfiles);
      profile.tellLocalControllers(false, true, otherProfiles);
      for(Profile otherProfile : otherProfiles) {
        if(!otherProfile.equals(profile)) {
          this.putProfile(otherProfile);
        }
      }
    }
    profilesChanged();
  }
  
  @Override
  public void close() {
    try {
      new UpdateAttributesProcessor(getAdvisee(),
                                    true/*removeProfile*/).distribute();
      
      //Notify any local bridge servers or controllers
      //that we are closing.
      GridProfile profile = (GridProfile) createProfile();
      profile.tellLocalBridgeServers(true, false, null);
      profile.tellLocalControllers(true, false, null);
      super.close();
    } catch (DistributedSystemDisconnectedException ignore) {
      // we are closing so ignore a shutdown exception.
    }
    profilesChanged();
  }
  
  
  
  @Override
  public Set adviseProfileRemove() {
    //Our set of profiles includes local members. However, the update
    //attributes message doesn't seem to be able to handle being sent to local
    //members
    Set results = super.adviseProfileRemove();
    results.remove(getDistributionManager().getId());
    return results;
  }



  /**
   * Describes profile data common for all Grid resources
   */
  public static abstract class GridProfile extends DistributionAdvisor.Profile {

    private String host;

    /**
     * SQLFabric uses a negative port value when creating a fake profile meant
     * to only gather information about all available locators.
     */
    private int port;

    private ProfileId id;

    /** for internal use, required for DataSerializer.readObject */
    public GridProfile() {
    }

    public GridProfile(InternalDistributedMember memberId, int version) {
      super(memberId, version);
    }

    public GridProfile(GridProfile toCopy) {
      super(toCopy.getDistributedMember(), toCopy.version);
      this.host = toCopy.host;
      this.port = toCopy.port;
      finishInit();
    }

    public final void setHost(String host) {
      this.host = host;
    }

    public final void setPort(int port) {
      this.port = port;
    }

    public final String getHost() {
      return this.host;
    }

    public final int getPort() {
      return this.port;
    }

    @Override
    public final ProfileId getId() {
      if (this.id == null) throw new IllegalStateException("profile id not yet initialized");
      return this.id;
    }

    /**
     * Tell local controllers about the received profile. Also if exchange
     * profiles then add each local controller to reply.
     * 
     * @since 5.7
     */
    protected final void tellLocalControllers(boolean removeProfile,
        boolean exchangeProfiles, final List<Profile> replyProfiles) {
      final List<Locator> locators = Locator.getLocators();
      for (int i = 0; i < locators.size(); i++) {
        InternalLocator l = (InternalLocator)locators.get(i);
        DistributionAdvisee advisee = l.getServerLocatorAdvisee();
        if(advisee != null && advisee.getProfile().equals(this)) {
          continue;
        }
        // negative value for port used by SQLFabric to indicate fake profile
        // meant to only gather remote profiles during profile exchange
        if (this.port > 0) {
          handleDistributionAdvisee(advisee, removeProfile, exchangeProfiles,
              replyProfiles);
        }
        else if (exchangeProfiles && advisee != null) {
          replyProfiles.add(advisee.getProfile());
        }
      }
      final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if (cache != null && !cache.isClosed()) {
        handleDistributionAdvisee(cache.getSqlfAdvisee(), removeProfile, false,
            replyProfiles);
      }
    }

    /**
     * Tell local bridge servers about the received profile. Also if exchange
     * profiles then add each local bridge server to reply.
     * 
     * @since 5.7
     */
    protected final void tellLocalBridgeServers(boolean removeProfile,
        boolean exchangeProfiles, final List<Profile> replyProfiles) {
      final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if (cache != null && !cache.isClosed()) {
        List<?> bridgeServers = cache.getCacheServersAndGatewayReceiver();
        for (int i = 0; i < bridgeServers.size(); i++) {
          CacheServerImpl bsi = (CacheServerImpl)bridgeServers.get(i);
          if (bsi.isRunning()) {
            if(bsi.getProfile().equals(this)) {
              continue;
            }
            // negative value for port used by SQLFabric to indicate fake
            // profile meant to only gather remote profiles during profile
            // exchange
            if (this.port > 0) {
              handleDistributionAdvisee(bsi, removeProfile, exchangeProfiles,
                  replyProfiles);
            }
            else if (exchangeProfiles) {
              replyProfiles.add(bsi.getProfile());
            }
          }
        }
      }
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(this.host, out);
      DataSerializer.writePrimitiveInt(this.port, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.host = DataSerializer.readString(in);
      this.port = DataSerializer.readPrimitiveInt(in);
      finishInit();
    }

    public void finishInit() {
      this.id = new GridProfileId(this);
    }
    
    @Override
    public void fillInToString(StringBuilder sb) {
      super.fillInToString(sb);
      sb.append("; host=" + this.host);
      sb.append("; port=" + this.port);
    }
  }

  /**
   * Basic idea is to base id off of host and port of GridProfile
   */
  public static final class GridProfileId implements ProfileId {

    private final GridProfile gp;

    public GridProfileId(GridProfile gp) {
      this.gp = gp;
    }

    public final InternalDistributedMember getMemberId() {
      return this.gp.getDistributedMember();
    }

    public final String getHost() {
      return this.gp.getHost();
    }

    public final int getPort() {
      return this.gp.getPort();
    }

    @Override
    public String toString() {
      return "GridProfile[host=" + this.gp.getHost() + ",port=" + gp.getPort()
          + ']';
    }

    @Override
    public int hashCode() {
      final String thisHost = this.gp.getHost();
      final int thisPort = this.gp.getPort();
      return thisHost != null ? (thisHost.hashCode() ^ thisPort) : thisPort;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof GridProfileId) {
        final GridProfileId other = (GridProfileId)obj;
        if (this.gp.getPort() == other.gp.getPort()) {
          final String thisHost = this.gp.getHost();
          final String otherHost = other.gp.getHost();
          if (thisHost != null) {
            return thisHost.equals(otherHost);
          }
          else {
            return (otherHost == null);
          }
        }
      }
      return false;
    }
  }
}
