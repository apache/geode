/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.model.member;

import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.mgmt.DataBrowser.model.region.GemFireRegion;

/**
 * This class represents a GemFire system member.
 * 
 * @author Hrishi
 **/
public final class GemFireMember {
  private static final GemFireRegion[] EMPTY_REGION_LIST = new GemFireRegion[0];
  public static final short     GEMFIRE_CACHELESS_MEMBER   = 0;
  public static final short     GEMFIRE_PEER               = 1;
  public static final short     GEMFIRE_CACHE_SERVER       = 3;
  public static final short     GEMFIRE_SERVER_LOCATOR     = 4;

  private String                id;
  private String                name;
  private String                host;
  private short                 type;
  private List<CacheServerInfo> cacheServers;
  private List<MemberConfigurationPrms> config;
  private GemFireRegion         root;

  // Make sure that the access for creating new objects is restricted to this
  // package.
  GemFireMember() {
    this.id = null;
    this.name = null;
    this.host = null;
    this.type = -1;
    this.cacheServers = new ArrayList<CacheServerInfo>();
    this.config = new ArrayList<MemberConfigurationPrms>();
    this.root = null;
  }

  void setHost(String hst) {
    this.host = hst;
  }

  void setId(String ident) {
    this.id = ident;
  }

  void setName(String nm) {
    this.name = nm;
  }

  void setType(short tp) {
    this.type = tp;
  }

  void addCacheServer(CacheServerInfo server) {
    this.cacheServers.add(server);
  }

  void resetCacheServerInfo() {
    this.cacheServers.clear();
  }
  
  public MemberConfigurationPrms[] getConfig() {
    return config.toArray(new MemberConfigurationPrms[0]);
  }
  
  public void setConfig(List<MemberConfigurationPrms> cfg) {
    if(cfg != null) {
     config.clear();
     config.addAll(cfg);
    }
  }

  void setRootRegion(GemFireRegion region) {
    root = region;
  }

  /**
   * This method returns member-id of this GemFire member.
   * 
   * @return member-id
   */
  public String getId() {
    return id;
  }

  /**
   * This method returns name of this GemFire member.
   * 
   * @return member-name
   */
  public String getName() {
    return name;
  }
  
  /**
   * This method returns representation name of this GemFire member for view.
   * 
   * @return member-name
   */
  public String getRepresentationName() {
    String repName =name + "(" + getId() +")";
    
    return repName;
  }

  /**
   * This method returns hostname of this GemFire member.
   * 
   * @return hostname
   */
  public String getHost() {
    return host;
  }

  /**
   * This method returns type of this GemFire member.
   * 
   * @return type
   */
  public short getType() {
    return type;
  }

  /**
   * This method returns a list of root regions defined on this GemFire member.
   * 
   * @return list of root regions.
   */
  public GemFireRegion[] getRootRegions() {
    //We get a /Root element, which is meaningless for user. 
    //Hence we filter the same.
    GemFireRegion[] retVal = EMPTY_REGION_LIST;
    if(root != null) {
      retVal = root.getSubRegions();  
    }    
    
    return retVal;
  }
  
  /**
   * This method returns a list of all the regions defined on this GemFire member.
   * 
   * @return list of root regions.
   */
  public GemFireRegion[] getAllRegions() {
    //We get a /Root element, which is meaningless for user. 
    //Hence we filter the same.
    if(this.root != null) {
      List<GemFireRegion> subRegions = root.getSubRegions(true);
      return subRegions.toArray(new GemFireRegion[0]);  
    }    
    return EMPTY_REGION_LIST;
  }

  /**
   * This method returns information regarding the Cache servers started on this
   * member.
   * 
   * @return list of cache servers started.
   */
  public CacheServerInfo[] getCacheServers() {
    return this.cacheServers.toArray(new CacheServerInfo[0]);
  }
  
  @Override
  public boolean equals(Object obj) {
    if(obj == null)
      return false;
    
    if (!(obj instanceof GemFireMember))
      return false;

    GemFireMember mem = (GemFireMember)obj;

    return getId() != null ? getId().equalsIgnoreCase(mem.getId())
        : super.equals(obj);
  }
  
  @Override
  public int hashCode() {
    return getId() != null ? getId().hashCode() : super.hashCode();
  }
  
  public boolean isNotifyBySubscriptionEnabled() {
    for(CacheServerInfo info : this.cacheServers) {
      if(info.isNotifyBySubscription())
       return true; 
    }
    return false;
  }
 
}
