/*=========================================================================
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.model.member;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.GemFireMemberStatus;
import com.gemstone.gemfire.admin.RegionSubRegionSnapshot;
import com.gemstone.gemfire.admin.SystemMember;
import com.gemstone.gemfire.admin.SystemMemberCache;
import com.gemstone.gemfire.admin.SystemMemberCacheServer;
import com.gemstone.gemfire.admin.SystemMemberRegion;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.ClientConfiguration;
import com.gemstone.gemfire.mgmt.DataBrowser.model.region.GemFireRegion;
import com.gemstone.gemfire.mgmt.DataBrowser.model.region.RegionFactory;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.EndPoint;

/**
 * This class provides factory methods to create member model beans from
 * corresponding GemFire member representation.
 *
 * @author Hrishi
 **/
public class MemberFactory {

  /**
   * Factory method to create member model beans from corresponding GemFire
   * member representation.
   *
   * @param member
   *          GemFire member representation.
   * @return Member model bean.
   */
  public static GemFireMember createGemFireMember(SystemMember member)
      throws AdminException {
    GemFireMember result = new GemFireMember();
    updateGemFireMember(result, member);
    return result;
  }

  public static void updateGemFireMember(GemFireMember member,
      SystemMember update) throws AdminException {
    member.resetCacheServerInfo();
// /   member.resetRegionInfo();

    member.setId(update.getId());
    member.setName(update.getName());
    member.setHost(update.getHost());

    if (update.hasCache()) {
      SystemMemberCache cache = update.getCache();
      SystemMemberCacheServer[] cacheServers = cache.getCacheServers();

      short type = (cacheServers.length > 0) ? GemFireMember.GEMFIRE_CACHE_SERVER
          : GemFireMember.GEMFIRE_PEER;
      member.setType(type);

      for (int i = 0; i < cacheServers.length; i++) {
        CacheServerInfo info = new CacheServerInfo();
        info.setBindAddress(cacheServers[i].getHostnameForClients());
        info.setPort(cacheServers[i].getPort());
        member.addCacheServer(info);
        LogUtil.info("Adding CacheServer :" + info);
      }

      Set regions = cache.getRootRegionNames();
      Iterator iterator = regions.iterator();
      while (iterator.hasNext()) {
        String regionName = (String) iterator.next();
        SystemMemberRegion region = cache.getRegion(regionName);
        // TODO MGH - what is this for?
        GemFireRegion reg = RegionFactory.getRegion(member.getId(), cache, region);
  //      member.addRegion(reg);
      }
    } else {
      member.setType(GemFireMember.GEMFIRE_CACHELESS_MEMBER);
    }
  }
  
  public static GemFireMember createGemFireMember(ClientConfiguration conf) {
    if(conf.getCacheServers().size() > 0) {
      GemFireMember member = new GemFireMember();
      member.setType(GemFireMember.GEMFIRE_CACHE_SERVER);
      member.setName("GemFire Cache Server");
      member.setId("default-id");
      member.setHost("host");
      
      for(EndPoint endpt : conf.getCacheServers()) {
        CacheServerInfo info = new CacheServerInfo();
        info.setBindAddress(endpt.getHostName());
        info.setPort(endpt.getPort()); 
        member.addCacheServer(info);      
      }
      
      return member;      
    } 
    
    GemFireMember member = new GemFireMember();
    member.setType(GemFireMember.GEMFIRE_SERVER_LOCATOR);
    member.setName("GemFire Server Locator");
    member.setId("default-id");
    member.setHost("host");
    
    for(EndPoint endpt : conf.getLocators()) {
      CacheServerInfo info = new CacheServerInfo();
      info.setBindAddress(endpt.getHostName());
      info.setPort(endpt.getPort());       
      member.addCacheServer(info);
    }
    
    return member;
  }  
  
  public static GemFireMember createGemFireMember(String memberId, List<MemberConfigurationPrms> config, List<CacheServerInfo> cs_info, GemFireMemberStatus status, RegionSubRegionSnapshot snapshot) {
    GemFireMember member = new GemFireMember();
    member.setType(GemFireMember.GEMFIRE_CACHE_SERVER);
    member.setHost(status.getHostAddress().getCanonicalHostName());
    member.setName(status.getMemberName());
    member.setId(memberId);
    member.setConfig(config);
    
    if((cs_info != null) && (!cs_info.isEmpty())) {
     for(CacheServerInfo info : cs_info)
      member.addCacheServer(info);      
    
    } else {
      CacheServerInfo info = new CacheServerInfo();
      info.setBindAddress(status.getBindAddress());
      info.setPort(status.getServerPort()); 
      info.setNotifyBySubscription(false); //TODO : There is no way to fetch this information from GemFireMemberStatus.
      member.addCacheServer(info); 
    }
    
    if(snapshot != null) {
      GemFireRegion root = RegionFactory.getRegion(memberId, snapshot);
      member.setRootRegion(root);
    }    
    
    return member;
  }
  
  public static GemFireMember createGemFireMemberMbean(String memberId, List<MemberConfigurationPrms> config, List<CacheServerInfo> cs_info, MemberStatus status, GemFireRegion root) {
    GemFireMember member = new GemFireMember();
    member.setType(GemFireMember.GEMFIRE_CACHE_SERVER);
    member.setHost(status.getHostAddress().getCanonicalHostName());
    member.setName(status.getMemberName());
    member.setId(memberId);
    member.setConfig(config);
    
    if((cs_info != null) && (!cs_info.isEmpty())) {
       for(CacheServerInfo info : cs_info) {
         LogUtil.fine("Adding cache server info: " + info);
         member.addCacheServer(info);
       }
    } else {
      CacheServerInfo info = new CacheServerInfo();
      info.setBindAddress(status.getBindAddress());
      info.setPort(status.getServerPort()); 
      info.setNotifyBySubscription(true); 
      member.addCacheServer(info); 
    }
  
    member.setRootRegion(root);
    
    return member;
  }
  

}
