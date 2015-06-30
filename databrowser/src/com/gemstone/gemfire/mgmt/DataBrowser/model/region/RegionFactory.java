/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.model.region;

import java.util.Iterator;
import java.util.Set;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.RegionSubRegionSnapshot;
import com.gemstone.gemfire.admin.SystemMemberCache;
import com.gemstone.gemfire.admin.SystemMemberRegion;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.RegionAttributesData;
import com.gemstone.gemfire.management.RegionMXBean;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * This class provides factory methods to create region model beans from
 * corresponding GemFire region representation.
 * 
 * @author Hrishi
 **/

public class RegionFactory {

  /**
   * Factory method to create region model beans from
   * corresponding GemFire region representation.
   * 
   * @param region GemFire region representation.
   * @return Region model bean.
   */
  public static GemFireRegion getRegion(String memberId,
      SystemMemberCache cache, SystemMemberRegion region) throws AdminException {
    GemFireRegion result = new GemFireRegion();
    
    result.setMemberId(memberId);    
    result.setName(region.getName());
    result.setFullPath(region.getFullPath());
    result.setDataPolicy(region.getDataPolicy().toString());
    result.setScope(region.getScope().toString());

    Set subRegions = region.getSubregionFullPaths();
    Iterator iter = subRegions.iterator();

    while (iter.hasNext()) {
      String path = (String)iter.next();
      SystemMemberRegion temp = cache.getRegion(path);
      GemFireRegion subRegion = getRegion(memberId, cache, temp);
      result.addSubRegion(subRegion);
    }

    return result;
  }
  
  public static GemFireRegion getRegion(String memberId, RegionSubRegionSnapshot snapshot) {
    
    LogUtil.fine("Region Update : "+snapshot);
    
    GemFireRegion result = new GemFireRegion();
    
    result.setMemberId(memberId);    
    result.setName(snapshot.getName());
    result.setFullPath(snapshot.getFullPath());
    //Need to make these available through JMX...
    //result.setDataPolicy(region.getDataPolicy().toString());
    //result.setScope(region.getScope().toString());

    Set subRegions = snapshot.getSubRegionSnapshots();
    Iterator iter = subRegions.iterator();

    while (iter.hasNext()) {
      RegionSubRegionSnapshot reg = (RegionSubRegionSnapshot)iter.next();
      GemFireRegion subRegion = getRegion(memberId, reg);
      result.addSubRegion(subRegion);
    }

    return result;
  }

  public static GemFireRegion getRootRegion(String memberId, DistributedSystemMXBean distributedSystemMXBeanProxy, MBeanServerConnection connServer, String[] regionNames) throws Exception{
    GemFireRegion gfRegion = new GemFireRegion();
    gfRegion.setName("Root");
    gfRegion.setMemberId(memberId);
    for (String region : regionNames) {
      gfRegion.addSubRegion(createGemFireRegion(distributedSystemMXBeanProxy, connServer,memberId, region));
    }
    return gfRegion;
  }
  
  public static GemFireRegion createGemFireRegion(DistributedSystemMXBean distributedSystemMXBeanProxy, MBeanServerConnection connServer, String memberId, String regionName) throws Exception{
      ObjectName regionObjectName = distributedSystemMXBeanProxy.fetchRegionObjectName(memberId, regionName);
      RegionMXBean rbean = JMX.newMXBeanProxy(connServer, regionObjectName, RegionMXBean.class);
      RegionAttributesData ra = rbean.listRegionAttributes();
      GemFireRegion region = new GemFireRegion();
      region.setDataPolicy( ra.getDataPolicy());
      region.setFullPath(rbean.getFullPath());
      region.setName(rbean.getName());
      
      for(String subRegionName : rbean.listSubRegionPaths(true)){
        GemFireRegion subRegion = createGemFireRegion(distributedSystemMXBeanProxy, connServer, memberId,  subRegionName);
        region.addSubRegion(subRegion);
      }
      return region;
  }
}
