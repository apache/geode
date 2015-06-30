/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection.internal;

import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.ClientConfiguration;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GFMemberDiscovery;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GemFireConnectionListener;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GemFireMemberListener;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.CacheServerInfo;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.GemFireMember;
import com.gemstone.gemfire.mgmt.DataBrowser.model.member.MemberFactory;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * @author hgadre
 *
 */
public class DummyMemberDiscoveryImpl implements GFMemberDiscovery {
  
  private GemFireMember member;
  private Pool pool;
  
  public DummyMemberDiscoveryImpl(ClientConfiguration conf) {
    member = MemberFactory.createGemFireMember(conf);
    preparePoolConnection();
  }
  
  private void preparePoolConnection() {
    if(this.pool != null) {
      throw new IllegalStateException();
    }
    
    PoolFactory factory = PoolManager.createFactory();
    
    if(this.member.getType() == GemFireMember.GEMFIRE_CACHE_SERVER) {
      for(int i = 0 ; i < this.member.getCacheServers().length ; i++) {
        CacheServerInfo info = this.member.getCacheServers()[i];
        factory.addServer(info.getBindAddress(), info.getPort());
        LogUtil.fine("Adding Cache Server :"+info);
      }
      
    } else {
      for(int i = 0 ; i < this.member.getCacheServers().length ; i++) {
        CacheServerInfo info = this.member.getCacheServers()[i];
        factory.addLocator(info.getBindAddress(), info.getPort());
      }
    }
    
    this.pool = factory.create("GFE_DATABROWSER_POOL");    
  }
  
  

  public void addGemFireMemberListener(GemFireMemberListener listener) {

  }

  public void close() {
  }

  public GemFireMember getMember(String id) {
    return member;
  }

  public GemFireMember[] getMembers() {
    return new GemFireMember[] {member};
  }

  public QueryService getQueryService(GemFireMember mbr) {
    // TODO - MGH: we will need to find the pool for this member ?  
    if(this.pool == null) {
      throw new IllegalStateException("pool should not be null");
    }  
    
    return this.pool.getQueryService();
  }

  public void removeGemFireMemberListener(GemFireMemberListener listener) {
  }
  
  
  public void addConnectionNotificationListener(GemFireConnectionListener listener) {
  }
  
  public void removeConnectionNotificationListener(GemFireConnectionListener listener) {
  }
  
  public String getGemFireSystemVersion() {
    return "UNKNOWN VERSION";
  }
  
  public long getRefreshInterval() {
    return -1;
  }
  
  public void setRefreshInterval(long time) {
    // Do nothing.    
  }

}
