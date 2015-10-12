/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.management.internal.cli.functions;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.CacheServerImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;



/**
 * 
 * @author ajayp
 * @since 8.0
 */

public class ContunuousQueryFunction implements Function, InternalEntity {
  public static final String ID = ContunuousQueryFunction.class.getName();
  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {            
    try {
      String clientID = (String) context.getArguments();      
      GemFireCacheImpl cache = (GemFireCacheImpl)CacheFactory.getAnyInstance();      
      if (cache.getCacheServers().size() > 0) {       
        CacheServerImpl server = (CacheServerImpl)cache.getCacheServers().iterator().next();        
        if(server != null){          
          AcceptorImpl  acceptorImpl  = server.getAcceptor(); 
          if(acceptorImpl != null){          
            CacheClientNotifier cacheClientNotifier = acceptorImpl.getCacheClientNotifier();
            if(cacheClientNotifier != null){          
              Collection<CacheClientProxy> cacheClientProxySet = cacheClientNotifier.getClientProxies();
              ClientInfo clientInfo = null ;
              boolean foundClientinCCP = false;
              Iterator<CacheClientProxy> it = cacheClientProxySet.iterator();
              while(it.hasNext()){
                     
                  CacheClientProxy ccp =  it.next();                                
                  if(ccp != null){
                    String clientIdFromProxy = ccp.getProxyID().getDSMembership();
                    if(clientIdFromProxy != null && clientIdFromProxy.equals(clientID)){
                      foundClientinCCP = true;
                      String durableId = ccp.getProxyID().getDurableId();
                      boolean isPrimary = ccp.isPrimary();
                      clientInfo = new ClientInfo((durableId != null && durableId.length() > 0 ? "Yes" : "No"), 
                                                  (isPrimary == true ? cache.getDistributedSystem().getDistributedMember().getId() : ""), 
                                                  (isPrimary == false ? cache.getDistributedSystem().getDistributedMember().getId() : "") );  
                      break;
                      
                    }
                  }
                }       
                
               //try getting from server connections
                if(foundClientinCCP == false){
                  ServerConnection[] serverConnections  = acceptorImpl.getAllServerConnectionList();
                  
                  for (ServerConnection conn : serverConnections){
                    ClientProxyMembershipID cliIdFrmProxy = conn.getProxyID();

                    if (clientID.equals(cliIdFrmProxy.getDSMembership() )) {
                      String durableId = cliIdFrmProxy.getDurableId();                                 
                      clientInfo = new ClientInfo((durableId != null && durableId.length() > 0 ? "Yes" : "No"), "N.A." ,  "N.A." );                    
                    }  
                  
                  }
               }
               context.getResultSender().lastResult(clientInfo);            
            }
          }           
        }      
      }
    }catch (Exception e) {
      context.getResultSender().lastResult(
          "Exception in ContunuousQueryFunction =" + e.getMessage());
    }
    context.getResultSender().lastResult(null);
    
  }

  @Override
  public String getId() {
    return ContunuousQueryFunction.ID;

  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }

  public class ClientInfo implements Serializable {
    private static final long serialVersionUID = 1L;
    public String isDurable;
    public String primaryServer;
    public String secondaryServer;

    

    public ClientInfo(String IsClientDurable, String primaryServerId, String secondaryServerId) {
      isDurable = IsClientDurable;
      primaryServer = primaryServerId;
      secondaryServer = secondaryServerId;
    }       
    
    @Override
    public String toString() {
      return "ClientInfo [isDurable=" + isDurable + ", primaryServer="
          + primaryServer + ", secondaryServer=" + secondaryServer + "]";
    }
    
    
  }

}
