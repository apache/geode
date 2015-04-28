/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.internal.security.AuthorizeRequest;
import com.gemstone.gemfire.internal.security.AuthorizeRequestPP;
import com.gemstone.gemfire.security.NotAuthorizedException;

public class ClientUserAuths
{
 // private AtomicLong counter = new AtomicLong(1);
  private Random uniqueIdGenerator = null;
  private int m_seed;
  private long m_firstId;

  private ConcurrentHashMap<Long, UserAuthAttributes> uniqueIdVsUserAuth = new ConcurrentHashMap<Long, UserAuthAttributes>();
  private ConcurrentHashMap<String, UserAuthAttributes> cqNameVsUserAuth = new ConcurrentHashMap<String, UserAuthAttributes>();

  public long putUserAuth(UserAuthAttributes userAuthAttr)
  {
    //TODO:hitesh should we do random here
    //long newId = counter.getAndIncrement();
    long newId = getNextID();
    uniqueIdVsUserAuth.put(newId, userAuthAttr);
    return newId;
  }
  
  public ClientUserAuths(int clientProxyHashcode)
  {
    m_seed = clientProxyHashcode;
    uniqueIdGenerator = new Random(m_seed + System.currentTimeMillis());
    m_firstId = uniqueIdGenerator.nextLong();
  }
  
  synchronized private long getNextID()
  {
    long uniqueId = uniqueIdGenerator.nextLong();
    if (uniqueId == m_firstId)
    { 
      uniqueIdGenerator = new Random(m_seed + System.currentTimeMillis());
      m_firstId = uniqueIdGenerator.nextLong();
      //now every user need to reauthenticate as we are short of Ids..
      //though possibility of this is rare.
      uniqueIdVsUserAuth.clear();
      return m_firstId;
    }
    return uniqueId;
  }
  
  public UserAuthAttributes getUserAuthAttributes(long userId)
  {
    return uniqueIdVsUserAuth.get(userId);
  }
  
  public UserAuthAttributes getUserAuthAttributes(String cqName)
  {
    //Long uniqueId = cqNameVsUserAuth.get(cqName);
    //return uniqueIdVsUserAuth.get(uniqueId);
    return cqNameVsUserAuth.get(cqName);
  }
  
  public void setUserAuthAttributesForCq(String cqName, long uniqueId, boolean isDurable)
  {
    UserAuthAttributes uaa = this.uniqueIdVsUserAuth.get(uniqueId);
    
    if (uaa != null)
    {      
      if (!isDurable)
        this.cqNameVsUserAuth.put(cqName, uaa);
      else 
      {
        UserAuthAttributes oldUaa = this.cqNameVsUserAuth.put(cqName, uaa);
        if(oldUaa != null)
        {
          if(oldUaa != uaa)//clean earlier one
          {
            this.cleanUserAuth(oldUaa);
            //add durable(increment)
            uaa.setDurable();
          }
          else
          {            
            //if looks extra call from client
          }
        }
        else
        {
          uaa.setDurable();
        }
      }
    }
    else
    {
      //TODO:throw not authorized exception? will this ever happen??
      throw new NotAuthorizedException("User is not authorized for CQ");
    }
  }
  
  public void removeUserAuthAttributesForCq(String cqName, boolean isDurable)
  {
    UserAuthAttributes uaa = this.cqNameVsUserAuth.remove(cqName);
    if(uaa != null && isDurable)
      uaa.unsetDurable();        
  }
  
  public boolean removeUserId(long userId, boolean keepAlive) {
    UserAuthAttributes uaa = uniqueIdVsUserAuth.get(userId);
    if (uaa != null && !(uaa.isDurable() && keepAlive)) {
      uaa = uniqueIdVsUserAuth.remove(userId);
      if (uaa != null) {
        cleanUserAuth(uaa);
        return true;
      }
    }
    return false;
  }
  
  public void cleanUserAuth(UserAuthAttributes userAuth)
  {
    if (userAuth != null)
    {
      AuthorizeRequest authReq = userAuth.getAuthzRequest();
      try {
        if (authReq != null) {
          authReq.close();
          authReq = null;
        }
        }
        catch (Exception ex) {
          //TODO:hitesh
          /*if (securityLogger.warningEnabled()) {
            securityLogger.warning(
              LocalizedStrings.ServerConnection_0_AN_EXCEPTION_WAS_THROWN_WHILE_CLOSING_CLIENT_AUTHORIZATION_CALLBACK_1,
              new Object[] {"", ex});
          }*/
        }
        try {
          AuthorizeRequestPP postAuthzReq = userAuth.getPostAuthzRequest(); 
          if (postAuthzReq != null) {
            postAuthzReq.close();
            postAuthzReq = null;
          }
        }
        catch (Exception ex) {
          //TODO:hitesh
          /*if (securityLogger.warningEnabled()) {
            securityLogger.warning(
              LocalizedStrings.ServerConnection_0_AN_EXCEPTION_WAS_THROWN_WHILE_CLOSING_CLIENT_POSTPROCESS_AUTHORIZATION_CALLBACK_1,
              new Object[] {"", ex});
          }*/
        }
    }
    
  }
  public void cleanup(boolean fromCacheClientProxy)
  {
    for (UserAuthAttributes  userAuth : this.uniqueIdVsUserAuth.values()) {
      //isDurable is checked for multiuser in CQ 
      if (!fromCacheClientProxy && !userAuth.isDurable()) {//from serverConnection class
        cleanUserAuth(userAuth);
      }
      else if (fromCacheClientProxy && userAuth.isDurable()) {//from cacheclientProxy class
        cleanUserAuth(userAuth);
      }
    }       
  }
  
  public void fillPreviousCQAuth(ClientUserAuths previousClientUserAuths)
  {
     for (Iterator<Map.Entry<String, UserAuthAttributes>> iter = previousClientUserAuths.cqNameVsUserAuth.entrySet().iterator(); iter.hasNext(); ) {
       Map.Entry<String, UserAuthAttributes> ent = iter.next();
       String cqName = ent.getKey();
       UserAuthAttributes prevUaa = ent.getValue();
       UserAuthAttributes newUaa = this.cqNameVsUserAuth.putIfAbsent(cqName, prevUaa);
       
       if(newUaa != null)
       {
         previousClientUserAuths.cleanUserAuth(prevUaa);
       }
    }     
  }
}
