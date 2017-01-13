/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.operations.GetOperationContext;
import org.apache.geode.cache.operations.OperationContext;
import org.apache.geode.cache.operations.PutOperationContext;
import org.apache.geode.cache.operations.RegionCreateOperationContext;
import org.apache.geode.cache.operations.OperationContext.OperationCode;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.security.AccessControl;
import org.apache.geode.security.NotAuthorizedException;

/**
 * A dummy implementation of the <code>AccessControl</code> interface that
 * allows authorization depending on the format of the <code>Principal</code>
 * string.
 * 
 * 
 * @since 6.5
 */
public class DummyAuthorization3 implements AccessControl {

  private String operationKeyIdx;

  private DistributedMember remoteDistributedMember;

  private LogWriter logger;
  private boolean m_closed;

  
  public DummyAuthorization3() {
    operationKeyIdx = "null";
    m_closed = false;
  }

  public static AccessControl create() {
    return new DummyAuthorization3();
  }

  public void init(Principal principal, 
                   DistributedMember remoteMember,
                   Cache cache) throws NotAuthorizedException {

    this.remoteDistributedMember = remoteMember;
    this.logger = cache.getLogger();
    
    if (principal != null) {
      String name = principal.getName().toLowerCase();
      if (name != null) {
        if(name.startsWith("user"))
        {
          String idx = name.substring(4);
          if(idx != null)
          {
            try
            {
              int keyIdx = Integer.valueOf(idx);
              operationKeyIdx = idx;
            }
            catch(Exception ex)
            {
              
            }
          }          
        }
      }
    }
    
  }

  public boolean authorizeOperation(String regionName, OperationContext context) {

    if(m_closed == true)
      return false;
    OperationCode opCode = context.getOperationCode();
    this.logger.fine("Invoked authorize operation for [" + opCode
        + "] in region [" + regionName + "] for client: " + remoteDistributedMember);
    this.logger.fine("Invoked authorize operation for user id = " + operationKeyIdx); 
    if (operationKeyIdx.equals("4"))
    {
      if (context  instanceof GetOperationContext) {
        this.logger.fine("4 Invoked authorize operation for get ");
        GetOperationContext goc = (GetOperationContext)context;
        String key = (String)goc.getKey();
        this.logger.fine("4 Invoked authorize operation for get key = " + key);
        if (key.indexOf(operationKeyIdx) >=0)
          return true;
        else
          return false;
      }
      else if(context  instanceof PutOperationContext)
      {
        this.logger.fine("4 Invoked authorize operation for put ");
        PutOperationContext goc = (PutOperationContext)context;
        String key = (String)goc.getKey();
        this.logger.fine("4 Invoked authorize operation for put key = " + key);
        if (key.indexOf(operationKeyIdx) >=0)
          return true;
        else
          return false;
      }
    }
    
    if (context  instanceof GetOperationContext) {
      this.logger.fine("Invoked authorize operation for get ");
    }
    else if(context  instanceof PutOperationContext)
    {
      this.logger.fine("Invoked authorize operation for put ");
    }
    else if (context instanceof RegionCreateOperationContext)
    {
      this.logger.fine("Invoked authorize operation for region create operation context ");
    }
    return true; 
  }

  public void close() {
    m_closed =true;
  }

}
