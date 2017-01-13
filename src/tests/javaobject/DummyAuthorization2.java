/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*=========================================================================
* This implementation is provided on an "AS IS" BASIS,  WITHOUT WARRANTIES
* OR CONDITIONS OF ANY KIND, either express or implied."
*==========================================================================
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
public class DummyAuthorization2 implements AccessControl {

  private String operationKeyIdx;

  private DistributedMember remoteDistributedMember;

  private LogWriter logger;

  
  public DummyAuthorization2() {
    operationKeyIdx = "null";
  }

  public static AccessControl create() {
    return new DummyAuthorization2();
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

    OperationCode opCode = context.getOperationCode();
    this.logger.fine("Invoked authorize operation for [" + opCode
        + "] in region [" + regionName + "] for client: " + remoteDistributedMember);
    
    if (context  instanceof GetOperationContext) {
      GetOperationContext goc = (GetOperationContext)context;
      if(goc != null)
      {
        String key = (String)goc.getKey();
        return (key.indexOf(operationKeyIdx) > 0);
      }
    }
    else if(context  instanceof PutOperationContext)
    {
      PutOperationContext poc = (PutOperationContext)context;
      if(poc != null)
      {
        String key = (String)poc.getKey();
        return (key.indexOf(operationKeyIdx) > 0);
      }
    }
    return false; 
  }

  public void close() {

  }

}
