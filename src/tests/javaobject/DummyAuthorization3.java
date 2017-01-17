/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
