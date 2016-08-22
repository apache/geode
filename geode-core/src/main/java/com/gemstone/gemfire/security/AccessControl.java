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

package com.gemstone.gemfire.security;

import java.security.Principal;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheCallback;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * Specifies the interface to authorize operations at the cache or region level
 * for clients or servers. Implementations should register name of the static
 * creation function as the <code>security-client-accessor</code> system
 * property with all the servers uniformly in the distributed system for client
 * authorization. When the <code>security-client-accessor-pp</code> property
 * is set then the callback mentioned is invoked after the operation completes
 * successfully and when sending notifications.
 * 
 * When the registration has been done for a client/peer then an object of this
 * class is created for each connection from the client/peer and the
 * <code>authorizeOperation</code> method invoked before/after each operation.
 * 
 * @since GemFire 5.5
 *
 * @deprecated since Geode 1.0, use {@link SecurityManager} instead
 */
public interface AccessControl extends CacheCallback {

  /**
   * Initialize the callback for a client/peer having the given principal.
   * 
   * This is invoked when a new connection from a client/peer is created with
   * the host. The callback is expected to store authentication information of
   * the given principal for the different regions for maximum efficiency when
   * invoking <code>authorizeOperation</code> in each operation.
   * 
   * @param principal
   *                the principal associated with the authenticated client or
   *                peer; a null principal implies an unauthenticated client
   *                which should be handled properly by implementations
   * @param remoteMember
   *                the {@link DistributedMember} object for the remote
   *                authenticated client or peer
   * @param cache
   *                reference to the cache object
   * 
   * @throws NotAuthorizedException
   *                 if some exception condition happens during the
   *                 initialization; in such a case all subsequent client
   *                 operations on that connection will throw
   *                 <code>NotAuthorizedException</code>
   */
  void init(Principal principal, DistributedMember remoteMember,
      Cache cache) throws NotAuthorizedException;

  default void init(Principal principal, DistributedMember remoteMember) throws NotAuthorizedException {
    init(principal, remoteMember, null);
  }

  default void init(Principal principal) throws NotAuthorizedException {
    init(principal, null, null);
  }

  /**
   * Check if the given operation is allowed for the cache/region.
   * 
   * This method is invoked in each cache and region level operation. It is,
   * therefore, expected that as far as possible relevant information has been
   * cached in the <code>init</code> call made when the connection was
   * established so that this call is as quick as possible.
   * 
   * @param regionName
   *                When null then it indicates a cache-level operation (i.e.
   *                one of {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#REGION_DESTROY} or
   *                {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#QUERY}, else the name of the region
   *                for the operation.
   * @param context
   *                When invoked before the operation then the data required by
   *                the operation. When invoked as a post-process filter then it
   *                contains the result of the operation. The data in the
   *                context can be possibly modified by the method.
   * 
   * @return true if the operation is authorized and false otherwise
   * 
   */
  boolean authorizeOperation(String regionName, OperationContext context);

}
