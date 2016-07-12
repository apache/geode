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
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.CacheCallback;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;

/**
 * Specifies the mechanism to verify credentials for a client or peer.
 * Implementations should register name of the static creation function as the
 * <code>security-peer-authenticator</code> system property with all the
 * locators in the distributed system for peer authentication, and as
 * <code>security-client-authenticator</code> for client authentication. For
 * P2P an object is initialized on the group coordinator for each member during
 * the {@link DistributedSystem#connect(Properties)} call of a new member. For
 * client-server, an object of this class is created for each connection during
 * the client-server handshake.
 * 
 * The static creation function should have the following signature:
 * <code>public static Authenticator [method-name]();</code> i.e. it should be
 * a zero argument function.
 * 
 * @since GemFire 5.5
 *
 * @deprecated since Geode 1.0, use {@link SecurityManager} instead
 */
public interface Authenticator extends CacheCallback {

  /**
   * Initialize the callback for a client/peer. This is invoked when a new
   * connection from a client/peer is created with the host.
   * 
   * @param securityProps
   *                the security properties obtained using a call to
   *                {@link DistributedSystem#getSecurityProperties}
   * @param systemLogger
   *                {@link LogWriter} for system logs
   * @param securityLogger
   *                {@link LogWriter} for security logs
   * 
   * @throws AuthenticationFailedException
   *                 if some exception occurs during the initialization
   */
  void init(Properties securityProps, LogWriter systemLogger,
      LogWriter securityLogger) throws AuthenticationFailedException;

  default void init(Properties securityProps)  throws AuthenticationFailedException{
    init(securityProps, null, null);
  }

  /**
   * Verify the credentials provided in the properties for the client/peer as
   * specified in member ID and returns the principal associated with the
   * client/peer.
   * 
   * @param props
   *                the credentials of the client/peer as a set of property
   *                key/values
   * @param member
   *                the {@link DistributedMember} object of the connecting
   *                client/peer member. NULL when invoked locally on the 
   *                member initiating the authentication request.
   * 
   * @return the principal for the client/peer when authentication succeeded
   * 
   * @throws AuthenticationFailedException
   *                 If the authentication of the client/peer fails.
   */
  Principal authenticate(Properties props, DistributedMember member)
      throws AuthenticationFailedException;

  default Principal authenticate(Properties props) throws AuthenticationFailedException{
    return authenticate(props, null);
  }

}
