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

import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.CacheCallback;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

// TODO Add example usage of this interface and configuration details
/**
 * Specifies the mechanism to obtain credentials for a client or peer. It is
 * mandatory for clients and peers when running in secure mode and an
 * {@link Authenticator} has been configured on the server/locator side
 * respectively. Implementations should register name of the static creation
 * function (that returns an object of the class) as the
 * <i>security-peer-auth-init</i> system property on peers and as the
 * <i>security-client-auth-init</i> system property on clients.
 * 
 * @since GemFire 5.5
 */
public interface AuthInitialize extends CacheCallback {

  /**
   * Initialize the callback for a client/peer. This is invoked when a new
   * connection from a client/peer is created with the host.
   * 
   * @param systemLogger
   *                {@link LogWriter} for system logs
   * @param securityLogger
   *                {@link LogWriter} for security logs
   * 
   * @throws AuthenticationFailedException
   *                 if some exception occurs during the initialization
   *
   *  @deprecated since Geode 1.0, use init()
   */
  public void init(LogWriter systemLogger, LogWriter securityLogger)
      throws AuthenticationFailedException;

  /**
   * @since Geode 1.0. implement this method instead of init with logwriters.
   * Implementation should use log4j instead of these loggers.
   */
  default public void init(){
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    init(cache.getLogger(), cache.getSecurityLogger());
  }
  /**
   * Initialize with the given set of security properties and return the
   * credentials for the peer/client as properties.
   * 
   * This method can modify the given set of properties. For example it may
   * invoke external agents or even interact with the user.
   * 
   * Normally it is expected that implementations will filter out <i>security-*</i>
   * properties that are needed for credentials and return only those.
   * 
   * @param securityProps
   *                the security properties obtained using a call to
   *                {@link DistributedSystem#getSecurityProperties} that will be
   *                used for obtaining the credentials
   * @param server
   *                the {@link DistributedMember} object of the
   *                server/group-coordinator to which connection is being
   *                attempted
   * @param isPeer
   *                true when this is invoked for peer initialization and false
   *                when invoked for client initialization
   * 
   * @throws AuthenticationFailedException
   *                 in case of failure to obtain the credentials
   * 
   * @return the credentials to be used for the given <code>server</code>
   */
  public Properties getCredentials(Properties securityProps,
      DistributedMember server, boolean isPeer)
      throws AuthenticationFailedException;
}
