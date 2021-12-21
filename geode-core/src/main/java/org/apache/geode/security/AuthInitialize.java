/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.security;

import java.util.Properties;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.CacheCallback;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;

/**
 * Specifies the mechanism to obtain credentials for a client or peer. It is mandatory for clients
 * and peers when running in secure mode and a {@link SecurityManager} has been configured on the
 * server/locator side respectively. Implementations should register name of the static creation
 * function (that returns an object of the class) as the <i>security-peer-auth-init</i> system
 * property on peers and as the <i>security-client-auth-init</i> system property on clients.
 *
 * @since GemFire 5.5
 */
public interface AuthInitialize extends CacheCallback {

  String SECURITY_USERNAME = "security-username";
  String SECURITY_PASSWORD = "security-password";

  /**
   * Initialize the callback for a client/peer. This is invoked when a new connection from a
   * client/peer is created with the host.
   *
   * For future implementations, do not use these loggers, use log4j logger directly.
   *
   * @param systemLogger {@link LogWriter} for system logs
   * @param securityLogger {@link LogWriter} for security logs
   *
   * @throws AuthenticationFailedException if some exception occurs during the initialization
   *
   */
  default void init(LogWriter systemLogger, LogWriter securityLogger)
      throws AuthenticationFailedException {}

  /**
   *
   * @since Geode 1.0.
   * @deprecated in Geode 1.5. Never called by the product. Use {@link #init(LogWriter systemLogger,
   *             LogWriter securityLogger)}
   */
  default void init() {}

  /**
   * Initialize with the given set of security properties and return the credentials for the
   * peer/client as properties.
   *
   * This method can modify the given set of properties. For example it may invoke external agents
   * or even interact with the user.
   *
   * Normally it is expected that implementations will filter out <i>security-*</i> properties that
   * are needed for credentials and return only those.
   *
   * @param securityProps the security properties obtained using a call to
   *        {@link DistributedSystem#getSecurityProperties} that will be used for obtaining the
   *        credentials
   * @param server the {@link DistributedMember} object of the server/group-coordinator to which
   *        connection is being attempted
   * @param isPeer true when this is invoked for peer initialization and false when invoked for
   *        client initialization
   *
   * @throws AuthenticationFailedException in case of failure to obtain the credentials
   *
   * @return the credentials to be used for the given <code>server</code>
   *         It needs to contain "security-username" and "security-password" if you use
   *         username/password combination as credentials
   *         When using Integrated security, all members, peer/client will use the same credentials.
   *         but we still need to use these params to support the old authenticator
   */
  Properties getCredentials(Properties securityProps, DistributedMember server, boolean isPeer)
      throws AuthenticationFailedException;

  /**
   *
   * @return the credentials to be used. It needs to contain "security-username" and
   *         "security-password"
   * @deprecated in Geode 1.3. Never called by the product. Use {@link #getCredentials(Properties
   *             securityProps, DistributedMember server, boolean isPeer)}
   */
  default Properties getCredentials(Properties securityProps) {
    return getCredentials(securityProps, null, true);
  }
}
