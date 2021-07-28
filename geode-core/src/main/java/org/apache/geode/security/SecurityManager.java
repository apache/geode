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

import org.apache.geode.distributed.DistributedSystem;

/**
 * User implementation of a authentication/authorization logic for Integrated Security. The
 * implementation will guard client/server, JMX, Pulse, GFSH commands
 *
 * @since Geode 1.0
 */
public interface SecurityManager {
  /**
   * property name of the username passed in the Properties in authenticate method
   */
  String USER_NAME = "security-username";
  /**
   * property name of the password passed in the Properties in authenticate method
   */
  String PASSWORD = "security-password";
  /**
   * property name of the token passed in the Properties in authenticate method
   */
  String TOKEN = "security-token";

  /**
   * Initialize the SecurityManager. This is invoked when a cache is created
   *
   * @param securityProps the security properties obtained using a call to
   *        {@link DistributedSystem#getSecurityProperties}
   * @throws AuthenticationFailedException if some exception occurs during the initialization
   */
  default void init(Properties securityProps) {}

  /**
   * Verify the credentials provided in the properties
   *
   * Your security manager needs to validate credentials coming from all communication channels.
   * If you use AuthInitialize to generate your client/peer credentials, then the input of this
   * method is the output of your AuthInitialize.getCredentials method. But remember that this
   * method will also need to validate credentials coming from gfsh/jmx/rest client, the framework
   * is putting the username/password under security-username and security-password keys in the
   * property, so your securityManager implementation needs to validate these kind of properties
   * as well.
   *
   * if a channel supports token-based-authentication, the token will be passed to the
   * security manager in the property with the key "security-token".
   *
   * @param credentials it contains the security-username, security-password or security-token,
   *        as keys of the properties, also the properties generated by your AuthInitialize
   *        interface
   * @return a serializable principal object
   */
  Object authenticate(Properties credentials) throws AuthenticationFailedException;

  /**
   * Authorize the ResourcePermission for a given Principal
   *
   * @param principal The principal that's requesting the permission
   * @param permission The permission requested
   * @return true if authorized, false if not
   *
   * @throw AuthenticationExpiredException
   */
  default boolean authorize(Object principal, ResourcePermission permission)
      throws AuthenticationExpiredException {
    return true;
  }

  /**
   * Close any resources used by the SecurityManager, called when a cache is closed.
   */
  default void close() {}
}
