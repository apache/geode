/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.security;

import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.CacheCallback;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;

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
 * @author Neeraj Kumar
 * @since 5.5
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
   */
  public void init(LogWriter systemLogger, LogWriter securityLogger)
      throws AuthenticationFailedException;

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
