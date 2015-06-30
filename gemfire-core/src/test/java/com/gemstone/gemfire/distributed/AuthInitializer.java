/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed;

import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.security.AuthInitialize;
import com.gemstone.gemfire.security.AuthenticationFailedException;

/** a security class used by LocatorTest */
public class AuthInitializer implements AuthInitialize {

  public static AuthInitialize create() {
    return new AuthInitializer();
  }

  public void init(LogWriter systemLogger, LogWriter securityLogger)
      throws AuthenticationFailedException {
  }

  public Properties getCredentials(Properties p, DistributedMember server,
      boolean isPeer) throws AuthenticationFailedException {
    p.put("UserName", "bruce");
    p.put("Password", "bruce");
    return p;
  }

  public void close() {
  }
}