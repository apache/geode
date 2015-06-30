/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed;

import java.net.InetAddress;
import java.security.Principal;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;

/** A security class used by LocatorTest */
public class MyAuthenticator implements Authenticator {

  public static Authenticator create() {
    return new MyAuthenticator();
  }

  public MyAuthenticator() {
  }

  public void init(Properties systemProps, LogWriter systemLogger,
      LogWriter securityLogger) throws AuthenticationFailedException {
  }

  public Principal authenticate(Properties props, DistributedMember member, InetAddress addr)
      throws AuthenticationFailedException {
    return MyPrincipal.create();
  }

  public Principal authenticate(Properties props, DistributedMember member)
      throws AuthenticationFailedException {
    return MyPrincipal.create();
  }

  public void close() {
  }

}