package org.apache.geode.security;

import java.util.Properties;

import org.apache.geode.distributed.DistributedMember;

/**
 * this is used in conjunction with ExpirableSecurityManager. It will create a new set of
 * credentials every time getCredentials are called, and they will always be authenticated
 * and authorized by the ExpirableSecurityManager.
 *
 * make sure reset is called after each test to clean things up.
 */


public class NewCredentialAuthInitialize implements AuthInitialize {
  private static int count;

  @Override
  public Properties getCredentials(Properties securityProps, DistributedMember server,
      boolean isPeer) throws AuthenticationFailedException {
    count++;
    Properties credentials = new Properties();
    credentials.put("security-username", "user" + count);
    credentials.put("security-password", "user" + count);
    return credentials;
  }

  public static String getCurrentUser() {
    return "user" + count;
  }

  public static void reset() {
    count = 0;
  }
}
