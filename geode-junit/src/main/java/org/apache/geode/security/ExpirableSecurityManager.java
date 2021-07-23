package org.apache.geode.security;

import java.util.Properties;

import org.apache.geode.examples.SimpleSecurityManager;

public class ExpirableSecurityManager extends SimpleSecurityManager {
  // the period in milli-seconds after which the user authentication will expire
  public static int EXPIRE_AFTER = 1000;
  private boolean expired = false;

  @Override
  public Object authenticate(Properties credentials) throws AuthenticationFailedException {
    Object principal = super.authenticate(credentials);
    // instantly expire
    return new ExpirablePrincipal(principal, EXPIRE_AFTER);
  }

  @Override
  public boolean authorize(Object principal, ResourcePermission permission) {
    ExpirablePrincipal expirablePrincipal = (ExpirablePrincipal) principal;
    if (expirablePrincipal.expired()) {
      expired = true;
      throw new AuthenticationExpiredException("User authorization attributes not found.");
    }
    return super.authorize(expirablePrincipal.getPrincipal(), permission);
  }

  public boolean isExpired() {
    return expired;
  }

  public void reset() {
    EXPIRE_AFTER = 1000;
    expired = false;
  }
}
