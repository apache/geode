package org.apache.geode.security;

import java.io.Serializable;

public class ExpirablePrincipal implements Serializable {
  private final long expiredAfter;
  private final Object principal;

  public ExpirablePrincipal(Object principal, long period) {
    this.principal = principal;
    expiredAfter = System.currentTimeMillis() + period;
  }

  public Object getPrincipal() {
    return principal;
  }

  public boolean expired() {
    return expiredAfter < System.currentTimeMillis();
  }
}
