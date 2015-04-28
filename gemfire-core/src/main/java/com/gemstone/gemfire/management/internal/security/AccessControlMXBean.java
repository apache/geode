package com.gemstone.gemfire.management.internal.security;

public interface AccessControlMXBean {

  public boolean authorize(String role);
  
}
