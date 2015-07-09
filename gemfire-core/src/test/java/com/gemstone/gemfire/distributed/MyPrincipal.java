package com.gemstone.gemfire.distributed;

/** a security class used by LocatorTest */
public class MyPrincipal implements java.security.Principal {

  public static MyPrincipal create() {
    return new MyPrincipal();
  }
  
  public String getName() {
    return "Bruce";
  }
}