package com.gemstone.gemfire.internal.cache.tier.sockets;

/**
 * Helper class to access the required functions of this package from
 * outside the package.
 * @author Girish Thombare
 */
public class HaHelper
{
  public static boolean checkPrimary(CacheClientProxy proxy)
  {
    System.out.println("proxy " + proxy.getProxyID()+  " : " +  proxy.isPrimary()); 
    return proxy.isPrimary();
    
  }
  
}
