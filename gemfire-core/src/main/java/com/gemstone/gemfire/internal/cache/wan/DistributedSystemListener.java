package com.gemstone.gemfire.internal.cache.wan;

/**
 * This interface is for Jayesh's use case for WAN BootStrapping and will not be part of the product release.
 * 
 * @author kbachhav
 *
 */
public interface DistributedSystemListener {

  // remoteDSId is the distributed-system-id of the distributed system that has joined existing sites
  public void addedDistributedSystem(int remoteDsId);
  
  // This is invoked when user explicitly removed the distributed system from the membership
  public void removedDistributedSystem(int remoteDsId);
}
