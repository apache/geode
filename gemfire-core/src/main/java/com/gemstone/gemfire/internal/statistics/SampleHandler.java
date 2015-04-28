/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

import java.util.List;

/**
 * Defines the operations required to handle statistics samples and receive
 * notifications of ResourceTypes and ResourceInstances.
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public interface SampleHandler {

 /**
  * Notification that a statistics sample has occurred.
  * <p/>
  * The timeStamp is an arbitrary nanoseconds time stamp only used for
  * archiving to stats files. The initial time in system milliseconds and the
  * first NanoTimer timeStamp are both written to the archive file. Thereafter
  * only the NanoTimer timeStamp is written to the archive file for each
  * sample. The delta in nanos can be determined by comparing any nano 
  * timeStamp to the first nano timeStamp written to the archive file. Adding 
  * this delta to the recorded initial time in milliseconds provides the 
  * actual (non-arbitrary) time for each sample.
  * 
  * @param nanosTimeStamp an arbitrary nanoseconds time stamp for this sample
  * @param resourceInstances the statistics resource instances captured in
  * this sample
  */
  public void sampled(long nanosTimeStamp, List<ResourceInstance> resourceInstances);
  
  /**
   * Notification that a new statistics {@link ResourceType} has been created.
   * 
   * @param resourceType the new statistics ResourceType that was created
   */
  public void allocatedResourceType(ResourceType resourceType);
  
  /**
   * Notification that a new statistics {@link ResourceInstance} has been
   * created.
   * 
   * @param resourceInstance the new statistics ResourceInstance that was 
   * created
   */
  public void allocatedResourceInstance(ResourceInstance resourceInstance);
  
  /**
   * Notification that an existing statistics {@link ResourceInstance} has been
   * destroyed.
   * 
   * @param resourceInstance the existing statistics ResourceInstance that was 
   * destroyed
   */
  public void destroyedResourceInstance(ResourceInstance resourceInstance);
}
