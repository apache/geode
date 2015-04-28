/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;

/**
 * Resource which uses a {@link DistributionAdvisor}.
 * @author darrel
 * @since 5.7
 */
public interface DistributionAdvisee {

  /**
   * Returns the underlying <code>DistributionManager</code> used for messaging.
   * @return the underlying <code>DistributionManager</code>
   */
  public DM getDistributionManager();
  
  /**
   * @return the cancellation object for the advisee
   */
  public CancelCriterion getCancelCriterion();
  
  /**
   * Returns the <code>DistributionAdvisor</code> that provides advice for
   * this advisee.
   * @return the <code>DistributionAdvisor</code>
   */
  public DistributionAdvisor getDistributionAdvisor();
  
  /**
   * Returns the <code>Profile</code> representing this member in the
   * <code>DistributionAdvisor</code>.
   * @return the <code>Profile</code> representing this member
   */
  public Profile getProfile();
  
  /**
   * Returns this advisees parent or null if no parent exists.
   * @return parent advisee
   */
  public DistributionAdvisee getParentAdvisee();
  
  /**
   * Returns the underlying <code>InternalDistributedSystem</code> connection.
   * @return the underlying <code>InternalDistributedSystem</code>
   */
  public InternalDistributedSystem getSystem();
  
  /**
   * Returns the simple name of this resource.
   * @return the simple name of this resource
   */
  public String getName();
  
  /**
   * Returns the full path of this resource.
   * @return the full path of this resource.
   */
  public String getFullPath();

  /** 
   * Fill in the <code>Profile</code> with details from the advisee.
   * @param profile the <code>Profile</code> to fill in with details
   */
  public void fillInProfile(Profile profile);

  /**
   * @return the serial number which identifies the static order in which this
   * region was created in relation to other regions or other instances of
   * this region during the life of this JVM.
   */
  public int getSerialNumber();

}
