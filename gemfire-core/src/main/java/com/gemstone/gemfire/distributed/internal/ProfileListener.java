/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;

/**
 * Callback for changes to profiles in a DistributionAdvisor. A ProfileListener
 * can be registered with a DistributionAdvisor.
 * 
 * These methods are called while the monitor is held on the DistributionAdvisor.
 * @author dsmith
 *
 */
public interface ProfileListener {
  /**
   * Method is invoked after
   * a new profile is created/added to profiles.
   * @param profile the created profile
   */
  void profileCreated(Profile profile);
  
  /**
   * Method is invoked after
   * a profile is updated in profiles.
   * @param profile the updated profile
   */
  void profileUpdated(Profile profile);

  /**
   * Method is invoked after a profile is removed from profiles.
   * 
   * @param profile
   *          the removed profile
   * @param destroyed
   *          indicated that the profile member was destroyed, rather than
   *          closed (used for persistence)
   */
  void profileRemoved(Profile profile, boolean destroyed);

}
