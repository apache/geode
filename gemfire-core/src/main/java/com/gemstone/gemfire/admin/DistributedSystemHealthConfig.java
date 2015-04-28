/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

/**
 * Provides configuration information relating to the health of an
 * entire GemFire distributed system.
 *
 * <P>
 *
 * If any of the following criteria is
 * true, then the distributed system is considered to be in
 * {@link GemFireHealth#OKAY_HEALTH OKAY_HEALTH}.
 *
 * <UL>
 *
 * </UL>
 *
 * If any of the following criteria is true, then the distributed
 * system is considered to be in {@link GemFireHealth#POOR_HEALTH
 * POOR_HEALTH}.
 *
 * <UL>
 *
 * <LI>Too many application members {@linkplain
 * #getMaxDepartedApplications unexpectedly leave} the distributed
 * system.</LI>
 *
 * <LI>Too many application members {@linkplain
 * #getMaxDepartedApplications unexpectedly leave} the distributed
 * system.</LI>
 *
 * </UL>
 *
 * @author David Whitlock
 *
 * @since 3.5
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 * */
public interface DistributedSystemHealthConfig {

  /** The default maximum number of application members that can
   * unexceptedly leave a healthy the distributed system. */
  public static final long DEFAULT_MAX_DEPARTED_APPLICATIONS = 10;

  ///////////////////////  Instance Methods  ///////////////////////

  /**
   * Returns the maximum number of application members that can
   * unexceptedly leave a healthy the distributed system.
   *
   * @see #DEFAULT_MAX_DEPARTED_APPLICATIONS
   */
  public long getMaxDepartedApplications();

  /**
   * Sets the maximum number of application members that can
   * unexceptedly leave a healthy the distributed system.
   *
   * @see #getMaxDepartedApplications
   */
  public void setMaxDepartedApplications(long maxDepartedApplications);
}
