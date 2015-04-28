/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

/**
 * Provides configuration information relating to all of the
 * components of a GemFire distributed system.
 *
 * @author David Whitlock
 *
 * @since 3.5
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 * */
public interface GemFireHealthConfig
  extends MemberHealthConfig, CacheHealthConfig {

  /** The default number of seconds between assessments of the health
   * of the GemFire components. */
  public static final int DEFAULT_HEALTH_EVALUATION_INTERVAL = 30;

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Returns the name of the host to which this configuration
   * applies.  If this is the "default" configuration, then
   * <code>null</code> is returned.
   *
   * @see GemFireHealth#getGemFireHealthConfig
   */
  public String getHostName();

  /**
   * Sets the number of seconds between assessments of the health of
   * the GemFire components.
   */
  public void setHealthEvaluationInterval(int interval);

  /**
   * Returns the number of seconds between assessments of the health of
   * the GemFire components.
   */
  public int getHealthEvaluationInterval();

}
