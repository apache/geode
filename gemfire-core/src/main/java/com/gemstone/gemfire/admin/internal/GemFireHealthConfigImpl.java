/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

// @todo davidw Delegate to a "parent" config for properties that are not overridden.
// This will be made easier with a special <code>HealthConfigAttribute</code> class.
/**
 * The implementation of <code>GemFireHealthConfig</code>
 *
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public class GemFireHealthConfigImpl
  extends CacheHealthConfigImpl
  implements GemFireHealthConfig {

  private static final long serialVersionUID = -6797673296902808018L;

  /** The name of the host to which this configuration applies. */
  private String hostName;

  /** The number of seconds to wait between evaluating the health of
   * GemFire. */
  private int interval = DEFAULT_HEALTH_EVALUATION_INTERVAL;

  ////////////////////////  Constructors  ////////////////////////

  /**
   * Creates a new <code>GemFireHealthConfigImpl</code> that applies
   * to the host with the given name.
   *
   * @param hostName
   *        The name of the host to which this configuration applies.
   *        If <code>null</code>, then this is the "default"
   *        configuration. 
   */
  public GemFireHealthConfigImpl(String hostName) {
    this.hostName = hostName;
  }

  ///////////////////////  Instance Methods  ///////////////////////

  public String getHostName() {
    return this.hostName;
  }

  public void setHealthEvaluationInterval(int interval) {
    this.interval = interval;
  }

  public int getHealthEvaluationInterval() {
    return this.interval;
  }

  @Override
  public String toString() {
    if (this.hostName == null) {
      return LocalizedStrings.GemFireHealthConfigImpl_DEFAULT_GEMFIRE_HEALTH_CONFIGURATION.toLocalizedString();

    } else {
      return LocalizedStrings.GemFireHealthConfigImpl_GEMFIRE_HEALTH_CONFIGURATION_FOR_HOST_0.toLocalizedString(this.hostName);
    }
  }

}
