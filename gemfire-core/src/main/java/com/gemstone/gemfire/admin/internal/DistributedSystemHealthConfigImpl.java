/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.admin.*;

/**
 * The implementation of <code>DistributedSystemHealthConfig</code>.
 * Note that because it never leaves the management VM, it is not
 * <code>Serializable</code> and is not part of the {@link
 * GemFireHealthConfigImpl} class hierarchy.
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public class DistributedSystemHealthConfigImpl
  implements DistributedSystemHealthConfig {

  /** The maximum number of application members that can
   * unexceptedly leave a healthy the distributed system. */
  private long maxDepartedApplications =
    DEFAULT_MAX_DEPARTED_APPLICATIONS;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>DistributedSystemHealthConfigImpl</code> with
   * the default configuration.
   */
  protected DistributedSystemHealthConfigImpl() {

  }

  /////////////////////  Instance Methods  /////////////////////

  public long getMaxDepartedApplications() {
    return this.maxDepartedApplications;
  }

  public void setMaxDepartedApplications(long maxDepartedApplications)
  {
    this.maxDepartedApplications = maxDepartedApplications;
  }
}
