/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

import javax.management.openmbean.CompositeData;

public interface GemFireXDMemberMBean {

  public static final String OBJECT_NAME = "GemFireXD:group=DEFAULT,type=Member";

  public boolean getDataStore();

  public CompositeData getNetworkServerClientConnectionStats();

}
