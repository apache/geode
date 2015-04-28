/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.admin.ConfigurationParameter;

/**
 * Listens to value changes of a 
 * {@link com.gemstone.gemfire.admin.ConfigurationParameter}.  This is for 
 * internal use only to allow a {@link SystemMemberImpl} to keep track of 
 * configuration changes made through 
 * {@link ConfigurationParameterImpl#setValue}.
 *
 * @author    Kirk Lund
 * @since     3.5
 *
 */
public interface ConfigurationParameterListener {
  public void configurationParameterValueChanged(ConfigurationParameter parm);
}

