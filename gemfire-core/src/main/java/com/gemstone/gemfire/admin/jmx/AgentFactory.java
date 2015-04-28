/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.jmx;

//import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.jmx.internal.AgentConfigImpl;
import com.gemstone.gemfire.admin.jmx.internal.AgentImpl;

/**
 * A factory class that creates JMX administration entities.
 *
 * @author David Whitlock
 * @since 4.0
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public class AgentFactory {

  /**
   * Defines a "default" GemFire JMX administration agent
   * configuration.
   */
  public static AgentConfig defineAgent() {
    return new AgentConfigImpl();
  }

  /**
   * Creates an unstarted GemFire JMX administration agent with the
   * given configuration.
   *
   * @see Agent#start
   */
  public static Agent getAgent(AgentConfig config) 
    throws AdminException {
    return new AgentImpl((AgentConfigImpl) config);
  }

}
