/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.admin.jmx;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.AdminDistributedSystem;

//import javax.management.MBeanException;
import javax.management.MalformedObjectNameException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * A server component that provides administration-related information
 * about a GemFire distributed system via the Java Management
 * Extension JMX API.  When a JMX <code>Agent</code> is created, it
 * registers an MBean that represents {@link #getObjectName itself}. 
 * Click <A href="doc-files/mbeans-descriptions.html">here</A> for a
 * description of the attributes, operations, and notifications of
 * this and other GemFire JMX MBeans.
 *
 * <P>
 *
 * The GemFire JMX Agent currently supports three JMX "adapters"
 * through which clients can access the GemFire management beans: an
 * "HTTP adapter" that allows a web browser client to view and modify
 * management beans via HTTP or HTTPS, an "RMI adapter" that allows
 * Java programs to access management beans using Remote Method
 * Invocation, and an "SNMP adapter" that allows SNMP to access
 * management beans.  Information about configuring these adapters can
 * be found in {@link AgentConfig}.
 *
 * <P>
 *
 * In most distributed caching architectures, JMX administration
 * agents are run in their own processes.  A stand-alone JMX agent is
 * managed using the <code>agent</code> command line utility:
 *
 * <PRE>
 * $ agent start
 * </PRE>
 *
 * This class allows a GemFire application VM to host a JMX management
 * agent.  Architectures with "co-located" JMX agents reduce the
 * number of overall proceses required.  However, hosting a JMX
 * management agent in the same VM as a GemFire application is not
 * generally recommended because it adds extra burden to an
 * application VM and in the event that the application VM exits the
 * administration information will no longer be available.
 *
 * @see AgentConfig
 * @see AgentFactory
 *
 * @since GemFire 4.0
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/com/gemstone/gemfire/management/package-summary.html">management</a></code> package instead
 */
public interface Agent {

  /** Lookup name for RMIConnector when rmi-registry-enabled is true */
  public static final String JNDI_NAME = "/jmxconnector";

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Returns the configuration object for this JMX Agent.
   */
  public AgentConfig getConfig();

  /**
   * Starts this JMX Agent and its associated adapters.  This method
   * does not {@linkplain #connectToSystem connect} to the distributed
   * system.
   */
  public void start();

  /**
   * Returns the JMX <code>MBeanServer</code> with which GemFire
   * MBeans are registered or <code>null</code> if this
   * <code>Agent</code> is not started.
   */
  public MBeanServer getMBeanServer();

  /**
   * {@linkplain #disconnectFromSystem Disconnects} from the
   * distributed system and stops this JMX Agent and all of its
   * associated adapters.
   */
  public void stop();

  /**
   * Returns the <code>ObjectName</code> of the JMX management bean
   * that represents this agent or <code>null</code> if this
   * <code>Agent</code> has not been started.
   */
  public ObjectName getObjectName();

  /**
   * Returns whether or not this JMX <code>Agent</code> is currently
   * providing information about a distributed system.
   */
  public boolean isConnected();

  /**
   * Connects to the distributed system described by this <code>Agent</code>'s 
   * configuration.
   *
   * @return The object name of the system that the <code>Agent</code>
   *         is now connected to.
   */
  public ObjectName connectToSystem()
    throws AdminException, MalformedObjectNameException;

  /**
   * Returns the <code>AdminDistributedSystem</code> that underlies
   * this JMX <code>Agent</code> or <code>null</code> is this agent is
   * not {@linkplain #isConnected connected}.
   */
  public AdminDistributedSystem getDistributedSystem();

  /**
   * Returns the object name of the JMX MBean that represents the
   * distributed system administered by this <code>Agent</code> or
   * <code>null</code> if this <code>Agent</code> has not {@linkplain
   * #connectToSystem connected} to the distributed system.
   */
  public ObjectName manageDistributedSystem()
    throws MalformedObjectNameException;

  /**
   * Disconnects this agent from the distributed system and
   * unregisters the management beans that provided information about
   * it.  However, this agent's adapters are not stopped and it is
   * possible to reconfigure this <code>Agent</code> to connect to
   * another distributed system.
   */
  public void disconnectFromSystem();

  /**
   * Saves the configuration for this <code>Agent</code> to the file
   * specified by @link AgentConfig#getPropertyFile.
   */
  public void saveProperties();

  /**
   * Returns the <code>LogWriter</code> used for logging information.
   */
  public LogWriter getLogWriter();

}
