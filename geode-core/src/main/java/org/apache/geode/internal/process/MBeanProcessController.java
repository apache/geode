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
package org.apache.geode.internal.process;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Properties;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.sun.tools.attach.AgentInitializationException;
import com.sun.tools.attach.AgentLoadException;
import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;

/**
 * Controls a {@link ControllableProcess} using the Attach API to manipulate
 * MBeans.
 * 
 * @since GemFire 8.0
 */
public final class MBeanProcessController implements ProcessController {

  /** Property name for the JMX local connector address (from sun.management.Agent) */
  private static final String LOCAL_CONNECTOR_ADDRESS_PROP =
      "com.sun.management.jmxremote.localConnectorAddress";

  private final MBeanControllerParameters arguments;
  private final int pid;
  
  protected JMXConnector jmxc;
  protected MBeanServerConnection server;
  
  /**
   * Constructs an instance for controlling a local process.
   * 
   * @param pid process id identifying the process to attach to
   * 
   * @throws IllegalArgumentException if pid is not a positive integer
   */
  public MBeanProcessController(final MBeanControllerParameters arguments, final int pid) {
    if (pid < 1) {
      throw new IllegalArgumentException("Invalid pid '" + pid + "' specified");
    }
    this.pid = pid;
    this.arguments = arguments;
  }
  
  @Override
  public int getProcessId() {
    return this.pid;
  }
  
  @Override
  public String status() throws UnableToControlProcessException, ConnectionFailedException, IOException, MBeanInvocationFailedException {
    return status(this.arguments.getNamePattern(), 
        this.arguments.getPidAttribute(), 
        this.arguments.getStatusMethod(), 
        this.arguments.getAttributes(), 
        this.arguments.getValues());
  }

  @Override
  public void stop() throws UnableToControlProcessException, ConnectionFailedException, IOException, MBeanInvocationFailedException {
    stop(this.arguments.getNamePattern(), 
        this.arguments.getPidAttribute(), 
        this.arguments.getStopMethod(), 
        this.arguments.getAttributes(), 
        this.arguments.getValues());
  }
  
  @Override
  public void checkPidSupport() {
  }
  
  /**
   * Connects to the process and tells it to shut down.
   * 
   * @param namePattern the name pattern of the MBean to use for stopping
   * @param pidAttribute the name of the MBean attribute with the process id to compare against
   * @param stopMethod the name of the MBean operation to invoke
   * @param attributes the names of the MBean attributes to compare with expected values
   * @param values the expected values of the specified MBean attributes
   *
   * @throws ConnectionFailedException if there was a failure to connect to the local JMX connector in the process
   * @throws IOException if a communication problem occurred when talking to the MBean server
   * @throws MBeanInvocationFailedException if failed to invoke stop on the MBean for any reason
   */
  private void stop(final ObjectName namePattern, final String pidAttribute, final String stopMethod, final String[] attributes, final Object[] values) 
      throws ConnectionFailedException, IOException, MBeanInvocationFailedException {
    invokeOperationOnTargetMBean(namePattern, pidAttribute, stopMethod, attributes, values);
  }
  
  /**
   * Connects to the process and acquires its status.
   * 
   * @param namePattern the name pattern of the MBean to use for stopping
   * @param pidAttribute the name of the MBean attribute with the process id to compare against
   * @param statusMethod the name of the MBean operation to invoke
   * @param attributes the names of the MBean attributes to compare with expected values
   * @param values the expected values of the specified MBean attributes
   * 
   * @return string describing the status of the process
   *
   * @throws ConnectionFailedException if there was a failure to connect to the local JMX connector in the process
   * @throws IOException if a communication problem occurred when talking to the MBean server
   * @throws MBeanInvocationFailedException if failed to invoke stop on the MBean for any reason
   */
  private String status(final ObjectName namePattern, final String pidAttribute, final String statusMethod, final String[] attributes, final Object[] values)
      throws ConnectionFailedException, IOException, MBeanInvocationFailedException {
    return invokeOperationOnTargetMBean(namePattern, pidAttribute, statusMethod, attributes, values).toString();
  }
  
  /**
   * Connects to the process and use its MBean to stop it.
   * 
   * @param namePattern the name pattern of the MBean to use for stopping
   * @param pidAttribute the name of the MBean attribute with the process id to compare against
   * @param methodName the name of the MBean operation to invoke
   * @param attributes the names of the MBean attributes to compare with expected values
   * @param values the expected values of the specified MBean attributes
   *
   * @throws ConnectionFailedException if there was a failure to connect to the local JMX connector in the process
   * @throws IOException if a communication problem occurred when talking to the MBean server
   * @throws MBeanInvocationFailedException if failed to invoke stop on the MBean for any reason
   */
  private Object invokeOperationOnTargetMBean(final ObjectName namePattern, final String pidAttribute, final String methodName, final String[] attributes, final Object[] values)
      throws ConnectionFailedException, IOException, MBeanInvocationFailedException {
    ObjectName objectName = namePattern;
    connect();
    try {
      final QueryExp constraint = buildQueryExp(pidAttribute, attributes, values);
      final Set<ObjectName> mbeanNames = this.server.queryNames(namePattern, constraint);
      
      if (mbeanNames.isEmpty()) {
        throw new MBeanInvocationFailedException("Failed to find mbean matching '" 
            + namePattern + "' with attribute '" + pidAttribute + "' of value '" + this.pid + "'");
      }
      if (mbeanNames.size() > 1) {
        throw new MBeanInvocationFailedException("Found more than one mbean matching '" 
            + namePattern + "' with attribute '" + pidAttribute + "' of value '" + this.pid + "'");
      }
      
      objectName = mbeanNames.iterator().next();
      return invoke(objectName, methodName);
    } catch (InstanceNotFoundException e) {
      throw new MBeanInvocationFailedException("Failed to invoke " + methodName + " on " + objectName, e);
    } catch (MBeanException e) {
      throw new MBeanInvocationFailedException("Failed to invoke " + methodName + " on " + objectName, e);
    } catch (ReflectionException e) {
      throw new MBeanInvocationFailedException("Failed to invoke " + methodName + " on " + objectName, e);
    } finally {
      disconnect();
    }
  }
  
  /**
   * Connects to the JMX agent in the local process.
   * 
   * @throws ConnectionFailedException if there was a failure to connect to the local JMX connector in the process
   * @throws IOException if the JDK management agent cannot be found and loaded
   */
  private void connect() throws ConnectionFailedException, IOException {
    try {
      final JMXServiceURL jmxUrl = getJMXServiceURL();
      this.jmxc = JMXConnectorFactory.connect(jmxUrl);
      this.server = this.jmxc.getMBeanServerConnection();
    } catch (AttachNotSupportedException e) {
      throw new ConnectionFailedException("Failed to connect to process '" + this.pid + "'", e);
    }
  }
  
  /**
   * Disconnects from the JMX agent in the local process.
   */
  private void disconnect() {
    this.server = null;
    if (this.jmxc != null) {
      try {
        this.jmxc.close();
      } catch (IOException e) {
        // ignore
      }
    }
    this.jmxc = null;
  }
  
  /**
   * Ensures that the other process identifies itself by the same pid used by 
   * this stopper to connect to that process. NOT USED EXCEPT IN TEST.
   * 
   * @return true if the pid matches
   * 
   * @throws IllegalStateException if the other process identifies itself by a different pid
   * @throws IOException if a communication problem occurred when accessing the MBeanServerConnection
   * @throws PidUnavailableException if parsing the pid from the RuntimeMXBean name fails
   */
  boolean checkPidMatches() throws IllegalStateException, IOException, PidUnavailableException {
    final RuntimeMXBean proxy = ManagementFactory.newPlatformMXBeanProxy(
        this.server, ManagementFactory.RUNTIME_MXBEAN_NAME, RuntimeMXBean.class);
    final int remotePid = ProcessUtils.identifyPid(proxy.getName());
    if (remotePid != this.pid) {
      throw new IllegalStateException("Process has different pid '" + remotePid + "' than expected pid '" + this.pid + "'");
    } else {
      return true;
    }
  }

  /**
   * Uses the Attach API to connect to the local process and ensures that it has
   * loaded the JMX management agent. The JMXServiceURL identifying the local
   * connector address for the JMX agent in the process is returned.
   *  
   * @return the address of the JMX API connector server for connecting to the local process
   * 
   * @throws AttachNotSupportedException if unable to use the Attach API to connect to the process
   * @throws IOException if the JDK management agent cannot be found and loaded
   */
  private JMXServiceURL getJMXServiceURL() throws AttachNotSupportedException, IOException {
    String connectorAddress = null;
    final VirtualMachine vm = VirtualMachine.attach(String.valueOf(this.pid));
    try {
      Properties agentProps = vm.getAgentProperties();
      connectorAddress = (String) agentProps.get(LOCAL_CONNECTOR_ADDRESS_PROP);
      
      if (connectorAddress == null) {
        // need to load the management-agent and get the address
        
        final String javaHome = vm.getSystemProperties().getProperty("java.home");
        
        // assume java.home is JDK and look in JRE for agent
        String managementAgentPath = javaHome + File.separator + "jre" 
            + File.separator + "lib" + File.separator + "management-agent.jar";
        File managementAgent = new File(managementAgentPath);
        if (!managementAgent.exists()) {
          // assume java.home is JRE and look in lib for agent
          managementAgentPath = javaHome + File.separator +  "lib" 
              + File.separator + "management-agent.jar";
          managementAgent = new File(managementAgentPath);
          if (!managementAgent.exists()) {
            throw new IOException("JDK management agent not found");
          }
        }
  
        // attempt to load the management agent
        managementAgentPath = managementAgent.getCanonicalPath();
        try {
          vm.loadAgent(managementAgentPath, "com.sun.management.jmxremote");
        } catch (AgentLoadException e) {
          IOException ioe = new IOException(e.getMessage());
          ioe.initCause(e);
          throw ioe;
        } catch (AgentInitializationException e) {
          IOException ioe = new IOException(e.getMessage());
          ioe.initCause(e);
          throw ioe;
        }
  
        // get the connector address
        agentProps = vm.getAgentProperties();
        connectorAddress = (String) agentProps.get(LOCAL_CONNECTOR_ADDRESS_PROP);
      }
    } finally {
      vm.detach();
    }
    
    if (connectorAddress == null) {
      // should never reach here
      throw new IOException("Failed to find address to attach to process");
    }
    
    return new JMXServiceURL(connectorAddress);
  }
  
  /**
   * Builds the QueryExp used to identify the target MBean.
   * 
   * @param pidAttribute the name of the MBean attribute with the process id to compare against
   * @param attributes the names of additional MBean attributes to compare with expected values
   * @param values the expected values of the specified MBean attributes
   *
   * @return the main QueryExp for matching the target MBean
   */
  private QueryExp buildQueryExp(final String pidAttribute, final String[] attributes, final Object[] values) {
    final QueryExp optionalAttributes = buildOptionalQueryExp(attributes, values);
    QueryExp constraint;
    if (optionalAttributes != null) {
      constraint = Query.and(optionalAttributes, Query.eq(
        Query.attr(pidAttribute),
        Query.value(this.pid)));
    } else {
      constraint = Query.eq(
          Query.attr(pidAttribute),
          Query.value(this.pid));
    }
    return constraint;
  }
  
  /**
   * Builds an optional QueryExp to aid in matching the correct MBean using 
   * additional attributes with the specified values. Returns null if no
   * attributes and values were specified during construction.
   * 
   * @param attributes the names of additional MBean attributes to compare with expected values
   * @param values the expected values of the specified MBean attributes
   *
   * @return optional QueryExp to aid in matching the correct MBean 
   */
  private QueryExp buildOptionalQueryExp(final String[] attributes, final Object[] values) {
    QueryExp queryExp = null;
    for (int i = 0; i < attributes.length; i++) {
      if (values[i] instanceof Boolean) {
        if (queryExp == null) { 
          queryExp = Query.eq(
              Query.attr(attributes[i]), 
              Query.value(((Boolean) values[i])));
        } else {
          queryExp = Query.and(queryExp, 
              Query.eq(Query.attr(attributes[i]), 
              Query.value(((Boolean) values[i]))));
        }
      } else if (values[i] instanceof Number) {
        if (queryExp == null) { 
          queryExp = Query.eq(
              Query.attr(attributes[i]), 
              Query.value((Number)values[i]));
        } else {
          queryExp = Query.and(queryExp, 
              Query.eq(Query.attr(attributes[i]), 
              Query.value((Number)values[i])));
        }
      } else if (values[i] instanceof String) {
        if (queryExp == null) { 
          queryExp = Query.eq(
              Query.attr(attributes[i]), 
              Query.value((String)values[i]));
        } else {
          queryExp = Query.and(queryExp, 
              Query.eq(Query.attr(attributes[i]), 
              Query.value((String)values[i])));
        }
      }
    }
    return queryExp;
  }

  /**
   * Invokes an operation on the specified MBean.
   * 
   * @param objectName identifies the MBean
   * @param method the name of the operation method invoke
   * 
   * @return the result of invoking the operation on the MBean specified or null
   * 
   * @throws InstanceNotFoundException if the specified MBean is not registered in the MBean server
   * @throws IOException if a communication problem occurred when talking to the MBean server
   * @throws MBeanException if the MBean operation throws an exception
   * @throws ReflectionException if the MBean does not have the specified operation
   */
  private Object invoke(final ObjectName objectName, final String method) 
      throws InstanceNotFoundException, IOException, MBeanException, ReflectionException  {
    return this.server.invoke(objectName, method, new Object[]{}, new String[]{});
  }
}
