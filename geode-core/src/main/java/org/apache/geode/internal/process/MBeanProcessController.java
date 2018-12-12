/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.process;

import static org.apache.commons.lang.Validate.isTrue;
import static org.apache.commons.lang.Validate.notNull;

import java.io.IOException;
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

import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;

/**
 * Controls a {@link ControllableProcess} using the Attach API to manipulate MBeans.
 *
 * @since GemFire 8.0
 */
class MBeanProcessController implements ProcessController {

  /** Property name for the JMX local connector address (from sun.management.Agent) */
  private static final String PROPERTY_LOCAL_CONNECTOR_ADDRESS =
      "com.sun.management.jmxremote.localConnectorAddress";

  private static final Object[] PARAMS = {};
  private static final String[] SIGNATURE = {};

  private final MBeanControllerParameters arguments;
  private final int pid;

  private JMXConnector jmxc;
  private MBeanServerConnection server;

  /**
   * Constructs an instance for controlling a local process.
   *
   * @param pid process id identifying the process to attach to
   *
   * @throws IllegalArgumentException if pid is not a positive integer
   */
  MBeanProcessController(final MBeanControllerParameters arguments, final int pid) {
    notNull(arguments, "Invalid arguments '" + arguments + "' specified");
    isTrue(pid > 0, "Invalid pid '" + pid + "' specified");

    this.pid = pid;
    this.arguments = arguments;
  }

  @Override
  public int getProcessId() {
    return pid;
  }

  @Override
  public String status() throws UnableToControlProcessException, ConnectionFailedException,
      IOException, MBeanInvocationFailedException {
    return status(arguments.getNamePattern(), arguments.getPidAttribute(),
        arguments.getStatusMethod(), arguments.getAttributes(), arguments.getValues());
  }

  @Override
  public void stop() throws UnableToControlProcessException, ConnectionFailedException, IOException,
      MBeanInvocationFailedException {
    stop(arguments.getNamePattern(), arguments.getPidAttribute(), arguments.getStopMethod(),
        arguments.getAttributes(), arguments.getValues());
  }

  @Override
  public void checkPidSupport() {
    // nothing
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
   * @throws ConnectionFailedException if there was a failure to connect to the local JMX connector
   *         in the process
   * @throws IOException if a communication problem occurred when talking to the MBean server
   * @throws MBeanInvocationFailedException if failed to invoke stop on the MBean for any reason
   */
  private void stop(final ObjectName namePattern, final String pidAttribute,
      final String stopMethod, final String[] attributes, final Object[] values)
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
   * @throws ConnectionFailedException if there was a failure to connect to the local JMX connector
   *         in the process
   * @throws IOException if a communication problem occurred when talking to the MBean server
   * @throws MBeanInvocationFailedException if failed to invoke stop on the MBean for any reason
   */
  private String status(final ObjectName namePattern, final String pidAttribute,
      final String statusMethod, final String[] attributes, final Object[] values)
      throws ConnectionFailedException, IOException, MBeanInvocationFailedException {
    return invokeOperationOnTargetMBean(namePattern, pidAttribute, statusMethod, attributes, values)
        .toString();
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
   * @throws ConnectionFailedException if there was a failure to connect to the local JMX connector
   *         in the process
   * @throws IOException if a communication problem occurred when talking to the MBean server
   * @throws MBeanInvocationFailedException if failed to invoke stop on the MBean for any reason
   */
  private Object invokeOperationOnTargetMBean(final ObjectName namePattern,
      final String pidAttribute, final String methodName, final String[] attributes,
      final Object[] values)
      throws ConnectionFailedException, IOException, MBeanInvocationFailedException {
    ObjectName objectName = namePattern;
    connect();
    try {
      QueryExp constraint = buildQueryExp(pidAttribute, attributes, values);
      Set<ObjectName> mbeanNames = server.queryNames(namePattern, constraint);

      if (mbeanNames.isEmpty()) {
        throw new MBeanInvocationFailedException("Failed to find mbean matching '" + namePattern
            + "' with attribute '" + pidAttribute + "' of value '" + pid + "'");
      }
      if (mbeanNames.size() > 1) {
        throw new MBeanInvocationFailedException("Found more than one mbean matching '"
            + namePattern + "' with attribute '" + pidAttribute + "' of value '" + pid + "'");
      }

      objectName = mbeanNames.iterator().next();
      return invoke(objectName, methodName);
    } catch (InstanceNotFoundException | MBeanException | ReflectionException e) {
      throw new MBeanInvocationFailedException(
          "Failed to invoke " + methodName + " on " + objectName, e);
    } finally {
      disconnect();
    }
  }

  /**
   * Connects to the JMX agent in the local process.
   *
   * @throws ConnectionFailedException if there was a failure to connect to the local JMX connector
   *         in the process
   * @throws IOException if the JDK management agent cannot be found and loaded
   */
  private void connect() throws ConnectionFailedException, IOException {
    try {
      JMXServiceURL jmxUrl = getJMXServiceURL();
      jmxc = JMXConnectorFactory.connect(jmxUrl);
      server = jmxc.getMBeanServerConnection();
    } catch (AttachNotSupportedException e) {
      throw new ConnectionFailedException("Failed to connect to process '" + pid + "'", e);
    }
  }

  /**
   * Disconnects from the JMX agent in the local process.
   */
  private void disconnect() {
    server = null;
    if (jmxc != null) {
      try {
        jmxc.close();
      } catch (IOException ignored) {
        // ignore
      }
    }
    jmxc = null;
  }

  /**
   * Uses the Attach API to connect to the local process and ensures that it has loaded the JMX
   * management agent. The JMXServiceURL identifying the local connector address for the JMX agent
   * in the process is returned.
   *
   * @return the address of the JMX API connector server for connecting to the local process
   *
   * @throws AttachNotSupportedException if unable to use the Attach API to connect to the process
   * @throws IOException if the JDK management agent cannot be found and loaded
   */
  private JMXServiceURL getJMXServiceURL() throws AttachNotSupportedException, IOException {
    String connectorAddress;
    VirtualMachine vm = VirtualMachine.attach(String.valueOf(pid));
    try {
      Properties agentProps = vm.getAgentProperties();
      connectorAddress = agentProps.getProperty(PROPERTY_LOCAL_CONNECTOR_ADDRESS);

      if (connectorAddress == null) {
        // make sure the local management agent is started and this will ensure
        // localConnectorAddress is present in the agent properties.
        vm.startLocalManagementAgent();
        // get the connector address
        agentProps = vm.getAgentProperties();
        connectorAddress = agentProps.getProperty(PROPERTY_LOCAL_CONNECTOR_ADDRESS);
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
  private QueryExp buildQueryExp(final String pidAttribute, final String[] attributes,
      final Object[] values) {
    QueryExp optionalAttributes = buildOptionalQueryExp(attributes, values);
    QueryExp constraint;
    if (optionalAttributes != null) {
      constraint =
          Query.and(optionalAttributes, Query.eq(Query.attr(pidAttribute), Query.value(pid)));
    } else {
      constraint = Query.eq(Query.attr(pidAttribute), Query.value(pid));
    }
    return constraint;
  }

  /**
   * Builds an optional QueryExp to aid in matching the correct MBean using additional attributes
   * with the specified values. Returns null if no attributes and values were specified during
   * construction.
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
          queryExp = Query.eq(Query.attr(attributes[i]), Query.value((Boolean) values[i]));
        } else {
          queryExp = Query.and(queryExp,
              Query.eq(Query.attr(attributes[i]), Query.value((Boolean) values[i])));
        }
      } else if (values[i] instanceof Number) {
        if (queryExp == null) {
          queryExp = Query.eq(Query.attr(attributes[i]), Query.value((Number) values[i]));
        } else {
          queryExp = Query.and(queryExp,
              Query.eq(Query.attr(attributes[i]), Query.value((Number) values[i])));
        }
      } else if (values[i] instanceof String) {
        if (queryExp == null) {
          queryExp = Query.eq(Query.attr(attributes[i]), Query.value((String) values[i]));
        } else {
          queryExp = Query.and(queryExp,
              Query.eq(Query.attr(attributes[i]), Query.value((String) values[i])));
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
      throws InstanceNotFoundException, IOException, MBeanException, ReflectionException {
    return server.invoke(objectName, method, PARAMS, SIGNATURE);
  }
}
