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
package org.apache.geode.management.internal.cli.shell;

import java.util.Set;
import javax.management.ObjectName;
import javax.management.QueryExp;

import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.internal.cli.CommandRequest;

/**
 * The OperationInvoker interface defines a contract for invoking operations on MBeans, accessing an MBean's attributes
 * and remotely executing/processing commands issued in Gfsh on the GemFire Manager.
 * <p/>
 * There could be many different implementations of OperationInvoker based on different protocols like JMX, HTTP
 * and so on.
 * <p/>
 * @since GemFire 7.0
 */
public interface OperationInvoker {

  int CLUSTER_ID_WHEN_NOT_CONNECTED = -1;

  /**
   * Determines whether there is an active, current connection.
   * <p/>
   * @return true if there is an active connection, false otherwise.
   * @see #isReady()
   */
  public boolean isConnected();

  /**
   * Determines whether there is an active, current active connection ready for use.
   * <p/>
   * @return true if this {@linkplain OperationInvoker} is ready for operation, false otherwise.
   * @see #isConnected()
   */
  public boolean isReady();

  /**
   * Read the attribute identified by name from a remote resource identified by name.
   * <p/>
   * @param resourceName name/url of the remote resource from which to fetch the attribute.
   * @param attributeName name of the attribute to be fetched.
   * @return value of the named attribute on the remote resource.
   */
  public Object getAttribute(String resourceName, String attributeName);

  /**
   * Gets the identifier of the GemFire cluster.
   * <p/>
   * @return an integer value indicating the identifier of the GemFire cluster.
   */
  public int getClusterId();

  // TODO continue to add MXBean accessor methods as necessary for GemFire MXBeans used in Gfsh and command classes...
  /**
   * Gets a proxy to the remote DistributedSystem MXBean to access attributes and invoke operations on the distributed
   * system, or the GemFire cluster.
   * <p/>
   * @return a proxy instance of the GemFire Manager's DistributedSystem MXBean.
   * @see org.apache.geode.management.DistributedSystemMXBean
   */
  public DistributedSystemMXBean getDistributedSystemMXBean();

  /**
   * Gets a proxy to an MXBean on a remote MBeanServer.
   * <p/>
   * @param <T> the class type of the remote MXBean.
   * @param objectName the JMX ObjectName uniquely identifying the remote MXBean.
   * @param mbeanInterface the interface of the remote MXBean to proxy for attribute/operation access.
   * @return a proxy to access the specified, remote MXBean.
   * @see javax.management.ObjectName
   */
  public <T> T getMBeanProxy(ObjectName objectName, Class<T> mbeanInterface);

  /**
   * Invoke an operation identified by name on a remote resource identified by name with the given arguments.
   * <p/>
   * @param resourceName name/url (object name) of the remote resource (MBea) on which operation is to be invoked.
   * @param operationName name of the operation to be invoked.
   * @param params an array of arguments for the parameters to be set when the operation is invoked.
   * @param signature an array containing the signature of the operation.
   * @return result of the operation invocation.
   */
  public Object invoke(String resourceName, String operationName, Object[] params, String[] signature);

  /**
   * This method searches the MBean server, based on the OperationsInvoker's JMX-based or other remoting capable
   * MBean server connection, for MBeans matching a specific ObjectName or matching an object name pattern along with
   * satisfying criteria of the query.
   * <p/>
   *
   * @param objectName the ObjectName or pattern for which matching MBeans in the target MBean server will be returned.
   * @param queryExpression the JMX-based query expression used to filter matching MBeans.
   * @return a set of ObjectName's matching MBeans in the MBean server matching the ObjectName and Query expression
   * criteria.
   * @see javax.management.ObjectName
   * @see javax.management.QueryExp
   */
  public Set<ObjectName> queryNames(ObjectName objectName, QueryExp queryExpression);

  /**
   * Processes the requested command.  Sends the command to the GemFire Manager for remote processing (execution).
   * NOTE refactoring return type in favor of covariant return types.
   * <p/>
   * @param command the Command entered and invoked by the user to be processed.
   * @return the result of the command execution.
   * @see org.apache.geode.management.internal.cli.CommandRequest
   */
  public Object processCommand(CommandRequest command);

  /**
   * Stops this {@linkplain OperationInvoker}
   */
  public void stop();

}
