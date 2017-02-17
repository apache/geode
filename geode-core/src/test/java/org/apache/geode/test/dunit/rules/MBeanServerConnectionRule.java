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

package org.apache.geode.test.dunit.rules;

import static org.junit.Assert.assertEquals;

import org.apache.geode.management.internal.security.AccessControlMXBean;
import org.apache.geode.test.junit.rules.DescribedExternalResource;
import org.awaitility.Awaitility;
import org.junit.runner.Description;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * This rules handles connection to the MBean Server. If used with {@link ConnectionConfiguration},
 * you will need to construct the rule with a port number, then the rule will call connect for you
 * before running your test.
 *
 * If constructed with no port number, you can connect to any port in your test at anytime, and the
 * rule will handle the closing of the connection for you.
 */
public class MBeanServerConnectionRule extends DescribedExternalResource {

  private int jmxServerPort = -1;
  private JMXConnector jmxConnector;
  private MBeanServerConnection con;

  public MBeanServerConnectionRule() {}

  /**
   * Rule constructor
   *
   * @param port The JMX server port to connect to
   */
  public MBeanServerConnectionRule(int port) {
    this.jmxServerPort = port;
  }

  /**
   * Retrieve a new proxy MBean
   *
   * @return A new proxy MBean of the same type with which the class was constructed
   */
  public <T> T getProxyMBean(Class<T> proxyClass, String beanQueryName)
      throws MalformedObjectNameException, IOException {
    ObjectName name = null;
    QueryExp query = null;

    if (proxyClass != null) {
      query = Query.isInstanceOf(Query.value(proxyClass.getName()));
    }

    if (beanQueryName != null) {
      name = ObjectName.getInstance(beanQueryName);
    }

    Set<ObjectInstance> beans = con.queryMBeans(name, query);
    assertEquals("failed to find only one instance of type " + proxyClass.getName() + " with name "
        + beanQueryName, 1, beans.size());

    return JMX.newMXBeanProxy(con, ((ObjectInstance) beans.toArray()[0]).getObjectName(),
        proxyClass);
  }

  public AccessControlMXBean getAccessControlMBean() throws Exception {
    return JMX.newMXBeanProxy(con, new ObjectName("GemFire:service=AccessControl,type=Distributed"),
        AccessControlMXBean.class);
  }

  /**
   * Retrieve a new proxy MBean
   *
   * @return A new proxy MBean of the same type with which the class was constructed
   */
  public <T> T getProxyMBean(Class<T> proxyClass) throws MalformedObjectNameException, IOException {
    return getProxyMBean(proxyClass, null);
  }

  public <T> T getProxyMBean(String beanQueryName)
      throws MalformedObjectNameException, IOException {
    return getProxyMBean(null, beanQueryName);
  }

  public MBeanServerConnection getMBeanServerConnection() throws IOException {
    return con;
  }

  @Override
  protected void before(Description description) throws Throwable {
    // do not auto connect if port is not set
    if (jmxServerPort < 0)
      return;

    // do not auto connect if no ConnectionConfiguration is defined.
    ConnectionConfiguration config = description.getAnnotation(ConnectionConfiguration.class);
    if (config == null)
      return;

    Map<String, String[]> env = new HashMap<>();
    String user = config.user();
    String password = config.password();
    env.put(JMXConnector.CREDENTIALS, new String[] {user, password});
    connect(null, jmxServerPort, env);
  }

  public void connect(int jmxPort) throws Exception {
    connect(null, jmxPort, null);
  }

  public void connect(int jmxPort, Map<String, ?> environment) throws Exception {
    connect(null, jmxPort, environment);
  }

  public void connect(String jmxServer, int jmxPort) throws Exception {
    connect(jmxServer, jmxPort, null);
  }

  public void connect(String jmxServer, int jmxPort, final Map<String, ?> environment)
      throws Exception {
    if (jmxServer == null) {
      jmxServer = "";
    }

    // ServiceUrl: service:jmx:rmi:///jndi/rmi://<TARGET_MACHINE>:<RMI_REGISTRY_PORT>/jmxrmi
    JMXServiceURL url =
        new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + jmxServer + ":" + jmxPort + "/jmxrmi");

    // same as GfshShellConnectorRule: if we connect before the RMI server is ready, we get "Failed
    // to retrieve RMIServer stub: javax.naming.CommunicationException [Root exception is
    // java.rmi.NoSuchObjectException: no such object in table]" Exception
    // Have to implement a wait mechanism here. We can use Awaitility here
    Awaitility.await().atMost(2, TimeUnit.MINUTES).pollDelay(2, TimeUnit.SECONDS).until(() -> {
      Map<String, ?> env = new HashMap<>();
      if (environment != null) {
        env = new HashMap<>(environment);
      }
      try {
        jmxConnector = JMXConnectorFactory.connect(url, env);
      } catch (Exception e) {
        if (e.getMessage().contains("no such object in table")) {
          // keep waiting
          return false;
        }
        throw e;
      }
      return true;
    });
    con = jmxConnector.getMBeanServerConnection();
  }

  /**
   * Override to tear down your specific external resource.
   */
  @Override
  protected void after(Description description) throws Throwable {
    disconnect();
  }

  public void disconnect() throws Exception {
    if (jmxConnector != null) {
      jmxConnector.close();
      con = null;
      jmxConnector = null;
    }
  }

}
