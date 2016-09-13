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
package org.apache.geode.management.internal.security;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

import org.junit.runner.Description;

import org.apache.geode.test.junit.rules.DescribedExternalResource;

/**
 * Class which eases the creation of MBeans for security testing. When combined with {@link JMXConnectionConfiguration}
 * it allows for the creation of per-test connections with different user/password combinations.
 */
public class MBeanServerConnectionRule extends DescribedExternalResource {

  private final int jmxServerPort;
  private JMXConnector jmxConnector;
  private MBeanServerConnection con;

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
  public <T> T getProxyMBean(Class<T> proxyClass, String beanQueryName) throws MalformedObjectNameException, IOException {
    ObjectName name = null;
    QueryExp query = null;

    if (proxyClass != null) {
      query = Query.isInstanceOf(Query.value(proxyClass.getName()));
    }

    if (beanQueryName != null) {
      name = ObjectName.getInstance(beanQueryName);
    }

    Set<ObjectInstance> beans = con.queryMBeans(name, query);
    assertEquals("failed to find only one instance of type " + proxyClass.getName() + " with name " + beanQueryName, 1, beans.size());

    return JMX.newMXBeanProxy(con, ((ObjectInstance) beans.toArray()[0]).getObjectName(), proxyClass);
  }

  public AccessControlMXBean getAccessControlMBean() throws Exception{
    return JMX.newMXBeanProxy(con, new ObjectName("GemFire:service=AccessControl,type=Distributed"), AccessControlMXBean.class);
  }

  /**
   * Retrieve a new proxy MBean
   *
   * @return A new proxy MBean of the same type with which the class was constructed
   */
  public <T> T getProxyMBean(Class<T> proxyClass) throws MalformedObjectNameException, IOException {
    return getProxyMBean(proxyClass, null);
  }

  public <T> T getProxyMBean(String beanQueryName) throws MalformedObjectNameException, IOException {
    return getProxyMBean(null, beanQueryName);
  }

  public MBeanServerConnection getMBeanServerConnection() throws IOException {
    return con;
  }

  protected void before(Description description) throws Throwable {
    JMXConnectionConfiguration config = description.getAnnotation(JMXConnectionConfiguration.class);
    Map<String, String[]> env = new HashMap<>();
    if (config != null) {
      String user = config.user();
      String password = config.password();
      env.put(JMXConnector.CREDENTIALS, new String[] { user, password });

      JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:" + jmxServerPort + "/jmxrmi");
      jmxConnector = JMXConnectorFactory.connect(url, env);
      con = jmxConnector.getMBeanServerConnection();
    }
  }

  /**
   * Override to tear down your specific external resource.
   */
  protected void after(Description description) throws Throwable {
    if (jmxConnector != null) {
      jmxConnector.close();
      jmxConnector = null;
    }

    con = null;
  }

}
