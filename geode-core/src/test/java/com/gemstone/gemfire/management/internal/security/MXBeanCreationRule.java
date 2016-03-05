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
package com.gemstone.gemfire.management.internal.security;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.HashMap;
import java.util.Map;

/**
 * Class which eases the creation of MBeans for security testing. When combined with {@link JMXConnectionConfiguration}
 * it allows for the creation of per-test connections with different user/password combinations.
 *
 * @param <T> The type of MBean which will be returned.
 */
public class MXBeanCreationRule<T> extends ExternalResource {

  private final int jmxServerPort;
  private Class<T> proxyClass;
  private JMXConnector jmxConnector;
  private ObjectName beanObjectName;
  private MBeanServerConnection mbeanServer;

  private static class JMXConfigurationStatement extends Statement {

    private final Description description;
    private final Statement base;
    private final MXBeanCreationRule mxRule;

    JMXConfigurationStatement(Statement base, Description description, MXBeanCreationRule rule) {
      this.description = description;
      this.base = base;
      this.mxRule = rule;
    }

    @Override
    public void evaluate() throws Throwable {
      mxRule.createConnection(getUser(), getPassword());
      try {
        base.evaluate();
      } finally {
        mxRule.close();
      }
    }

    private String getUser() {
      return description.getAnnotation(JMXConnectionConfiguration.class).user();
    }

    private String getPassword() {
      return description.getAnnotation(JMXConnectionConfiguration.class).password();
    }
  }

  /**
   * Rule constructor
   * @param port The JMX server port to connect to
   * @param proxyClass The class for which a proxy MBean will be created
   */
  public MXBeanCreationRule(int port, Class<T> proxyClass) {
    this.jmxServerPort = port;
    this.proxyClass = proxyClass;
  }

  /**
   * Retrieve a new proxy MBean
   * @return A new proxy MBean of the same type with which the class was constructed
   */
  public T getProxyMBean() {
    return JMX.newMBeanProxy(mbeanServer, beanObjectName, proxyClass);
  }

  private void close() throws Exception{
    jmxConnector.close();
  }

  private void createConnection(String user, String password) throws Exception {
    Map<String, String[]> env = new HashMap<>();
    env.put(JMXConnector.CREDENTIALS, new String[] {user, password});
    JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:" + jmxServerPort + "/jmxrmi");

    jmxConnector = JMXConnectorFactory.connect(url, env);
    mbeanServer = jmxConnector.getMBeanServerConnection();

    ObjectName objectNamePattern = lookupObjectName();
    ObjectInstance bean = (ObjectInstance) mbeanServer.queryMBeans(objectNamePattern, null).toArray()[0];
    beanObjectName = bean.getObjectName();
  }

  private ObjectName lookupObjectName() throws Exception {
    switch (proxyClass.getName()) {
      case "com.gemstone.gemfire.management.CacheServerMXBean":
        return(ObjectName.getInstance("GemFire:service=CacheServer,*"));
      default:
        throw new RuntimeException("Unknown MBean class: " + proxyClass.getName());
    }
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new JMXConfigurationStatement(base, description, this);
  }
}
