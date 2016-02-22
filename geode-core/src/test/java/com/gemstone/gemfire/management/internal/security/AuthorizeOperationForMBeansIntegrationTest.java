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

import static org.junit.Assert.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.security.ResourceOperationContext.ResourceOperationCode;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.Authenticator;
import com.gemstone.gemfire.security.NotAuthorizedException;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests <code>JSONAuthorization.authorizeOperation(...)</code> with GemFire MBeans.
 */
@Category(IntegrationTest.class)
@SuppressWarnings("deprecation")
public class AuthorizeOperationForMBeansIntegrationTest {

  private GemFireCacheImpl cache;
  private DistributedSystem ds;
  private int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
  private JMXConnector jmxConnector;
  private MBeanServerConnection mbeanServer;

  @Rule
  public TestName testName = new TestName();
  
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    System.setProperty("resource-auth-accessor", TestAccessControl.class.getName());
    System.setProperty("resource-authenticator", TestAuthenticator.class.getName());
    
    Properties properties = new Properties();
    properties.put("name", this.testName.getMethodName());
    properties.put(DistributionConfig.LOCATORS_NAME, "");
    properties.put(DistributionConfig.MCAST_PORT_NAME, "0");
    properties.put(DistributionConfig.JMX_MANAGER_NAME, "true");
    properties.put(DistributionConfig.JMX_MANAGER_START_NAME, "true");
    properties.put(DistributionConfig.JMX_MANAGER_PORT_NAME, String.valueOf(this.jmxManagerPort));
    properties.put(DistributionConfig.HTTP_SERVICE_PORT_NAME, "0");
    
    this.ds = DistributedSystem.connect(properties);
    this.cache = (GemFireCacheImpl) CacheFactory.create(ds);

    this.jmxConnector = getGemfireMBeanServer(this.jmxManagerPort, "tushark", "tushark");
    this.mbeanServer = this.jmxConnector.getMBeanServerConnection();
  }

  @After
  public void tearDown() throws Exception {
    if (this.jmxConnector != null) {
      this.jmxConnector.close();
      this.jmxConnector = null;
    }
    if (this.cache != null) {
      this.cache.close();
      this.cache = null;
    }
    if (this.ds != null) {
      this.ds.disconnect();
      this.ds = null;
    }
  }

  /**
   * This is testing a sampling of operations for DistributedSystemMXBean and AccessControlMXBean
   */
  @Test
  public void operationsShouldBeCoveredByAuthorization() throws Exception {
    ObjectName objectName = MBeanJMXAdapter.getDistributedSystemName();
    
    checkListCacheServerObjectNames(objectName);
    checkAlertLevel(objectName);
    checkAccessControlMXBean();
    checkBackUpMembers(objectName);
    checkShutDownAllMembers(objectName);
    checkCLIContext(this.mbeanServer);
  }
  
  private void checkListCacheServerObjectNames(final ObjectName objectName) throws Exception {
    Object cacheServerObjectNames = this.mbeanServer.invoke(objectName, "listCacheServerObjectNames", null, null);
    assertThat(cacheServerObjectNames).isNotNull().isInstanceOf(ObjectName[].class);
    assertThat((ObjectName[])cacheServerObjectNames).hasSize(0); // this isn't really testing much since there are no CacheServers
  }
  
  private void checkAlertLevel(final ObjectName objectName) throws Exception {
    // attribute AlertLevel
    String oldLevel = (String) this.mbeanServer.getAttribute(objectName, "AlertLevel");
    assertThat(oldLevel).isEqualTo("severe");
    
    // operation changeAlertLevel
    this.mbeanServer.invoke(
        objectName, 
        "changeAlertLevel", 
        new Object[] { "warning" },
        new String[] { String.class.getName() }
    );
    String newLevel = (String) this.mbeanServer.getAttribute(objectName, "AlertLevel");
    assertThat(newLevel).isEqualTo("warning");
  }
  
  private void checkAccessControlMXBean() throws Exception {
    final ResourceOperationCode resourceOperationCodes[] = { 
        ResourceOperationCode.LIST_DS, 
        ResourceOperationCode.READ_DS, 
        ResourceOperationCode.CHANGE_ALERT_LEVEL_DS,
        ResourceOperationCode.LOCATE_ENTRY_REGION 
    };
    
    ObjectName objectName = new ObjectName(ManagementInterceptor.OBJECT_NAME_ACCESSCONTROL);
    for (ResourceOperationCode resourceOperationCode : resourceOperationCodes) {
      boolean isAuthorizedForOperation = (Boolean) this.mbeanServer.invoke(
          objectName, 
          "authorize", 
          new Object[] { resourceOperationCode.toString() },
          new String[] { String.class.getName() }
      );
      assertThat(isAuthorizedForOperation).isTrue();
    }

    boolean isAuthorizedForAllOperations = (Boolean) mbeanServer.invoke(
        objectName, 
        "authorize", 
        new Object[] { ResourceOperationCode.ADMIN_DS.toString() },
        new String[] { String.class.getName() }
    );
    assertThat(isAuthorizedForAllOperations).isFalse();
  }

  private void checkBackUpMembers(final ObjectName objectName) throws Exception {
    try {
      this.mbeanServer.invoke(
          objectName, 
          "backupAllMembers", 
          new Object[] { "targetPath", "baseLinePath" },
          new String[] { String.class.getCanonicalName(), String.class.getCanonicalName() });
      fail("Should not be authorized for backupAllMembers");
    } catch (SecurityException expected) {
      // expected
    }
  }
  
  private void checkShutDownAllMembers(final ObjectName objectName) throws Exception {
    try {
      this.mbeanServer.invoke(
          objectName, 
          "shutDownAllMembers", 
          null, 
          null
      );
      fail("Should not be authorized for shutDownAllMembers");
    } catch (SecurityException expected) {
      // expected
    }
  }
  
  private void checkCLIContext(MBeanServerConnection mbeanServer) {
    ObjectName objectName = MBeanJMXAdapter.getDistributedSystemName();
    DistributedSystemMXBean proxy = JMX.newMXBeanProxy(mbeanServer, objectName, DistributedSystemMXBean.class);
    ObjectName managerMemberObjectName = proxy.getMemberObjectName();
    MemberMXBean memberMXBeanProxy = JMX.newMXBeanProxy(mbeanServer, managerMemberObjectName, MemberMXBean.class);

    Map<String, String> map = new HashMap<String, String>();
    map.put("APP", "GFSH");
    String result = memberMXBeanProxy.processCommand("locate entry --key=k1 --region=/region1", map);
    
    assertThat(result).isNotNull().doesNotContain(SecurityException.class.getSimpleName());
  }

  private JMXConnector getGemfireMBeanServer(final int port, final String user, final String pwd) throws Exception {
    JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:" + port + "/jmxrmi");
    if (user != null) { // TODO: why isn't this deterministic? need to create 2nd test without a user?
      Map<String, String[]> env = new HashMap<String, String[]>();
      String[] creds = { user, pwd };
      env.put(JMXConnector.CREDENTIALS, creds);
      JMXConnector jmxc = JMXConnectorFactory.connect(url, env);
      return jmxc;
    } else {
      JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
      return jmxc;
    }
  }

  /**
   * Fake Principal for testing.
   */
  @SuppressWarnings("serial")
  public static class TestUsernamePrincipal implements Principal, Serializable {

    private final String userName;

    public TestUsernamePrincipal(final String userName) {
      this.userName = userName;
    }

    @Override
    public String getName() {
      return this.userName;
    }

    @Override
    public String toString() {
      return this.userName;
    }
  }

  /**
   * Fake Authenticator for testing.
   */
  public static class TestAuthenticator implements Authenticator {

    @Override
    public void close() {
    }

    @Override
    public void init(final Properties securityProps, final LogWriter systemLogger, final LogWriter securityLogger) throws AuthenticationFailedException {
    }

    @Override
    public Principal authenticate(final Properties props, final DistributedMember member) throws AuthenticationFailedException {
      String user = props.getProperty(ManagementInterceptor.USER_NAME);
      String pwd = props.getProperty(ManagementInterceptor.PASSWORD);
      if (user != null && !user.equals(pwd) && !"".equals(user)) {
        throw new AuthenticationFailedException("Wrong username/password");
      }
      return new TestUsernamePrincipal(user);
    }
  }

  /**
   * Fake AccessControl for testing.
   */
  public static class TestAccessControl implements AccessControl {

    private Principal principal;

    @Override
    public void close() {
    }

    @Override
    public void init(final Principal principal, final DistributedMember remoteMember, final Cache cache) throws NotAuthorizedException {
      this.principal = principal;
    }

    @Override
    public boolean authorizeOperation(String regionName, OperationContext context) {
      if (principal.getName().equals("tushark")) {
        ResourceOperationCode authorizedOps[] = { 
            ResourceOperationCode.LIST_DS, 
            ResourceOperationCode.READ_DS, 
            ResourceOperationCode.CHANGE_ALERT_LEVEL_DS,
            ResourceOperationCode.LOCATE_ENTRY_REGION 
        };

        ResourceOperationContext ctx = (ResourceOperationContext) context;
        boolean found = false;
        for (ResourceOperationCode code : authorizedOps) {
          if (ctx.getResourceOperationCode().equals(code)) {
            found = true;
            break;
          }
        }
        return found;
      }
      return false;
    }
  }
}
