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

package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_ACCESSOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.security.templates.SimpleAccessController;
import org.apache.geode.security.templates.SimpleAuthenticator;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.security.templates.UsernamePrincipal;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * This test class reproduces the tests present in
 * {@link ClientDataAuthorizationUsingLegacySecurityDUnitTest} and confirms that permissions are
 * maintained over failover.
 */
@Category({DistributedTest.class, SecurityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class ClientDataAuthorizationUsingLegacySecurityWithFailoverDUnitTest {
  @Rule
  public ClusterStartupRule csRule = new ClusterStartupRule();

  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;
  private static String regionName = "testRegion";

  // Some data values against which we will test.
  private static final String server_k1 = "server-key-1";
  private static final String server_v1 = "server-value-1";
  private static final String server_k2 = "server-key-2";
  private static final String server_v2 = "server-value-2";
  private static Map<String, String> serverData = new HashMap<>();

  static {
    serverData.put(server_k1, server_v1);
    serverData.put(server_k2, server_v2);
  }

  // Some data values against which we will test.
  private static final String client_k1 = "client-key-1";
  private static final String client_v1 = "client-value-1";
  private static final String client_k2 = "client-key-2";
  private static final String client_v2 = "client-value-2";
  private static final String client_k3 = "client-key-3";
  private static final String client_v3 = "client-value-3";

  private static final String client_k4 = "client-key-4";
  private static final String client_v4 = "client-value-4";
  private static final String client_k5 = "client-key-5";
  private static final String client_v5 = "client-value-5";

  private static final String client_k6 = "client-key-6";
  private static final String client_v6 = "client-value-6";
  private static final String client_k7 = "client-key-7";
  private static final String client_v7 = "client-value-7";
  private static Map<String, String> clientData45 = new HashMap<>();

  static {
    clientData45.put(client_k4, client_v4);
    clientData45.put(client_k5, client_v5);
  }

  private static Map<String, String> clientData67 = new HashMap<>();

  static {
    clientData67.put(client_k6, client_v6);
    clientData67.put(client_k7, client_v7);
  }

  // Test against every client version
  @Parameterized.Parameter
  public String clientVersion;

  @Parameterized.Parameters(name = "clientVersion={0}")
  public static Collection<String> data() {
    return VersionManager.getInstance().getVersions();
  }

  @Before
  public void setup() throws Exception {
    Properties clusterMemberProperties = getVMPropertiesWithPermission("cluster,data");
    int version = Integer.parseInt(clientVersion);
    if (version == 0 || version >= 140) {
      clusterMemberProperties.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
          "org.apache.geode.security.templates.UsernamePrincipal");
    }

    locator = csRule.startLocatorVM(0, clusterMemberProperties);
    server1 = csRule.startServerVM(1, clusterMemberProperties, locator.getPort());
    server2 = csRule.startServerVM(2, clusterMemberProperties, locator.getPort());

    // put some data on the cluster.
    server1.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
      rf.addCacheListener(new ClientAuthorizationFailoverTestListener());
      Region<String, String> region = rf.create(regionName);
      region.putAll(serverData);
    });

    server2.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
      Region<String, String> region = rf.create(regionName);
      assertThat(region.getAll(serverData.keySet())).containsAllEntriesOf(serverData);
    });
  }

  @Test
  public void dataReaderCanStillOnlyReadAfterFailover() throws Exception {
    // Connect to the server that will fail
    ClientVM client = createAndInitializeClientAndCache("dataRead");

    // Client should be able to read and not write.
    client.invoke(() -> {
      Region<String, String> region = ClusterStartupRule.getClientCache().getRegion(regionName);
      // Assert that the client can get
      assertThat(region.get(server_k1)).isEqualTo(server_v1);
      assertThat(region.get(server_k2, null)).isEqualTo(server_v2);
      assertThat(region.getAll(serverData.keySet())).containsAllEntriesOf(serverData);
      assertThat(region.getAll(serverData.keySet(), null)).containsAllEntriesOf(serverData);
      // Assert that the client cannot put
      assertThatThrownBy(() -> region.put(client_k1, client_v1))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.put(client_k2, client_v2, null))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.putIfAbsent(client_k3, client_v3))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.putAll(clientData45))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.putAll(clientData67, null))
          .hasCauseInstanceOf(NotAuthorizedException.class);
    });

    // Initialize client cache and region. Get the port of the primary connected server.
    MemberVM server_to_fail = determinePrimaryServer(client);

    // Bring down primary server
    server_to_fail.stopMember(true);

    // Confirm failover
    MemberVM secondaryServer = (server1.getPort() == server_to_fail.getPort()) ? server2 : server1;
    await().until(() -> getPrimaryServerPort(client) == secondaryServer.getPort());

    // Confirm permissions: client should still only be able to read and not write.
    client.invoke(() -> {
      Region<String, String> region = ClusterStartupRule.getClientCache().getRegion(regionName);
      // Assert that the client can get
      assertThat(region.get(server_k1)).isEqualTo(server_v1);
      assertThat(region.get(server_k2, null)).isEqualTo(server_v2);
      assertThat(region.getAll(serverData.keySet())).containsAllEntriesOf(serverData);
      assertThat(region.getAll(serverData.keySet(), null)).containsAllEntriesOf(serverData);
      // Assert that the client cannot put
      assertThatThrownBy(() -> region.put(client_k1, client_v1))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.put(client_k2, client_v2, null))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.putIfAbsent(client_k3, client_v3))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.putAll(clientData45))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.putAll(clientData67, null))
          .hasCauseInstanceOf(NotAuthorizedException.class);
    });

    // Confirm that no puts went through
    secondaryServer.invoke(() -> {
      assertThat(ClusterStartupRule.getCache().getRegion(regionName))
          .containsOnlyKeys(server_k1, server_k2).containsAllEntriesOf(serverData);
    });
  }

  @Test
  public void dataWriterCanStillOnlyWriteAfterFailover() throws Exception {
    // Connect to the server that will fail
    ClientVM client = createAndInitializeClientAndCache("dataWrite");

    // Client should be able to write but not read.
    client.invoke(() -> {
      Region<String, String> region = ClusterStartupRule.getClientCache().getRegion(regionName);
      // Puts do not throw
      // Assert that the client can put
      region.put(client_k1, client_v1);
      region.put(client_k2, client_v2, null);
      region.putIfAbsent(client_k3, client_v3);
      region.putAll(clientData45);
      region.putAll(clientData67, null);
      // Assert that the client cannot get
      assertThatThrownBy(() -> region.get(server_k1))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.get(server_k2, null))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      // An unauthorized getAll does not throw; it just does not return the requested values.
      // See GEODE-3632.
      assertThat(region.getAll(serverData.keySet())).isEmpty();
      assertThat(region.getAll(serverData.keySet(), null)).isEmpty();
    });

    // Initialize client cache and region. Get the port of the primary connected server.
    MemberVM server_to_fail = determinePrimaryServer(client);

    // Bring down primary server
    server_to_fail.stopMember(true);

    // Confirm failover
    MemberVM secondaryServer = (server1.getPort() == server_to_fail.getPort()) ? server2 : server1;
    await().until(() -> getPrimaryServerPort(client) == secondaryServer.getPort());

    // Confirm permissions: client should still only be able to write and not read.
    client.invoke(() -> {
      Region<String, String> region = ClusterStartupRule.getClientCache().getRegion(regionName);
      // Puts do not throw
      // Assert that the client can put
      region.put(client_k1, client_v1);
      region.put(client_k2, client_v2, null);
      region.putIfAbsent(client_k3, client_v3);
      region.putAll(clientData45);
      region.putAll(clientData67, null);
      // Assert that the client cannot get
      assertThatThrownBy(() -> region.get(server_k1))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.get(server_k2, null))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      // An unauthorized getAll does not throw; it just does not return the requested values.
      // See GEODE-3632.
      assertThat(region.getAll(serverData.keySet())).isEmpty();
      assertThat(region.getAll(serverData.keySet(), null)).isEmpty();
    });
  }

  @Test
  public void dataReaderCanRegisterAndUnregisterAcrossFailover() throws Exception {
    // Connect to the server that will fail
    ClientVM client = createAndInitializeClientAndCache("dataRead");

    // Client should be able to register and unregister interests.
    client.invoke(() -> {
      Region<String, String> region = ClusterStartupRule.getClientCache().getRegion(regionName);
      region.unregisterInterest(client_k1);
      region.registerInterest(client_k1);
      region.registerInterestRegex("client-.*");
      region.unregisterInterestRegex("client-.*");
    });

    // Initialize client cache and region. Get the port of the primary connected server.
    MemberVM server_to_fail = determinePrimaryServer(client);

    // Bring down primary server
    server_to_fail.stopMember(true);

    // Confirm failover
    MemberVM secondaryServer = (server1.getPort() == server_to_fail.getPort()) ? server2 : server1;
    await().until(() -> getPrimaryServerPort(client) == secondaryServer.getPort());

    // Confirm permissions.
    client.invoke(() -> {
      Region<String, String> region = ClusterStartupRule.getClientCache().getRegion(regionName);
      region.unregisterInterest(client_k1);
      region.registerInterest(client_k1);
      region.registerInterestRegex("client-.*");
      region.unregisterInterestRegex("client-.*");
    });
  }

  @Test
  public void dataWriterCannotRegisterInterestAcrossFailover() throws Exception {
    Properties props = getVMPropertiesWithPermission("dataWrite");
    int version = Integer.parseInt(clientVersion);
    if (version == 0 || version >= 140) {
      props.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
          "org.apache.geode.security.templates.UsernamePrincipal");
    }

    int server1Port = this.server1.getPort();
    int server2Port = this.server2.getPort();

    ClientVM client1 = csRule.startClientVM(3, props, cf -> cf
        .addPoolServer("localhost", server1Port).addPoolServer("localhost", server2Port)
        .setPoolSubscriptionEnabled(true).setPoolSubscriptionRedundancy(2), clientVersion);

    // Initialize cache
    client1.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      ClientRegionFactory<String, String> rf =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<String, String> region1 = rf.create(regionName);
    });

    ClientVM client = client1;

    // Client should be able to register and unregister interests.
    client.invoke(() -> {
      Region<String, String> region = ClusterStartupRule.getClientCache().getRegion(regionName);
      assertThatThrownBy(() -> region.registerInterest(client_k1))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.registerInterestRegex("client-.*"))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      // Attempts to unregister will fail client-side. The client maintains its own lists of
      // interests and, since the above failed, any unregistering of interest will prematurely
      // terminate before contacting any server. No authorization is attempted.
    });

    // Initialize client cache and region. Get the port of the primary connected server.
    MemberVM server_to_fail = determinePrimaryServer(client);

    // Bring down primary server
    server_to_fail.stopMember(true);

    // Confirm failover
    MemberVM secondaryServer = (server1.getPort() == server_to_fail.getPort()) ? server2 : server1;
    await().until(() -> getPrimaryServerPort(client) == secondaryServer.getPort());

    // Confirm permissions.
    client.invoke(() -> {
      Region<String, String> region = ClusterStartupRule.getClientCache().getRegion(regionName);
      assertThatThrownBy(() -> region.registerInterest(client_k1))
          .hasCauseInstanceOf(NotAuthorizedException.class);
      assertThatThrownBy(() -> region.registerInterestRegex("client-.*"))
          .hasCauseInstanceOf(NotAuthorizedException.class);
    });
  }

  private ClientVM createAndInitializeClientAndCache(String withPermission) throws Exception {
    int server1Port = this.server1.getPort();
    int server2Port = this.server2.getPort();

    Properties props = getVMPropertiesWithPermission(withPermission);

    int version = Integer.parseInt(clientVersion);
    if (version == 0 || version >= 140) {
      props.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
          "org.apache.geode.security.templates.UsernamePrincipal");
    }

    ClientVM client = csRule.startClientVM(3, props, cf -> cf
        .addPoolServer("localhost", server1Port).addPoolServer("localhost", server2Port)
        .setPoolSubscriptionEnabled(true).setPoolSubscriptionRedundancy(2), clientVersion);

    // Initialize cache
    client.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      ClientRegionFactory<String, String> rf =
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
      Region<String, String> region = rf.create(regionName);
    });

    return client;
  }

  private MemberVM determinePrimaryServer(ClientVM client) {
    int primaryPort = getPrimaryServerPort(client);
    return (primaryPort == server1.getPort()) ? server1 : server2;
  }

  private int getPrimaryServerPort(ClientVM client) {
    return client.invoke(() -> {
      ClientCache cache = ClusterStartupRule.getClientCache();
      PoolImpl pool = (PoolImpl) cache.getDefaultPool();
      return pool.getPrimaryPort();
    });
  }

  private Properties getVMPropertiesWithPermission(String permission) {
    Properties props = new Properties();
    // Using the legacy security framework
    props.setProperty(SECURITY_CLIENT_AUTHENTICATOR,
        SimpleAuthenticator.class.getCanonicalName() + ".create");
    props.setProperty(SECURITY_CLIENT_ACCESSOR,
        SimpleAccessController.class.getCanonicalName() + ".create");

    // Using the given username/permission string
    props.setProperty(UserPasswordAuthInit.USER_NAME, permission);
    props.setProperty(UserPasswordAuthInit.PASSWORD, permission);
    props.setProperty(SECURITY_CLIENT_AUTH_INIT,
        UserPasswordAuthInit.class.getCanonicalName() + ".create");
    // We can't sent the object filter property versions before 1.4.0 because
    // it's not a valid property, but we must set it in 140 and above to allow
    // serialization of UsernamePrincipal
    if (clientVersion.compareTo("140") >= 0) {
      props.setProperty(SERIALIZABLE_OBJECT_FILTER, UsernamePrincipal.class.getCanonicalName());
    }
    return props;
  }

  /** A trivial listener */
  private static class ClientAuthorizationFailoverTestListener
      extends CacheListenerAdapter<String, String> {
    private static final Logger logger = LogService.getLogger();

    @Override
    public void afterCreate(EntryEvent<String, String> event) {
      logger.info("In afterCreate");
    }
  }
}
