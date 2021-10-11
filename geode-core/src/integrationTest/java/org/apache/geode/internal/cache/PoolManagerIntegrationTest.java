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

package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.SocketFactory;
import org.apache.geode.cache.client.internal.DataSerializerRecoveryListener;
import org.apache.geode.cache.client.internal.EndpointManager;
import org.apache.geode.cache.client.internal.EndpointManagerImpl;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.RegisterDataSerializersOp;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.xmlcache.Declarable2;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;


public class PoolManagerIntegrationTest {

  private PoolImpl pool;
  private PoolManagerImpl poolManager;

  @ClassRule
  public static ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @BeforeClass
  public static void classSetup() {
    clusterStartupRule.startLocatorVM(0, 10334);
  }

  @Before
  public void setUp() {

    pool = mock(PoolImpl.class);
    poolManager = spy(new PoolManagerImpl(true));

    assertThat(poolManager.getMap()).isEmpty();
  }

  @Test
  public void whenMultiUserAuthenticationIsEnabledDataSerializerSynchronizationMessagesAreNotSent() {
    InternalDistributedSystem mockSystem = setupFakeSystem(true);
    DataSerializer dataSerializer = setupFakeEventAndDataSerializer(mockSystem);
    PoolManagerImpl.allPoolsRegisterDataSerializers(dataSerializer);
    verify(pool, times(0))
        .execute(any(RegisterDataSerializersOp.RegisterDataSerializersOpImpl.class));
  }

  @Test
  public void whenMultiUserAuthenticationIsDisabledDataSerializerSynchronizationMessagesAreSent() {
    InternalDistributedSystem mockSystem = setupFakeSystem(false);
    DataSerializer dataSerializer = setupFakeEventAndDataSerializer(mockSystem);
    PoolManagerImpl.allPoolsRegisterDataSerializers(dataSerializer);
    verify(pool, times(1))
        .execute(any(RegisterDataSerializersOp.RegisterDataSerializersOpImpl.class));
  }

  @Test
  public void whenUsingMultiUserAuthModeDataSerializerRecoveryTaskNotStarted()
      throws UnknownHostException {

    PoolImpl poolImpl = setupPool(true);
    EndpointManagerImpl endpointManager = (EndpointManagerImpl) poolImpl.getEndpointManager();
    Set<EndpointManager.EndpointListener> listeners = endpointManager.getListeners();

    assertThat(listeners).allSatisfy(
        listener -> assertThat(listener).isNotInstanceOf(DataSerializerRecoveryListener.class));
  }

  @Test
  public void whenNotUsingMultiUserAuthModeDataSerializerRecoveryTaskIsStarted()
      throws UnknownHostException {

    PoolImpl poolImpl = setupPool(false);
    EndpointManagerImpl endpointManager = (EndpointManagerImpl) poolImpl.getEndpointManager();
    Set<EndpointManager.EndpointListener> listeners = endpointManager.getListeners();

    assertThat(listeners).anySatisfy(
        listener -> assertThat(listener).isInstanceOf(DataSerializerRecoveryListener.class));
  }

  public static class TestSocketFactory implements SocketFactory, Declarable2 {
    @Override
    public Socket createSocket() throws IOException {
      return new Socket();
    }

    @Override
    public Properties getConfig() {
      return new Properties();
    }

    @Override
    public void initialize(Cache cache, Properties properties) {

    }
  }

  private InternalDistributedSystem setupFakeSystem(boolean multiUserAuthentication) {
    PoolManagerImpl poolManagerImpl = poolManager;
    InternalDistributedSystem mockSystem = mock(InternalDistributedSystem.class);
    InternalDistributedSystem.addTestSystem(mockSystem);
    InternalDistributedMember mockMember = mock(InternalDistributedMember.class);
    doReturn(mockMember).when(mockSystem).getDistributedMember();

    PoolFactory poolFactory = mock(PoolFactory.class);
    when(poolManagerImpl.createFactory()).thenReturn(poolFactory);
    when(poolFactory.create(any())).thenReturn(pool);

    assertThat(poolManagerImpl.createFactory().create("test")).isEqualTo(pool);
    doReturn(multiUserAuthentication).when(pool).getMultiuserAuthentication();
    doReturn(null).when(pool).execute(any());
    PoolManagerImpl.setImpl(poolManagerImpl);
    Map<String, Pool> map = new HashMap<>();
    map.put("test_pool", pool);
    when(poolManagerImpl.getMap()).thenReturn(map);
    return mockSystem;
  }

  private DataSerializer setupFakeEventAndDataSerializer(InternalDistributedSystem mockSystem) {
    DataSerializer dataSerializer = mock(DataSerializer.class);
    EventID eventID = new EventID(mockSystem);
    doReturn(eventID).when(dataSerializer).getEventId();
    return dataSerializer;
  }

  private PoolImpl setupPool(boolean multiUserAuthEnabled) throws UnknownHostException {
    PoolManagerImpl poolManagerImpl = poolManager;
    Properties properties = new Properties();
    properties.setProperty(DURABLE_CLIENT_ID, "1");
    DistributionConfig distributionConfig = new DistributionConfigImpl(properties);
    InternalDistributedSystem.BuilderForTesting builderForTesting =
        new InternalDistributedSystem.BuilderForTesting(properties);

    InternalDistributedSystem mockSystem = spy(builderForTesting.build());
    doReturn(distributionConfig).when(mockSystem).getConfig();
    InternalDistributedMember mockMember =
        new InternalDistributedMember(InetAddress.getByName("localhost"), 50505, false,
            false);
    doReturn(mockMember).when(mockSystem).getDistributedMember();

    InternalDistributedSystem.addTestSystem(mockSystem);
    PoolFactory poolFactory = poolManagerImpl.createFactory();
    poolFactory.setMultiuserAuthentication(multiUserAuthEnabled);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    poolFactory.setSocketFactory(new TestSocketFactory());
    poolFactory.addLocator("localhost", clusterStartupRule.getMember(0).getPort());
    return (PoolImpl) poolFactory.create("test_pool");
  }

}
