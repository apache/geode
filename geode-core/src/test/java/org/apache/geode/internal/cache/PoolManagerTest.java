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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.xmlcache.Declarable2;
import org.apache.geode.internal.net.SocketCreatorFactory;

public class PoolManagerTest {
  private PoolImpl pool;
  private PoolManagerImpl poolManager;

  @Before
  public void setUp() {
    pool = mock(PoolImpl.class);
    poolManager = spy(new PoolManagerImpl(true));

    assertThat(poolManager.getMap()).isEmpty();
  }

  // in Mulituser Auth mode
  @Test
  public void Test_thatDataSerializerSynchronizationMessagesAreNotSent() {
    PoolManagerImpl poolManagerImpl = poolManager;

    InternalDistributedSystem mockSystem = mock(InternalDistributedSystem.class);
    InternalDistributedSystem.addTestSystem(mockSystem);
    InternalDistributedMember mockMember = mock(InternalDistributedMember.class);
    doReturn(mockMember).when(mockSystem).getDistributedMember();

    PoolFactory poolFactory = mock(PoolFactory.class);
    when(poolManagerImpl.createFactory()).thenReturn(poolFactory);
    when(poolFactory.create(any())).thenReturn(pool);

    assertThat(poolManagerImpl.createFactory().create("test")).isEqualTo(pool);
    doReturn(true).when(pool).getMultiuserAuthentication();
    doReturn(null).when(pool).execute(any());
    PoolManagerImpl.setImpl(poolManagerImpl);
    Map<String, Pool> map = new HashMap<>();
    map.put("test_pool", pool);
    when(poolManagerImpl.getMap()).thenReturn(map);
    DataSerializer dataSerializer = mock(DataSerializer.class);
    EventID eventID = new EventID(mockSystem);
    doReturn(eventID).when(dataSerializer).getEventId();
    PoolManagerImpl.allPoolsRegisterDataSerializers(dataSerializer);
    verify(pool, times(0)).execute(any());
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
        new InternalDistributedMember(InetAddress.getByName("localhost"), 10334, false, false);
    doReturn(mockMember).when(mockSystem).getDistributedMember();

    InternalDistributedSystem.addTestSystem(mockSystem);
    PoolFactory poolFactory = poolManagerImpl.createFactory();
    poolFactory.setMultiuserAuthentication(multiUserAuthEnabled);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    poolFactory.setSocketFactory(new TestSocketFactory());
    poolFactory.addLocator("localhost", 10334);
    return (PoolImpl) poolFactory.create("test_pool");
  }



  @Test
  public void Test_thatWhenUsingMultiUserAuthModeDataSerializerRecoveryTaskNotStarted()
      throws UnknownHostException {

    PoolImpl poolImpl = setupPool(true);
    EndpointManagerImpl endpointManager = (EndpointManagerImpl) poolImpl.getEndpointManager();
    Set<EndpointManager.EndpointListener> listeners = endpointManager.getListeners();
    boolean foundDataStoreRecoveryListener = false;
    for (EndpointManager.EndpointListener endpointListener : listeners) {
      if (endpointListener instanceof DataSerializerRecoveryListener) {
        foundDataStoreRecoveryListener = true;
        break;
      }
    }
    assertThat(foundDataStoreRecoveryListener).isFalse();
  }


  @Test
  public void Test_thatWhenUsingMultiUserAuthModeDataSerializerRecoveryTaskIsStarted()
      throws UnknownHostException {

    PoolImpl poolImpl = setupPool(false);
    EndpointManagerImpl endpointManager = (EndpointManagerImpl) poolImpl.getEndpointManager();
    Set<EndpointManager.EndpointListener> listeners = endpointManager.getListeners();
    boolean foundDataStoreRecoveryListener = false;
    for (EndpointManager.EndpointListener endpointListener : listeners) {
      if (endpointListener instanceof DataSerializerRecoveryListener) {
        foundDataStoreRecoveryListener = true;
        break;
      }
    }
    assertThat(foundDataStoreRecoveryListener).isTrue();
  }

  @Test
  public void unregisterShouldThrowExceptionWhenPoolHasRegionsStillAssociated() {
    when(pool.getAttachCount()).thenReturn(2);

    assertThatThrownBy(() -> poolManager.unregister(pool)).isInstanceOf(IllegalStateException.class)
        .hasMessage("Pool could not be destroyed because it is still in use by 2 regions");
  }

  @Test
  public void unregisterShouldReturnFalseWhenThePoolIsNotPartOfTheManagedPools() {
    when(pool.getAttachCount()).thenReturn(0);

    assertThat(poolManager.unregister(pool)).isFalse();
  }

  @Test
  public void unregisterShouldReturnTrueWhenThePoolIsSuccessfullyRemovedFromTheManagedPools() {
    when(pool.getAttachCount()).thenReturn(0);
    poolManager.register(pool);
    assertThat(poolManager.getMap()).isNotEmpty();

    assertThat(poolManager.unregister(pool)).isTrue();
    assertThat(poolManager.getMap()).isEmpty();
  }
}
