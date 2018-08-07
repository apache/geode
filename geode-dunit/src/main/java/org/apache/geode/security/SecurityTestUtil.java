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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.assertj.core.api.ThrowableAssert;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.internal.Version;
import org.apache.geode.security.templates.UserPasswordAuthInit;

public class SecurityTestUtil {

  public static ClientCache createClientCache(String username, String password, int serverPort) {
    Properties props = new Properties();
    return createClientCache(username, password, serverPort, props);
  }

  public static ClientCache createClientCache(String username, String password, int serverPort,
      Properties extraProperties) {
    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, username);
    props.setProperty(UserPasswordAuthInit.PASSWORD, password);
    props.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName());
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    props.putAll(extraProperties);
    if (Version.CURRENT.ordinal() >= 75) {
      props.setProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.security.query.data.*");
    }
    ClientCache cache = new ClientCacheFactory(props).setPoolSubscriptionEnabled(true)
        .addPoolServer("localhost", serverPort).create();
    return cache;
  }

  public static Region createProxyRegion(ClientCache cache, String regionName) {
    return cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(regionName);
  }

  public static void assertNotAuthorized(ThrowableAssert.ThrowingCallable shouldRaiseThrowable,
      String permString) {
    assertThatThrownBy(shouldRaiseThrowable).hasMessageContaining(permString);
  }
}
