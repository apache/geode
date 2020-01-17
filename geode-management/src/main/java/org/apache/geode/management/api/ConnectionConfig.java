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
 *
 */

package org.apache.geode.management.api;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.geode.annotations.Experimental;

/**
 * Interface representing the properties required to configure and create a
 * {@link ClusterManagementServiceTransport}. Although the current implementation is HTTP based,
 * these properties should be general enough that they are relevant to any different implementation.
 * <p/>
 * Concrete implementations of this interface can be created through either the
 * {@link BaseConnectionConfig} or
 * {@code GeodeClusterManagementServiceConnectionConfig} classes.
 */
@Experimental
public interface ConnectionConfig {

  String getHost();

  int getPort();

  String getAuthToken();

  SSLContext getSslContext();

  String getUsername();

  String getPassword();

  HostnameVerifier getHostnameVerifier();
}
