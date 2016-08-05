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
package com.gemstone.gemfire.distributed;

/**
 * This class defines all the static definitions for the {@link ConfigurationProperties#SSL_ENABLED_COMPONENTS}
 * <U>Since</U>: Geode 1.0
 */
public interface SSLEnabledComponents {

  /**
   * This determines that all components will use the SSL for their communications.
   * <U>Since</U>: Geode 1.0
   */
  String ALL = "all";
  /**
   * This determines that only the server components will use the SSL for their communications. This means that all communications
   * between clients and servers will use SSL. In addition this also means that client-locator and server-locator communications will use SSL credentials.
   * <U>Since</U>: Geode 1.0
   */
  String SERVER = "server";
  /**
   * This determines that only the inter-server (or server-to-server) communication will use the SSL.
   * In addition this also means that server-locator communications will use SSL credentials.
   * <U>Since</U>: Geode 1.0
   */
  String CLUSTER = "cluster";
  /**
   * This determines that only the jmx component will use the SSL for its communications.
   * <U>Since</U>: Geode 1.0
   */
  String JMX = "jmx";
  /**
   * This determines that the http service component will use the SSL for its communications
   * <U>Since</U>: Geode 1.0
   */
  String HTTP_SERVICE = "http";
  /**
   * This determines that the gateway component will use the SSL for its communications.
   * <U>Since</U>: Geode 1.0
   */
  String GATEWAY = "gateway";
  /**
   * This determines that the locator component will use the SSL for its communications between server and locator and client and locator.
   * <U>Since</U>: Geode 1.0
   */
  String LOCATOR = "locator";
}
