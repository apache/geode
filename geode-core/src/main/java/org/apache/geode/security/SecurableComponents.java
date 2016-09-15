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
package org.apache.geode.security;

import org.apache.geode.distributed.ConfigurationProperties;

/**
 * This class defines all the static definitions for the {@link ConfigurationProperties#SECURITY_ENABLED_COMPONENTS}
 * <U>Since</U>: Geode 1.0
 */
public interface SecurableComponents {

  /**
   * This determines that all components will be secured.
   * <U>Since</U>: Geode 1.0
   */
  String ALL = "all";
  /**
   * This determines that the client-server communication will be secured.
   * <U>Since</U>: Geode 1.0
   */
  String SERVER = "server";
  /**
   * This determines that the inter-server (or server-to-server) communication will be secured.
   * <U>Since</U>: Geode 1.0
   */
  String CLUSTER = "cluster";
  /**
   * This determines that test jmx communication will be secured.
   * <U>Since</U>: Geode 1.0
   */
  String JMX = "jmx";
  /**
   * This determines that the http service communication will be secured.
   * <U>Since</U>: Geode 1.0
   */
  String HTTP_SERVICE = "http";
  /**
   * This determines that the gateway communication will be secured.
   * <U>Since</U>: Geode 1.0
   */
  String GATEWAY = "gateway";
  /**
   * This determines that the locator communication will be secured.
   * <U>Since</U>: Geode 1.0
   */
  String LOCATOR = "locator";
}
