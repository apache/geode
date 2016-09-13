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
/**
 * 
 */
package org.apache.geode.management.internal.cli.util;

/**
 *  
 *
 */
public class ConnectionEndpoint {
  public static final String JMXMANAGER_OPTION_CONTEXT  = "__jmx-manager__";
  public static final String LOCATOR_OPTION_CONTEXT = "__locator__";
  
  private final String host;
  private final int port;
  
  /**
   * @param host
   * @param port
   */
  public ConnectionEndpoint(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /**
   * @return the host
   */
  public String getHost() {
    return host;
  }

  /**
   * @return the port
   */
  public int getPort() {
    return port;
  }
  
  public String toString(boolean includeClassName) {
    StringBuilder builder = new StringBuilder();
    
    if (includeClassName) {
      builder.append(ConnectionEndpoint.class.getSimpleName());
    }
    builder.append("[host=").append(host).
            append(", port=").append(port).append("]");
    
    return builder.toString();
  }

  @Override
  public String toString() {
    return toString(true);
  }
}
