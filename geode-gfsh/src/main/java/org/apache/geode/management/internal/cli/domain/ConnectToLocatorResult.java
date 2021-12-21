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
package org.apache.geode.management.internal.cli.domain;

import org.apache.geode.management.internal.cli.util.ConnectionEndpoint;

/**
 *
 *
 * @since GemFire 7.0
 */
public class ConnectToLocatorResult {
  private ConnectionEndpoint memberEndpoint = null;
  private final String resultMessage;
  private final boolean isJmxManagerSslEnabled;

  public ConnectToLocatorResult(ConnectionEndpoint memberEndpoint, String resultMessage,
      boolean isJmxManagerSslEnabled) {
    this.memberEndpoint = memberEndpoint;
    this.resultMessage = resultMessage;
    this.isJmxManagerSslEnabled = isJmxManagerSslEnabled;
  }

  public ConnectionEndpoint getMemberEndpoint() {
    return memberEndpoint;
  }

  public String getResultMessage() {
    return resultMessage;
  }

  public boolean isJmxManagerSslEnabled() {
    return isJmxManagerSslEnabled;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Member Endpoint :" + memberEndpoint.toString());
    sb.append("\nResult Message : " + resultMessage);
    return sb.toString();
  }
}
