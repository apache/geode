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

package org.apache.geode.management.internal;


import static org.apache.geode.management.internal.ManagementAgent.getRmiServerHostname;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ManagementAgentTest {

  public static final String CLIENT = "client.host.name";
  public static final String SERVER = "server.host.name";
  public static final String EMPTY = "";

  @Test
  void getRmiServerHostnameReturnsHostnameForClientsWhenHostnameForClientsNotBlank() {
    assertThat(getRmiServerHostname(CLIENT, SERVER, false)).isEqualTo(CLIENT);
    assertThat(getRmiServerHostname(CLIENT, SERVER, true)).isEqualTo(CLIENT);
  }

  @Test
  void getRmiServerHostnameReturnsNullWhenHostnameForClientsIsBlankAndSslDisabled() {
    assertThat(getRmiServerHostname(null, SERVER, false)).isNull();
    assertThat(getRmiServerHostname(EMPTY, SERVER, false)).isNull();
  }

  @Test
  void getRmiServerHostnameReturnsHostnameForServerWhenHostnameForClientsIsBlankAndSslEnabled() {
    assertThat(getRmiServerHostname(null, SERVER, true)).isEqualTo(SERVER);
    assertThat(getRmiServerHostname(EMPTY, SERVER, true)).isEqualTo(SERVER);
  }

  @Test
  void getRmiServerHostnameReturnsNullWhenHostnameForClientsIsBlankAndHostameForServerIsBlank() {
    assertThat(getRmiServerHostname(null, EMPTY, false)).isNull();
    assertThat(getRmiServerHostname(null, null, false)).isNull();
    assertThat(getRmiServerHostname(EMPTY, null, false)).isNull();
    assertThat(getRmiServerHostname(EMPTY, EMPTY, false)).isNull();
    assertThat(getRmiServerHostname(null, EMPTY, true)).isNull();
    assertThat(getRmiServerHostname(null, null, true)).isNull();
    assertThat(getRmiServerHostname(EMPTY, null, true)).isNull();
    assertThat(getRmiServerHostname(EMPTY, EMPTY, true)).isNull();
  }

}
