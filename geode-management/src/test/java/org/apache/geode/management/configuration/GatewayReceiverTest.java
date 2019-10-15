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

package org.apache.geode.management.configuration;

import static org.apache.geode.management.api.Links.URI_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

public class GatewayReceiverTest {
  private GatewayReceiver receiver;

  @Before
  public void before() {
    receiver = new GatewayReceiver();
  }

  @Test
  public void getId() {
    assertThat(receiver.getId()).isEqualTo("cluster");

    receiver.setGroup("group");
    assertThat(receiver.getId()).isEqualTo("group");
  }

  @Test
  public void getUri() {
    assertThat(receiver.getLinks().getSelf())
        .isEqualTo("/gateways/receivers/cluster");

    receiver.setGroup("group");
    assertThat(receiver.getLinks().getSelf())
        .isEqualTo(URI_CONTEXT + "/experimental/gateways/receivers/group");
  }
}
