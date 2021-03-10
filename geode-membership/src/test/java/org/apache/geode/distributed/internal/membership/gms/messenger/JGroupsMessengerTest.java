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
package org.apache.geode.distributed.internal.membership.gms.messenger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.net.UnknownHostException;

import org.junit.Test;

import org.apache.geode.distributed.internal.membership.api.MembershipConfigurationException;
import org.apache.geode.internal.inet.LocalHostUtil;

public class JGroupsMessengerTest {

  @Test
  public void getAddressForUDPBinding_ReturnsLocalAddressWhenInputParametersAreEmpty()
      throws MembershipConfigurationException, UnknownHostException {
    JGroupsMessenger j = new JGroupsMessenger();
    String localHostAddress = LocalHostUtil.getLocalHost().getHostAddress();
    assertThat(j.getAddressForUDPBinding("", ""))
        .isEqualTo(localHostAddress);
  }

  @Test
  public void getAddressForUDPBinding_ReturnsBindAddressWhenDefinedAndMembershipBindAddressIsEmpty()
      throws MembershipConfigurationException {
    JGroupsMessenger j = new JGroupsMessenger();
    String bindAddress = "1.2.3.4";
    String membershipBindAddress = "";
    assertThat(j.getAddressForUDPBinding(bindAddress, membershipBindAddress))
        .isEqualTo(bindAddress);
  }

  @Test
  public void getAddressForUDPBinding_ReturnsMembershipAddressWhenDefinedAndBindAddressIsEmpty()
      throws MembershipConfigurationException {
    JGroupsMessenger j = new JGroupsMessenger();
    String bindAddress = "";
    String membershipBindAddress = "4.3.2.1";
    assertThat(j.getAddressForUDPBinding(bindAddress, membershipBindAddress))
        .isEqualTo(membershipBindAddress);
  }

  @Test
  public void getAddressForUDPBinding_ReturnsMembershipAddressWhenBothInputParametersAreDefined()
      throws MembershipConfigurationException {
    JGroupsMessenger j = new JGroupsMessenger();
    String bindAddress = "1.2.3.4";
    String membershipBindAddress = "4.3.2.1";
    assertThat(j.getAddressForUDPBinding(bindAddress, membershipBindAddress))
        .isEqualTo(membershipBindAddress);
  }

  @Test
  public void getAddressForUDPBinding_ReturnsLocalAddressWhenBindAddressIsIPv4WildcardAndMembershipBindAddressIsEmpty()
      throws MembershipConfigurationException, UnknownHostException {
    JGroupsMessenger j = new JGroupsMessenger();
    String bindAddress = "0.0.0.0";
    String membershipBindAddress = "";
    String localHostAddress = LocalHostUtil.getLocalHost().getHostAddress();
    assertThat(j.getAddressForUDPBinding(bindAddress, membershipBindAddress))
        .isEqualTo(localHostAddress);
  }

  @Test
  public void getAddressForUDPBinding_ThrowsExceptionIfMembershipBindAddressIsIPv4WildcardAddress() {
    JGroupsMessenger j = new JGroupsMessenger();
    String membershipBindAddress = "0.0.0.0";
    assertThatExceptionOfType(MembershipConfigurationException.class).isThrownBy(() -> {
      j.getAddressForUDPBinding("", membershipBindAddress);
    })
        .withMessage(
            "'membership-bind-address' cannot be a wildcard address. JGroups UDP protocol requires a bind address.");
  }

  @Test
  public void getAddressForUDPBinding_ThrowsExceptionIfMembershipBindAddressIsIPv6WildcardAddress() {
    JGroupsMessenger j = new JGroupsMessenger();
    String membershipBindAddress = "::";
    assertThatExceptionOfType(MembershipConfigurationException.class).isThrownBy(() -> {
      j.getAddressForUDPBinding("", membershipBindAddress);
    })
        .withMessage(
            "'membership-bind-address' cannot be a wildcard address. JGroups UDP protocol requires a bind address.");
  }
}
