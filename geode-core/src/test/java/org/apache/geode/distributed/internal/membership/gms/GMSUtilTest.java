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

package org.apache.geode.distributed.internal.membership.gms;

import static org.apache.geode.distributed.internal.membership.gms.GMSUtil.parseLocators;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.distributed.internal.membership.gms.membership.HostAddress;

@RunWith(JUnitParamsRunner.class)
public class GMSUtilTest {

  static final int PORT = 1234; // any old port--no need to have anything actually bound here

  static final String RESOLVEABLE_LOOPBACK_HOST = "127.0.0.1"; // loopback addy

  static final String RESOLVEABLE_NON_LOOPBACK_HOST = "1.1.1.1";

  static final String UNRESOLVEABLE_HOST = "not-localhost-937c64aa"; // some FQDN that does not


  @Test
  public void resolveableLoopBackAddress() {
    assertThat(
        parseLocators(RESOLVEABLE_LOOPBACK_HOST + "[" + PORT + "]",
            InetAddress.getLoopbackAddress()))
                .contains(
                    new HostAddress(new InetSocketAddress(RESOLVEABLE_LOOPBACK_HOST, PORT),
                        RESOLVEABLE_LOOPBACK_HOST));
  }

  @Test
  public void resolveableNonLoopBackAddress() {
    assertThatThrownBy(
        () -> parseLocators(RESOLVEABLE_NON_LOOPBACK_HOST + "[" + PORT + "]",
            InetAddress.getLoopbackAddress()))
                .isInstanceOf(MembershipConfigurationException.class)
                .hasMessageContaining("does not have a local address");
  }

  @Test
  public void unresolveableAddress() {
    assertThatThrownBy(
        () -> parseLocators(UNRESOLVEABLE_HOST + "[" + PORT + "]",
            InetAddress.getLoopbackAddress()))
                .isInstanceOf(MembershipConfigurationException.class)
                .hasMessageContaining("unknown address or FQDN: " + UNRESOLVEABLE_HOST);
  }

  @Test
  @Parameters({"1234", "0"})
  public void validPortSpecified(final int validPort) {
    final String locatorsString = RESOLVEABLE_LOOPBACK_HOST + "[" + validPort + "]";
    assertThat(parseLocators(locatorsString, InetAddress.getLoopbackAddress()))
        .contains(
            new HostAddress(new InetSocketAddress(RESOLVEABLE_LOOPBACK_HOST, validPort),
                RESOLVEABLE_LOOPBACK_HOST));
  }

  @Test
  @Parameters({"[]", "1234]", "[1234", ":1234", ""})
  public void malformedPortSpecification(final String portSpecification) {
    final String locatorsString = RESOLVEABLE_LOOPBACK_HOST + portSpecification;
    assertThatThrownBy(
        () -> parseLocators(locatorsString, InetAddress.getLoopbackAddress()))
            .isInstanceOf(MembershipConfigurationException.class)
            .hasMessageContaining("malformed port specification: " + locatorsString);
  }

  @Test
  @Parameters({"host@127.0.0.1[1234]", "host:127.0.0.1[1234]"})
  public void validHostSpecified(final String locatorsString) {
    assertThat(parseLocators(locatorsString, (InetAddress) null))
        .contains(
            new HostAddress(new InetSocketAddress("127.0.0.1", 1234), "127.0.0.1"));
  }

  @Test
  @Parameters({"server1@fdf0:76cf:a0ed:9449::5[12233]", "fdf0:76cf:a0ed:9449::5[12233]"})
  public void validIPV6AddySpecified(final String locatorsString) {
    assertThat(parseLocators(locatorsString, (InetAddress) null))
        .contains(
            new HostAddress(new InetSocketAddress("fdf0:76cf:a0ed:9449::5", 12233),
                "fdf0:76cf:a0ed:9449::5"));
  }

}
