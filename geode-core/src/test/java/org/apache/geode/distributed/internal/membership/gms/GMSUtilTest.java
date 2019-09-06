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

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.internal.membership.gms.membership.HostAddress;

@RunWith(JUnitParamsRunner.class)
public class GMSUtilTest {

  static final int PORT = 1234; // any old port--no need to have anything actually bound here

  static final String RESOLVEABLE_HOST = "127.0.0.1"; // loopback addy
  static final String RESOLVEABLE_AND_PORT = RESOLVEABLE_HOST + "[" + PORT + "]";

  static final String
      UNRESOLVEABLE_HOST = "not-localhost-937c64aa"; // some FQDN that does not exist
  static final String UNRESOLVEABLE_HOST_AND_PORT = UNRESOLVEABLE_HOST + "[" + PORT + "]";

  @Test
  public void resolveableAddress() {
    assertThat(
        parseLocators(RESOLVEABLE_AND_PORT, InetAddress.getLoopbackAddress()))
        .contains(new HostAddress(new InetSocketAddress(RESOLVEABLE_HOST, PORT), RESOLVEABLE_HOST));
  }

  @Test
  public void unresolveableAddress() {
    assertThatThrownBy(
        () -> parseLocators(UNRESOLVEABLE_HOST_AND_PORT, InetAddress.getLoopbackAddress()))
        .isInstanceOf(GemFireConfigException.class)
        .hasMessageContaining("unknown address or FQDN: " + UNRESOLVEABLE_HOST);
  }

  @Test
  @Parameters({"[]","1234]","[1234",":1234",""})
  public void malformedPortSpecification(final String portSpecification) {
    final String locatorsString = RESOLVEABLE_HOST + portSpecification;
    assertThatThrownBy(
        () -> parseLocators(locatorsString, InetAddress.getLoopbackAddress()))
        .isInstanceOf(GemFireConfigException.class)
        .hasMessageContaining("malformed or missing port specification: " + locatorsString);
  }

}