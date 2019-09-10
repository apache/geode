package org.apache.geode.distributed.internal.membership.gms;

import static org.apache.geode.distributed.internal.membership.gms.GMSUtil.parseLocators;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.function.BiFunction;

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
  static final String RESOLVEABLE_HOST_AND_PORT = RESOLVEABLE_HOST + "[" + PORT + "]";

  static final String UNRESOLVEABLE_HOST = "not-localhost-937c64aa"; // some FQDN that does not
                                                                     // exist
  static final String UNRESOLVEABLE_HOST_AND_PORT = UNRESOLVEABLE_HOST + "[" + PORT + "]";

  @Test
  public void resolveableLoopBackAddress() {
    assertThat(
        parseLocators(RESOLVEABLE_HOST_AND_PORT, InetAddress.getLoopbackAddress()))
            .contains(
                new HostAddress(new InetSocketAddress(RESOLVEABLE_HOST, PORT), RESOLVEABLE_HOST));
  }

  @Test
  public void resolveableNonLoopBackAddress() {

    final InetAddress nonLoopbackAddy = mock(InetAddress.class);
    when(nonLoopbackAddy.isLoopbackAddress()).thenReturn(false);

    final InetSocketAddress resolveableNonLoopbackSocketAddress = mock(InetSocketAddress.class);
    when(resolveableNonLoopbackSocketAddress.getAddress()).thenReturn(nonLoopbackAddy);

    final BiFunction<String, Integer, InetSocketAddress> socketAddyFactory =
        (host, port) -> resolveableNonLoopbackSocketAddress;

    assertThatThrownBy(
        () -> parseLocators(
            "fake@123.4.5.6[7890]",
            InetAddress.getLoopbackAddress(),
            socketAddyFactory))
                .isInstanceOf(GemFireConfigException.class)
                .hasMessageContaining("does not have a local address");
  }

  @Test
  public void unresolveableAddress() {
    assertThatThrownBy(
        () -> parseLocators(UNRESOLVEABLE_HOST_AND_PORT, InetAddress.getLoopbackAddress()))
            .isInstanceOf(GemFireConfigException.class)
            .hasMessageContaining("unknown address or FQDN: " + UNRESOLVEABLE_HOST);
  }

  @Test
  @Parameters({"1234", "0"})
  public void validPortSpecified(final int validPort) {
    final String locatorsString = RESOLVEABLE_HOST + "[" + validPort + "]";
    assertThat(parseLocators(locatorsString, InetAddress.getLoopbackAddress()))
        .contains(
            new HostAddress(new InetSocketAddress(RESOLVEABLE_HOST, validPort), RESOLVEABLE_HOST));
  }

  @Test
  @Parameters({"[]", "1234]", "[1234", ":1234", ""})
  public void malformedPortSpecification(final String portSpecification) {
    final String locatorsString = RESOLVEABLE_HOST + portSpecification;
    assertThatThrownBy(
        () -> parseLocators(locatorsString, InetAddress.getLoopbackAddress()))
            .isInstanceOf(GemFireConfigException.class)
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
