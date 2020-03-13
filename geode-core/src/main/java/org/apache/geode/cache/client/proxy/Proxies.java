package org.apache.geode.cache.client.proxy;

import org.apache.geode.cache.client.SocketFactory;

public class Proxies {

  /**
   * Create a {@link SocketFactory} that will connect a geode client through
   * the configured SNI proxy.
   *
   *
   * @param hostname the hostname of the sni proxy
   * @param port the port of the sni proxy
   */
  public static SocketFactory sni(String hostname, int port) {
    return new SniSocketFactory(hostname, port);
  }
}
