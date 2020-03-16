package org.apache.geode.cache.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;

public class SocketFactoryDUnitTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();
  private int locatorPort;
  private int serverPort;

  @Before
  public void createCluster() {
    locatorPort = cluster.startLocatorVM(0).getPort();
    serverPort = cluster.startServerVM(1, locatorPort).getPort();
  }

  @Test
  public void customSocketFactoryUsedForLocators() throws IOException {
    ClientCache client = new ClientCacheFactory()
        // Add a locator with the wrong hostname
        .addPoolLocator("notarealhostname", locatorPort)
        // Set a socket factory that switches the hostname back
        .setPoolSocketFactory(new ChangeHostSocketFactory("localhost"))
        .create();

    // Verify the socket factory switched the hostname so we can connect
    verifyConnection(client);
  }

  @Test
  public void customSocketFactoryUsedForServers() {
    ClientCache client = new ClientCacheFactory()
        // Add a locator with the wrong hostname
        .addPoolServer("notarealhostname", serverPort)
        // Set a socket factory that switches the hostname back
        .setPoolSocketFactory(new ChangeHostSocketFactory("localhost"))
        .create();


    // Verify the socket factory switched the hostname so we can connect
    verifyConnection(client);
  }

  private void verifyConnection(ClientCache client) {
    // Verify connectivity with servers
    Object functionResult =
        FunctionService.onServers(client).execute(new TestFunction()).getResult();

    assertThat(functionResult).isEqualTo(Arrays.asList("test"));
  }


  public static class TestFunction implements Function, DataSerializable {
    @Override
    public void toData(DataOutput out) throws IOException {

    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {

    }

    @Override
    public void execute(FunctionContext context) {
      context.getResultSender().lastResult("test");

    }
  }


  private static class ChangeHostSocketFactory implements SocketFactory {

    private final String newHost;

    private ChangeHostSocketFactory(String newHost) {
      this.newHost = newHost;
    }

    @Override
    public Socket createSocket() throws IOException {
      return new ChangeHostSocket();
    }

    private class ChangeHostSocket extends Socket {

      @Override
      public void connect(SocketAddress endpoint, int timeout) throws IOException {
        InetSocketAddress oldEndpoint = (InetSocketAddress) endpoint;
        super.connect(new InetSocketAddress(newHost, oldEndpoint.getPort()), timeout);
      }
    }
  }

}
