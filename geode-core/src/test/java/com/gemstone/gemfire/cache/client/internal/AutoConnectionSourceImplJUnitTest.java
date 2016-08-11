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
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.NoAvailableLocatorsException;
import com.gemstone.gemfire.cache.client.SubscriptionNotEnabledException;
import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionResponse;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorListResponse;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.PoolStatHelper;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpHandler;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpServer;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.PoolStats;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 */
@SuppressWarnings("deprecation")
@Category(IntegrationTest.class)
public class AutoConnectionSourceImplJUnitTest {
  
  private Cache cache;
  private int port;
  private FakeHandler handler;
  private FakePool pool = new FakePool();
  private AutoConnectionSourceImpl source;
  private TcpServer server;
  private ScheduledExecutorService background;
  private PoolStats poolStats;
  
  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    DistributedSystem ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);
    poolStats = new PoolStats(ds, "pool");
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    
    handler = new FakeHandler();
    ArrayList responseLocators = new ArrayList();
    responseLocators.add(new ServerLocation(InetAddress.getLocalHost().getHostName(), port));
    handler.nextLocatorListResponse = new LocatorListResponse(responseLocators, false);
    
    //very irritating, the SystemTimer requires having a distributed system
    Properties properties = new Properties();
    properties.put(MCAST_PORT, "0");
    properties.put(LOCATORS, "");
    background = Executors.newSingleThreadScheduledExecutor(); 
    
    List/*<InetSocketAddress>*/ locators = new ArrayList();
    locators.add(new InetSocketAddress(InetAddress.getLocalHost(), port));
    source = new AutoConnectionSourceImpl(locators, "", 60 * 1000);
    source.start(pool);
  }
  
  @After
  public void tearDown() {
    background.shutdownNow();
    try {
      cache.close();
    } catch (Exception e) {
      // do nothing
    }
    try {
      if(server != null && server.isAlive()) {
        try {
          TcpClient.stop(InetAddress.getLocalHost(), port);
        } catch ( ConnectException ignore ) {
          // must not be running
        }
        server.join(60 * 1000);
      }
    } catch(Exception e) {
      //do nothing
    }
    
    try {
      InternalDistributedSystem.getAnyInstance().disconnect();
    } catch(Exception e) {
      //do nothing
    }
  }
  
  @Test
  public void testNoRespondingLocators() {
    try {
      source.findServer(null);
      fail("Should have gotten a NoAvailableLocatorsException");
    } catch(NoAvailableLocatorsException expected) {
      //do nothing
    }
  }
  
  @Test
  public void testNoServers() throws Exception {
    startFakeLocator();
    handler.nextConnectionResponse = new ClientConnectionResponse(null);
    assertEquals(null, source.findServer(null));
  }
  
  @Test
  public void testDiscoverServers() throws Exception {
    startFakeLocator();
    ServerLocation loc1 = new ServerLocation("localhost", 4423);
    handler.nextConnectionResponse = new ClientConnectionResponse(loc1);
    assertEquals(loc1, source.findServer(null));
  }
  
  @Test
  public void testDiscoverLocators() throws Exception {
    startFakeLocator();
    int secondPort = AvailablePortHelper.getRandomAvailableTCPPort();
    TcpServer server2 = new TcpServer(secondPort, InetAddress.getLocalHost(), null, null, handler, new FakeHelper(), Thread.currentThread().getThreadGroup(), "tcp server");
    server2.start();
    
    try {
      ArrayList locators = new ArrayList();
      locators.add(new ServerLocation(InetAddress.getLocalHost().getHostName(), secondPort));
      handler.nextLocatorListResponse = new LocatorListResponse(locators,false);
      Thread.sleep(500);
      try {
        TcpClient.stop(InetAddress.getLocalHost(), port);
      } catch ( ConnectException ignore ) {
        // must not be running
      }
      server.join(1000);
      
      ServerLocation server1 = new ServerLocation("localhost", 10); 
      handler.nextConnectionResponse = new ClientConnectionResponse(server1);
      assertEquals(server1, source.findServer(null));
    } finally {
      try {
        TcpClient.stop(InetAddress.getLocalHost(), secondPort);
      } catch ( ConnectException ignore ) {
        // must not be running
      }
      server.join(60 * 1000);
    }
  }
  
  private void startFakeLocator() throws UnknownHostException, IOException, InterruptedException {
    server = new TcpServer(port, InetAddress.getLocalHost(), null, null, handler, new FakeHelper(), Thread.currentThread().getThreadGroup(), "Tcp Server" );
    server.start();
    Thread.sleep(500);
  }
  
  protected static class FakeHandler implements TcpHandler {
    protected volatile ClientConnectionResponse nextConnectionResponse;
    protected volatile LocatorListResponse nextLocatorListResponse;;

    
    public void init(TcpServer tcpServer) {
    }

    public Object processRequest(Object request) throws IOException {
      if(request instanceof ClientConnectionRequest) {
        return nextConnectionResponse;
      }
      else {
        return nextLocatorListResponse;
      }
    }

    public void shutDown() {
    }
    public void endRequest(Object request,long startTime) { }
    public void endResponse(Object request,long startTime) { }
    public void restarting(DistributedSystem ds, GemFireCache cache, SharedConfiguration sharedConfig) { }

  }
  
  public static class FakeHelper implements PoolStatHelper {

    public void endJob() {
    }

    public void startJob() {
    }
    
  }
  
  public class FakePool implements InternalPool {
      public String getPoolOrCacheCancelInProgress() {return null; }

    @Override
    public boolean getKeepAlive() {
      return false;
    }

    public EndpointManager getEndpointManager() {
        return null;
      }

      public String getName() {
        return null;
      }
      
      public PoolStats getStats() {
        return poolStats;
      }

      public void destroy() {
        
      }

      public void detach() {
      }
      
      public void destroy(boolean keepAlive) {
        
      }
      
      public boolean isDurableClient() {
        return false;
      }
      
      public boolean isDestroyed() { 
        return false;
      }
    public int getFreeConnectionTimeout() {return 0;}
    public int getLoadConditioningInterval() {return 0;}
    public int getSocketBufferSize() {return 0;}
    public int getReadTimeout() {return 0;}
    public int getConnectionsPerServer() {return 0;}
    public boolean getThreadLocalConnections() {return false;}
    public boolean getSubscriptionEnabled() {return false;}
    public boolean getPRSingleHopEnabled() {return false;}
    public int getSubscriptionRedundancy() {return 0;}
    public int getSubscriptionMessageTrackingTimeout() {return 0;}
    public String getServerGroup() {return "";}
    public List/*<InetSocketAddress>*/ getLocators() {return new ArrayList();}
    public List/*<InetSocketAddress>*/ getServers() {return new ArrayList();}
    public void releaseThreadLocalConnection() {}
    public ConnectionStats getStats(ServerLocation location) { return null; }
    public boolean getMultiuserAuthentication() {return false;}
    public long getIdleTimeout() {
      return 0;
    }

    public int getMaxConnections() {
      return 0;
    }

    public int getMinConnections() {
      return 0;
    }

    public long getPingInterval() {
      return 100;
    }

    public int getStatisticInterval() {
      return -1;
    }

    public int getRetryAttempts() {
      return 0;
    }

    public Object execute(Op op) {
      return null;
    }

    public Object executeOn(ServerLocation server, Op op) {
      return null;
    }
    public Object executeOn(ServerLocation server, Op op, boolean accessed,boolean onlyUseExistingCnx) {
      return null;
    }

    public Object executeOnPrimary(Op op) {
      return null;
    }

    public Map getEndpointMap() {
      return null;
    }

    public ScheduledExecutorService getBackgroundProcessor() {
      return background;
    }

    public Object executeOn(Connection con, Op op) {
      return null;
    }
    public Object executeOn(Connection con, Op op, boolean timeoutFatal) {
      return null;
    }

    public RegisterInterestTracker getRITracker() {
      return null;
    }

    public int getSubscriptionAckInterval() {
      return 0;
    }

    public Object executeOnQueuesAndReturnPrimaryResult(Op op) {
      return null;
    }

    public CancelCriterion getCancelCriterion() {
      return new CancelCriterion() {

        public String cancelInProgress() {
          return null;
        }

        public RuntimeException generateCancelledException(Throwable e) {
          return null;
        }
        
      };
    }

    public void executeOnAllQueueServers(Op op)
        throws NoSubscriptionServersAvailableException, SubscriptionNotEnabledException {
      
    }

    public Object execute(Op op, int retryAttempts) {
      return null;
    }
    
    public QueryService getQueryService() {
      return null;
    }
    
    public int getPendingEventCount() {
      return 0;
    }
    
    public RegionService createAuthenticatedCacheView(Properties properties){
      return null;
    }
    public void setupServerAffinity(boolean allowFailover) {
    }
    public void releaseServerAffinity() {
    }
    public ServerLocation getServerAffinityLocation() {
      return null;
    }
    public void setServerAffinityLocation(ServerLocation serverLocation) {
    }
  }
}
