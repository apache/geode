package com.gemstone.gemfire.cache.query.cq.dunit;

import java.util.Properties;

import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.dunit.HelperTestCase;
import com.gemstone.gemfire.internal.AvailablePortHelper;

import dunit.AsyncInvocation;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

public class CqStateDUnitTest extends HelperTestCase {

  
  public CqStateDUnitTest(String name) {
    super(name);
  }
  
  public void testNothingBecauseBug51953() {
    // remove when bug #51953 is fixed
  }
  
  // this test is disabled due to a 25% failure rate in
  // CI testing.  See internal ticket #52229
  public void disabledtestBug51222() throws Exception {
    //The client can log this when the server shuts down.
    addExpectedException("Could not find any server");
    addExpectedException("java.net.ConnectException");
    final String cqName = "theCqInQuestion";
    final String regionName = "aattbbss";
    final Host host = Host.getHost(0);
    VM serverA = host.getVM(1);
    VM serverB = host.getVM(2);
    VM client = host.getVM(3);
    
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    startCacheServer(serverA, ports[0], getAuthenticatedServerProperties());
    createReplicatedRegion(serverA, regionName, null);

    final String host0 = getServerHostName(serverA.getHost());
    startClient(client, new VM[]{ serverA, serverB }, ports, 1, getClientProperties());
    createCQ(client, cqName, "select * from /"+ regionName, null);
    
    //create the cacheserver but regions must be present first or else cq execute will fail with no region found
    createCacheServer(serverB, ports[1], getServerProperties(0));
    createReplicatedRegion(serverB, regionName, null);
    startCacheServers(serverB);
    
    AsyncInvocation async = executeCQ(client, cqName);
    DistributedTestCase.join(async, 10000, getLogWriter());

    Boolean clientRunning = (Boolean) client.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        final CqQuery cq = getCache().getQueryService().getCq(cqName);
        waitForCriterion(new WaitCriterion() {
          @Override
          public boolean done() {
            return cq.getState().isRunning();
          }
          @Override
          public String description() {
            return "waiting for Cq to be in a running state: " + cq;
          }
        },  30000, 1000, false);
        return cq.getState().isRunning();
      }
    });
    assertTrue("Client was not running", clientRunning);
    
    //hope that server 2 comes up before num retries is exhausted by the execute cq command
    //hope that the redundancy satisfier sends message and is executed after execute cq has been executed
    //This is the only way bug 51222 would be noticed
    //verify that the cq on the server is still in RUNNING state;
    Boolean isRunning = (Boolean) serverB.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        CqQuery cq = getCache().getQueryService().getCqs()[0];
        return cq.getState().isRunning();
      }
      
    });
    
    assertTrue("Cq was not running on server" , isRunning);
  }
  
  public Properties getAuthenticatedServerProperties() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("security-client-accessor",
        "com.gemstone.gemfire.cache.query.dunit.CloseCacheAuthorization.create");
    props.put("security-client-accessor-pp",
        "com.gemstone.gemfire.cache.query.dunit.CloseCacheAuthorization.create");
    props.put("security-client-authenticator",
        "templates.security.DummyAuthenticator.create");
    return props;
  }
  
  public Properties getServerProperties() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    return props;
  }
  
  public Properties getClientProperties() {
    Properties props = new Properties();
    props.put("security-client-auth-init",
        "templates.security.UserPasswordAuthInit.create");
    
    props.put("security-username", "root");
    props.put("security-password", "root");
    return props;
  }  
  
}

