package com.gemstone.gemfire.cache;

import java.io.IOException;
import java.util.Set;

import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;
import com.gemstone.gemfire.cache30.CacheXmlTestCase;
import com.gemstone.gemfire.cache30.MyGatewayTransportFilter1;
import com.gemstone.gemfire.cache30.MyGatewayTransportFilter2;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;

public class CacheXml80GatewayDUnitTest extends CacheXmlTestCase {

  public CacheXml80GatewayDUnitTest(String name) {
    super(name);
  }

  protected String getGemFireVersion() {
    return CacheXml.VERSION_8_0;
  }
  
  public void testGatewayReceiverWithManualStartTRUE() throws CacheException{
    //getSystem();
    CacheCreation cache = new CacheCreation();
    
    GatewayReceiverFactory gatewayReceiverFactory = cache.createGatewayReceiverFactory();
    gatewayReceiverFactory.setBindAddress("");
    gatewayReceiverFactory.setStartPort(54321);
    gatewayReceiverFactory.setEndPort(54331);
    gatewayReceiverFactory.setMaximumTimeBetweenPings(2000);
    gatewayReceiverFactory.setSocketBufferSize(1500);
    gatewayReceiverFactory.setManualStart(true);
    GatewayTransportFilter myStreamfilter1 = new MyGatewayTransportFilter1();
    gatewayReceiverFactory.addGatewayTransportFilter(myStreamfilter1);
    GatewayTransportFilter myStreamfilter2 = new MyGatewayTransportFilter2();
    gatewayReceiverFactory.addGatewayTransportFilter(myStreamfilter2);
    GatewayReceiver receiver1 = gatewayReceiverFactory.create();
    try {
      receiver1.start();
    }
    catch (IOException e) {
      fail("Could not start GatewayReceiver");
    }
    testXml(cache);
    Cache c = getCache();
    assertNotNull(c);
    Set<GatewayReceiver> receivers = c.getGatewayReceivers();
    for(GatewayReceiver receiver : receivers){
      validateGatewayReceiver(receiver1, receiver);
    }
  }

  protected void validateGatewayReceiver(GatewayReceiver receiver1,
      GatewayReceiver gatewayReceiver){
    CacheXml70GatewayDUnitTest.validateGatewayReceiver(receiver1, gatewayReceiver);
    assertEquals(receiver1.isManualStart(), gatewayReceiver.isManualStart());
  }
}
