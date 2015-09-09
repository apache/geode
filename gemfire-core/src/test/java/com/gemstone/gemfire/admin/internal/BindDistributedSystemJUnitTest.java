/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.Socket;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminDistributedSystemFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.util.StopWatch;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests {@link com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl}.
 *
 * @author    Kirk Lund
 * @created   August 30, 2004
 * @since     3.5
 */
@SuppressWarnings("deprecation")
@Category(IntegrationTest.class)
public class BindDistributedSystemJUnitTest {

  private final static int RETRY_ATTEMPTS = 3;
  private final static int RETRY_SLEEP = 100;

  private DistributedSystem system;

  @After
  public void tearDown() {
    if (this.system != null) {
      this.system.disconnect();
    }
    this.system = null;
  }
  
//  public void testBindToAddressNull() throws Exception {
//    DistributedSystemFactory.bindToAddress(null);
//     todo...
//  }
//
//  public void testBindToAddressEmpty() throws Exception {
//    DistributedSystemFactory.bindToAddress("");
//     todo...
//  }

  @Test
  public void testBindToAddressLoopback() throws Exception {
    String bindTo = "127.0.0.1";
    // make sure bindTo is the loopback... needs to be later in test...
    assertEquals(true, InetAddressUtil.isLoopback(bindTo));

    Properties props = new Properties();
    props.setProperty(DistributionConfig.BIND_ADDRESS_NAME, bindTo);
    props.setProperty(DistributionConfig.START_LOCATOR_NAME,
        "localhost["+AvailablePortHelper.getRandomAvailableTCPPort()+"]");
    this.system = com.gemstone.gemfire.distributed.DistributedSystem.connect(
        props);
        
    assertEquals(true, this.system.isConnected());

    // Because of fix for bug 31409
    this.system.disconnect();

  }


}

