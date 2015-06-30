/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import java.util.Properties;

import org.apache.logging.log4j.Logger;

import util.TestException;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * A little class for testing the {@link LocalDistributionManager}
 */
public class LDM {

  private static final Logger logger = LogService.getLogger();
  
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.setProperty("locators", "localhost[31576]");
    props.setProperty("mcastPort", "0");
    props.setProperty("logLevel", "config");
    InternalDistributedSystem system = (InternalDistributedSystem)
      DistributedSystem.connect(props);
    DM dm = system.getDistributionManager();

    DistributionMessage message = new HelloMessage();
    dm.putOutgoing(message);

    system.getLogWriter().info("Waiting 5 seconds for message");

    try {
      Thread.sleep(5 * 1000);

    } catch (InterruptedException ex) {
      throw new TestException("interrupted");
    }

    system.disconnect();
  }

  static class HelloMessage extends SerialDistributionMessage {

    public HelloMessage() { }   // for Externalizable
    @Override
    public void process(DistributionManager dm) {
      logger.fatal("Hello World");
    }
    public int getDSFID() {
      return NO_FIXED_ID;
    }
  }

}
