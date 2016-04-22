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
package com.gemstone.gemfire.distributed.internal;

import java.util.Properties;

import org.apache.logging.log4j.Logger;

import util.TestException;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * A little class for testing the local DistributionManager
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
