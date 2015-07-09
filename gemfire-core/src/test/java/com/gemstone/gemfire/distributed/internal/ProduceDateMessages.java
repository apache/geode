/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

//import com.gemstone.gemfire.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.Assert;
import java.util.Date;
import java.util.Properties;

/**
 * A little program that periodically produces {@link DateMessage}s.
 */
public class ProduceDateMessages {

  public static void main(String[] args) throws InterruptedException {
    InternalDistributedSystem system = (InternalDistributedSystem)
      DistributedSystem.connect(new Properties());
    DM dm = system.getDistributionManager();
    System.out.println("Got DM: " + dm);

    while (true) {
      DateMessage message = new DateMessage();

      // Make sure that message state was reset
      Assert.assertTrue(message.getDate() == null);
      Assert.assertTrue(message.getRecipients() == null);
      Assert.assertTrue(message.getSender() == null);

      message.setRecipient(DistributionMessage.ALL_RECIPIENTS);
      message.setDate(new Date());

      System.out.println("Produced: " + message);
      dm.putOutgoing(message);
      Thread.sleep(1000);
    }
  }

}
