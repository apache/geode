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
package org.apache.geode.distributed.internal;

//import org.apache.geode.*;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.Assert;
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
