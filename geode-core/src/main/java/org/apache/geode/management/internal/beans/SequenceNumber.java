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
package org.apache.geode.management.internal.beans;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Class to give a consistent sequence number to notifications
 * 
 * 
 */
public class SequenceNumber {

  /** Sequence number for resource related notifications **/
  private static AtomicLong sequenceNumber = new AtomicLong(1);

  public static long next() {
    long retVal = sequenceNumber.incrementAndGet();

    if (retVal == Long.MAX_VALUE || retVal < 0) {//retVal <0 is checked for cases where other threads might have incremented sequenceNumber beyond Long.MAX_VALUE
      
      synchronized (SequenceNumber.class) {
        long currentValue = sequenceNumber.get();
        if (currentValue == Long.MAX_VALUE || retVal < 0) {
          sequenceNumber.set(1);
          retVal = sequenceNumber.get();
        } else {
          retVal = currentValue;
        }
      }
    }
    return retVal;
  }

}
