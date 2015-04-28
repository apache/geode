/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Class to give a consistent sequence number to notifications
 * 
 * @author rishim
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
