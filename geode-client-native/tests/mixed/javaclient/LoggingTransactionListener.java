/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaclient;

import com.gemstone.gemfire.cache.*;
import java.util.*;

/**
 * A GemFire <code>TransactionListener</code> that logs information
 * about the events it receives.
 *
 * @author GemStone Systems, Inc.
 * @since Brandywine
 */
public class LoggingTransactionListener extends LoggingCacheCallback
  implements TransactionListener {

  /**
   * Zero-argument constructor required for declarative cache XML
   * file. 
   */
  public LoggingTransactionListener() {
    super();
  }

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Logs information about a <code>TransactionEvent</code>
   *
   * @param kind
   *        The kind of event to be logged
   */
  protected void log(String kind, TransactionEvent event) {
    StringBuffer sb = new StringBuffer();
    sb.append(kind);
    sb.append(" in transaction ");
    sb.append(event.getTransactionId());
    sb.append("\n");

    Collection creates = event.getCreateEvents();
    sb.append(creates.size());
    sb.append(" Creation Events\n");
    for (Iterator iter = creates.iterator(); iter.hasNext(); ) {
      sb.append(format((EntryEvent) iter.next()));
    }
    sb.append("\n");
    
    Collection destroys = event.getDestroyEvents();
    sb.append(destroys.size());
    sb.append(" Destroy Events\n");
    for (Iterator iter = destroys.iterator(); iter.hasNext(); ) {
      sb.append(format((EntryEvent) iter.next()));
    }
    sb.append("\n");

    Collection invalidates = event.getInvalidateEvents();
    sb.append(invalidates.size());
    sb.append(" Invalidate Events\n");
    for (Iterator iter = invalidates.iterator(); iter.hasNext(); ) {
      sb.append(format((EntryEvent) iter.next()));
    }
    sb.append("\n");

    Collection puts = event.getPutEvents();
    sb.append(puts.size());
    sb.append(" Put Events\n");
    for (Iterator iter = puts.iterator(); iter.hasNext(); ) {
      sb.append(format((EntryEvent) iter.next()));
    }
    sb.append("\n");

    log(sb.toString(), event.getCache());
  }

  public void afterCommit(TransactionEvent event) {
    log("TransactionListener.afterCommit", event);
  }

  public void afterFailedCommit(TransactionEvent event) {
    log("TransactionListener.afterFailedCommit", event);
  }

  public void afterRollback(TransactionEvent event) {
    log("TransactionListener.afterRollback", event);
  }

}
