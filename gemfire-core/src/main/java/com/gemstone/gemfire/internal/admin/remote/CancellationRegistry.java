/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */   
   
package com.gemstone.gemfire.internal.admin.remote;

import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This class provides a way for a {@link CancellationMessage} to find its prey.
 * An {@link AdminRequest} that implements {@link Cancellable} should register
 * with this class before doing any work, and deregister just before returning
 * it's response.
 */
public class CancellationRegistry {
  private static CancellationRegistry internalRef;
  private Map map = new HashMap();
  
  private CancellationRegistry() {}

  public synchronized static CancellationRegistry getInstance() {
    if (internalRef == null) {
      internalRef = new CancellationRegistry();
    }
    return internalRef;
  }

  public synchronized void cancelMessage(InternalDistributedMember console, int msgId) {
    Key key = new Key(console, msgId);
    AdminRequest msg = (AdminRequest)map.get(key);
    if (msg instanceof Cancellable) {
      ((Cancellable)msg).cancel();
    }
  }

  public synchronized void registerMessage(AdminRequest msg) {
    Key key = new Key(msg.getSender(), msg.getMsgId());
    map.put(key, msg);
  }

  public synchronized void deregisterMessage(AdminRequest msg) {
    Key key = new Key(msg.getSender(), msg.getMsgId());
    map.remove(key);
  }

  /////// Inner Classes ////////////////////////////////////////

  static private class Key {
    private final InternalDistributedMember console;
    private final int msgId;

    public Key(InternalDistributedMember console, int msgId) {
      if (console == null) {
        throw new NullPointerException(LocalizedStrings.CancellationRegistry_NULL_CONSOLE.toLocalizedString());
      }

      this.console = console;
      this.msgId = msgId;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (other instanceof Key) {
        Key toTest = (Key)other;
        return (toTest.console.equals(this.console) &&
                toTest.msgId == this.msgId);
      }
      return false;
    }

    @Override
    public int hashCode() {
      int result = 17;
      result = 37 * result + msgId;
      result = 37 * result + console.hashCode();
      return result;
    }
  }
}
