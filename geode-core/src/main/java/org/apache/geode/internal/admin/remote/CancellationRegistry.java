/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.admin.remote;

import java.util.HashMap;
import java.util.Map;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 * This class provides a way for a {@link CancellationMessage} to find its prey. An
 * {@link AdminRequest} that implements {@link Cancellable} should register with this class before
 * doing any work, and deregister just before returning it's response.
 */
public class CancellationRegistry {
  private static CancellationRegistry internalRef;
  private Map map = new HashMap();

  private CancellationRegistry() {}

  public static synchronized CancellationRegistry getInstance() {
    if (internalRef == null) {
      internalRef = new CancellationRegistry();
    }
    return internalRef;
  }

  public synchronized void cancelMessage(InternalDistributedMember console, int msgId) {
    Key key = new Key(console, msgId);
    AdminRequest msg = (AdminRequest) map.get(key);
    if (msg instanceof Cancellable) {
      ((Cancellable) msg).cancel();
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

  private static class Key {
    private final InternalDistributedMember console;
    private final int msgId;

    public Key(InternalDistributedMember console, int msgId) {
      if (console == null) {
        throw new NullPointerException(
            "Null Console!");
      }

      this.console = console;
      this.msgId = msgId;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other)
        return true;
      if (other instanceof Key) {
        Key toTest = (Key) other;
        return (toTest.console.equals(this.console) && toTest.msgId == this.msgId);
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
