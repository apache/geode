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
package org.apache.geode.distributed.internal.deadlock;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

/**
 *
 * This class keeps track of the information we need to determine which threads in the receiver
 * system a reply processor thread is waiting for.
 *
 * This will allow us to programatically discover deadlocks.
 *
 */
public class MessageDependencyMonitor implements DependencyMonitor {
  private final UnsafeThreadLocal<ReplyProcessor21> waitingProcessors =
      new UnsafeThreadLocal<ReplyProcessor21>();
  private final UnsafeThreadLocal<MessageWithReply> processingMessages =
      new UnsafeThreadLocal<MessageWithReply>();

  @MakeNotStatic
  public static final MessageDependencyMonitor INSTANCE;

  static {
    INSTANCE = new MessageDependencyMonitor();
    DependencyMonitorManager.addMonitor(INSTANCE);
  }

  public static void waitingForReply(ReplyProcessor21 reply) {
    INSTANCE.waitingProcessors.set(reply);
  }

  public static void doneWaiting(ReplyProcessor21 reply) {
    INSTANCE.waitingProcessors.set(null);
  }

  public static void processingMessage(DistributionMessage message) {
    if (message instanceof MessageWithReply) {
      INSTANCE.processingMessages.set((MessageWithReply) message);
    }
  }

  public static void doneProcessing(DistributionMessage message) {
    if (message instanceof MessageWithReply) {
      INSTANCE.processingMessages.set(null);
    }
  }

  @Override
  public Set<Dependency<Thread, Serializable>> getBlockedThreads(Thread[] allThreads) {

    InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
    if (system == null) {
      return Collections.emptySet();
    }
    InternalDistributedMember myId = system.getDistributedMember();

    Set<Dependency<Thread, Serializable>> blockedThreads =
        new HashSet<Dependency<Thread, Serializable>>();
    for (Thread thread : allThreads) {
      ReplyProcessor21 processor = waitingProcessors.get(thread);
      if (processor != null && processor.getProcessorId() > 0) {
        blockedThreads.add(new Dependency<Thread, Serializable>(thread,
            new MessageKey(myId, processor.getProcessorId())));
      }
    }
    return blockedThreads;
  }

  @Override
  public Set<Dependency<Serializable, Thread>> getHeldResources(Thread[] allThreads) {
    Set<Dependency<Serializable, Thread>> heldResources =
        new HashSet<Dependency<Serializable, Thread>>();

    for (Thread thread : allThreads) {
      MessageWithReply message = processingMessages.get(thread);
      if (message != null && message.getProcessorId() > 0) {
        heldResources.add(new Dependency<Serializable, Thread>(
            new MessageKey(message.getSender(), message.getProcessorId()), thread));
      }
    }
    return heldResources;
  }

  public static class MessageKey implements Serializable {

    private static final long serialVersionUID = 414781046295505260L;

    private final InternalDistributedMember myId;
    private final int processorId;

    public MessageKey(InternalDistributedMember myId, int processorId) {
      super();
      this.myId = myId;
      this.processorId = processorId;
    }

    public InternalDistributedMember getMyId() {
      return myId;
    }

    public int getProcessorId() {
      return processorId;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((myId == null) ? 0 : myId.hashCode());
      result = prime * result + processorId;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof MessageKey)) {
        return false;
      }
      MessageKey other = (MessageKey) obj;
      if (myId == null) {
        if (other.myId != null) {
          return false;
        }
      } else if (!myId.equals(other.myId)) {
        return false;
      }
      return processorId == other.processorId;
    }

    @Override
    public String toString() {
      return "MessageFrom(" + myId + ", " + processorId + ")";
    }
  }
}
