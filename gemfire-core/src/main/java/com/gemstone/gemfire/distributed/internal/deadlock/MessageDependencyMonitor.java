/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.deadlock;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * 
 * This class keeps track of the information we need
 * to determine which threads in the receiver system a reply
 * processor thread is waiting for.
 * 
 * This will allow us to programatically discover deadlocks.
 * @author dsmith
 *
 */
public class MessageDependencyMonitor implements DependencyMonitor {
  private final UnsafeThreadLocal<ReplyProcessor21> waitingProcessors = new UnsafeThreadLocal<ReplyProcessor21>();
  private final UnsafeThreadLocal<MessageWithReply> processingMessages= new UnsafeThreadLocal<MessageWithReply>();
  
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
    if(message instanceof MessageWithReply) {
      INSTANCE.processingMessages.set((MessageWithReply) message);
    }
  }
  
  public static void doneProcessing(DistributionMessage message) {
    if(message instanceof MessageWithReply) {
      INSTANCE.processingMessages.set(null);
    }
  }

  public Set<Dependency<Thread, Serializable>> getBlockedThreads(Thread[] allThreads) {
    
    InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
    if(system == null) {
      return Collections.emptySet();
    }
    InternalDistributedMember myId = system.getDistributedMember();
    
    Set<Dependency<Thread, Serializable>> blockedThreads = new HashSet<Dependency<Thread, Serializable>>();
    for(Thread thread : allThreads) {
      ReplyProcessor21 processor = waitingProcessors.get(thread);
      if(processor != null && processor.getProcessorId() > 0) {
        blockedThreads.add(new Dependency<Thread, Serializable>(thread, new MessageKey(myId, processor.getProcessorId())));
      }
    }
    return blockedThreads;
  }

  public Set<Dependency<Serializable, Thread>> getHeldResources(Thread[] allThreads) {
    Set<Dependency<Serializable, Thread>> heldResources = new HashSet<Dependency<Serializable, Thread>>();
    
    for(Thread thread : allThreads) {
      MessageWithReply message = processingMessages.get(thread);
      if(message != null && message.getProcessorId() > 0) {
        heldResources.add(new Dependency<Serializable, Thread>(new MessageKey(message.getSender(), message.getProcessorId()), thread));
      }
    }
    return heldResources;
  }
  
  private static class MessageKey implements Serializable {

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
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof MessageKey))
        return false;
      MessageKey other = (MessageKey) obj;
      if (myId == null) {
        if (other.myId != null)
          return false;
      } else if (!myId.equals(other.myId))
        return false;
      if (processorId != other.processorId)
        return false;
      return true;
    }
    
    @Override
    public String toString() {
      return "MessageFrom(" + myId + ", " + processorId + ")"; 
    }
  }
}
