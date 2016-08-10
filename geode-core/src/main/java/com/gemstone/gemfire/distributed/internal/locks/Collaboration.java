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

package com.gemstone.gemfire.distributed.internal.locks;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Synchronization structure which allows multiple threads to lock the
 * structure. Implementation is fair: the next waiting thread will be
 * serviced.
 * <p>
 * Collaborating threads may jointly synchronize on this structure if they all
 * agree on the same topic of collaboration.
 * <p>
 * Threads that want to change the topic will wait until the current topic
 * has been released.
 *
 */
public class Collaboration {
  
  private static final Logger logger = LogService.getLogger();
  
  private final static Object NULL_TOPIC = null;
  
  /** The current topic of collaboration 
   *
   * guarded.By {@link #topicsQueue}
   */
  private volatile Topic currentTopic;
  
  /** Ordered queue of pending topics for collaboration */
  private final List topicsQueue = new ArrayList();
  
  /** Map of external topic to internal wrapper object (Topic) */
  private final Map topicsMap = new HashMap();
  
  private final CancelCriterion stopper;
  
  /** 
   * Constructs new stoppable instance of Collaboration which will heed an 
   * interrupt request if it is acceptable to the creator of the lock.
   */
  public Collaboration(CancelCriterion stopper) {
    this.stopper = stopper;
  }
  
  /** 
   * Acquire permission to participate in the collaboration. Returns 
   * immediately if topic matches the current topic. Otherwise, this will 
   * block until the Collaboration has been freed by the threads working
   * on the current topic. This call is interruptible.
   *
   * @param topicObject Object to collaborate on
   *
   * @throws InterruptedException if thread is interrupted
   */
  public void acquire(Object topicObject) throws InterruptedException {
    throw new UnsupportedOperationException(LocalizedStrings.Collaboration_NOT_IMPLEMENTED.toLocalizedString());
  }

  /**
   * Must be synchronized on this.topicsQueue... Asserts that thread is not 
   * reentering. 
   */
  private void assertNotRecursingTopic(Object topicObject) {
    Assert.assertTrue(false,
      Thread.currentThread() + " attempting to lock topic " + topicObject +
      " while locking topic " + this.currentTopic);
  }
  
  /** 
   * Acquire permission to participate in the collaboration. Returns 
   * immediately if topic matches the current topic. Otherwise, this will 
   * block until the Collaboration has been freed by the threads working
   * on the current topic. This call is uninterruptible.
   *
   * @param topic Object to collaborate on
   */
  public void acquireUninterruptibly(final Object topic) {
    Object topicObject = topic;
    if (topicObject == null) {
      topicObject = NULL_TOPIC;
    }
    
    Topic pendingTopic = null;
    synchronized (this.topicsQueue) {
      // if no topic then setup and return
      if (this.currentTopic == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Collaboration.acquireUninterruptibly: {}: no current topic, setting topic to {}", this.toString(), topicObject);
        }
        setCurrentTopic(new Topic(topicObject));
        this.currentTopic.addThread(Thread.currentThread());
        this.topicsMap.put(topicObject, this.currentTopic);
        return;
      }
      
      else if (isCurrentTopic(topicObject)) {
        //assertNotRecursingTopic(topicObject);
        if (logger.isDebugEnabled()) {
          logger.debug("Collaboration.acquireUninterruptibly: {}: already current topic: {}", this.toString(), topicObject);
        }
        this.currentTopic.addThread(Thread.currentThread());
        return;
      }
      
      else if (hasCurrentTopic(Thread.currentThread())) {
        assertNotRecursingTopic(topicObject);
      }
      
      // if other topic then add to pending topics and then wait
      else {
        pendingTopic = (Topic) this.topicsMap.get(topicObject);
        if (pendingTopic == null) {
          pendingTopic = new Topic(topicObject);
          this.topicsMap.put(topicObject, pendingTopic);
          this.topicsQueue.add(pendingTopic);
        }
        pendingTopic.addThread(Thread.currentThread());
        if (logger.isDebugEnabled()) {
          logger.debug("Collaboration.acquireUninterruptibly: {}: adding pendingTopic {}; current topic is {}", this.toString(), pendingTopic, this.currentTopic);
        }
      }
    } // synchronized
    // now await the topic change uninterruptibly...
    boolean interrupted = Thread.interrupted();
    try {
      awaitTopic(pendingTopic, false);
    }
    catch (InterruptedException e) { // LOST INTERRUPT
      interrupted = true;
      this.stopper.checkCancelInProgress(e);
    }
    finally {
      if (interrupted) Thread.currentThread().interrupt();
    }
  }
  
  private void setCurrentTopic(Topic topic) {
    synchronized (this.topicsQueue) {
      if (this.currentTopic != null) {
        synchronized (this.currentTopic) {
          this.currentTopic.setCurrentTopic(false);
          this.currentTopic.setOldTopic(true);
        }
      }
      if (topic != null) {
        synchronized (topic) {
          topic.setCurrentTopic(true);
          this.currentTopic = topic;
          if (logger.isDebugEnabled()) {
            logger.debug("Collaboration.setCurrentTopic: {}: new topic is {}", this.getIdentity(), topic);
          }
          this.currentTopic.notifyAll();
        }
      }
      else {
        if (logger.isDebugEnabled()) {
          logger.debug("Collaboration.setCurrentTopic: {} setting current topic to null", this.toString());
        }
        this.currentTopic = null;
      }
    } // synchronized
  }
  
  private void awaitTopic(Topic topic, boolean interruptible)
  throws InterruptedException {
    // wait while currentTopic exists and doesn't match my topic
    boolean isDebugEnabled = logger.isDebugEnabled();
    synchronized (topic) {
      while (!topic.isCurrentTopic()) {
        if (topic.isOldTopic()) {
          // warning: cannot call toString while under sync(topic)
          Assert.assertTrue(false, "[" + getIdentity() +
            ".awaitTopic] attempting to wait on old topic");
        }
        boolean interrupted = Thread.interrupted();
        try {
          // In order to examine the current topic, we would need to
          // lock the topicsQueue and then the topic, in that order.
          // No can do in this instance (wrong lock ordering) but we still want 
          // a sense of why we did the wait.
          Topic sniff = this.currentTopic;
          if (isDebugEnabled) {
            logger.debug("Collaboration.awaitTopic: {} waiting for topic {}; current topic probably {}, which may have a thread count of {}", getIdentity(), topic, sniff.toString(), sniff.threadCount());
          }
          topic.wait();
        }
        catch (InterruptedException e) {
          if (interruptible) throw e;
          interrupted = true;
          this.stopper.checkCancelInProgress(e);
        }
        finally {
          if (interrupted) Thread.currentThread().interrupt();
        }
      }
    }
    
    // remove this assertion after we're sure this class is working...
    /*Assert.assertTrue(isCurrentTopic(topic.getTopicObject()), 
        "Failed to make " + topic + " the topic for " + this);*/ 
  }
  
  /** 
   * Acquire permission to participate in the collaboration without waiting.
   *
   * @param topicObject Object to collaborate on
   * @return true if participation in the collaboration was acquired
   */
  public boolean tryAcquire(Object topicObject) {
    throw new UnsupportedOperationException(LocalizedStrings.Collaboration_NOT_IMPLEMENTED.toLocalizedString());
  }
      
  /** 
   * Acquire permission to participate in the collaboration; waits the
   * specified timeout.
   *
   * @param topicObject Object to collaborate on
   * @param timeout the maximum time to wait for a permit
   * @param unit the time unit of the <tt>timeout</tt> argument.
   * @return true if participation in the collaboration was acquired
   *
   * @throws InterruptedException if thread is interrupted
   */
  public boolean tryAcquire(Object topicObject, long timeout, TimeUnit unit)
  throws InterruptedException {
    throw new UnsupportedOperationException(LocalizedStrings.Collaboration_NOT_IMPLEMENTED.toLocalizedString());
  }
  
  /**
   * Releases the current thread's participation in the collaboration. When
   * the last thread involved in the current topic has released, a new topic
   * can be started by any waiting threads.
   * <p>
   * Nothing happens if the calling thread is not participating in the current
   * topic.
   */
  public void release() {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    synchronized (this.topicsQueue) {
      Topic topic = this.currentTopic;
      if (topic == null) {
        throw new IllegalStateException(LocalizedStrings.Collaboration_COLLABORATION_HAS_NO_CURRENT_TOPIC.toLocalizedString());
      }
      if (isDebugEnabled) {
        logger.debug("Collaboration.release: {} releasing topic", this.toString());
      }
      if (topic.isEmptyAfterRemovingThread(Thread.currentThread())) {
        if (isDebugEnabled) {
          logger.debug("Collaboration.release: {} released old topic {}", this.toString(), topic);
        }
        // current topic is done... release it
        this.topicsMap.remove(topic.getTopicObject());
        if (!this.topicsQueue.isEmpty()) {
          // next topic becomes the current topic
          Topic nextTopic = (Topic) this.topicsQueue.remove(0);
          setCurrentTopic(nextTopic);
        } else {
          setCurrentTopic(null);
        }
      }
      else  {
        if (isDebugEnabled) {
          logger.debug("Collaboration.release: {} released current topic ", this.toString());
        }
      }
    } // synchronized
  }

  /** Returns true if a collaboration topic currently exists. */
  public boolean hasCurrentTopic(Thread thread) {
    synchronized (this.topicsQueue) {
      if (this.currentTopic == null) return false;
      return this.currentTopic.hasThread(thread);
    }
  }
  
  /** Returns true if a collaboration topic currently exists. */
  public boolean hasCurrentTopic() {
    synchronized (this.topicsQueue) {
      return (this.currentTopic != null);
    }
  }
  
  /** Returns true if topic matches the current collaboration topic. */
  public boolean isCurrentTopic(Object topicObject) {
    if (topicObject == null) {
      throw new IllegalArgumentException(LocalizedStrings.Collaboration_TOPIC_MUST_BE_SPECIFIED.toLocalizedString());
    }
    synchronized (this.topicsQueue) {
      if (this.currentTopic == null) {
        return false;
      }
      return this.currentTopic.getTopicObject().equals(topicObject);
    }
  }
    
  @Override
  public String toString() {
    synchronized (this.topicsQueue) {
      Topic topic = this.currentTopic;
      int threadCount = 0;
      if (topic != null) {
        threadCount = topic.threadCount();
      }
      return getIdentity() + ": topic=" + topic + " threadCount=" + threadCount;
    }
  }
  
  protected String getIdentity() {
    String me = super.toString();
    return me.substring(me.lastIndexOf(".")+1);
  }
  
  /**
   * Blocking threads will wait on this wrapper object. As threads release,
   * they will be removed from the Topic. The last one removed will notifyAll 
   * on the next Topic in topicsQueue.
   */
  static public class Topic {
    
    private boolean isCurrentTopic = false;
    
    private boolean isOldTopic = false;
    
    private final Object topicObject;
    
    /**
     * guarded.By {@link Collaboration#topicsQueue}
     * guarded.By this instance, <em>after</em> acquiring the topicsQueue
     */
    private final List participatingThreads = new ArrayList();
    
    /** Constructs new Topic to wrap the internal topicObject. */
    public Topic(Object topicObject) {
      this.topicObject = topicObject;
    }
    
    public boolean isCurrentTopic() {
      synchronized (this) {
        return this.isCurrentTopic;
      }
    }
    
    public boolean isOldTopic() {
      synchronized (this) {
        return this.isOldTopic;
      }
    }
    
    public Object getTopicObject() {
      synchronized (this) {
        return this.topicObject;
      }
    }
    
    public void setOldTopic(boolean v) {
      synchronized (this) {
        this.isOldTopic = v;
      }
    }
    
    public void setCurrentTopic(boolean v) {
      synchronized (this) {
        this.isOldTopic = v;
      }
    }
    /** 
     * Atomically removes thread and returns true if there are no more 
     * participating threads. 
     */
    public boolean isEmptyAfterRemovingThread(Thread thread) {
      synchronized (this) {
        boolean removed = this.participatingThreads.remove(thread);
        if (!removed) {
          Assert.assertTrue(false, 
              "thread " + thread + " was not participating in " + this);
        }
        /*if (Collaboration.this.debugEnabled()) {
          Collaboration.this.log.fine("[" + Collaboration.this.getIdentity() + 
              ".Topic] removed " + thread + " from " + this +
              "; remaining threads: " + this.participatingThreads);
        }*/
        return this.participatingThreads.isEmpty();
      }
    }
    
    /** Adds thread to list of threads participating in this topic. */ 
    public void addThread(Thread thread) {
      synchronized (this) {
        this.participatingThreads.add(thread);
      }
    }
    
    /** Returns true if the thread was removed from participating threads. */
    public boolean removeThread(Thread thread) {
      synchronized (this) {
        return this.participatingThreads.remove(thread);
      }
    }
    
    /** Returns count of threads participating in this topic. */
    public int threadCount() {
      synchronized (this) {
        return this.participatingThreads.size();
      }
    }
    
    /** Returns true if the thread is one of the participating threads. */
    public boolean hasThread(Thread thread) {
      synchronized (this) {
        return this.participatingThreads.contains(thread);
      }
    }
    
    @Override
    public String toString() {
      String nick = super.toString();
      nick = nick.substring(nick.lastIndexOf(".") + 1);
      return nick + ": " + topicObject;
    }
    
  }
  
}

