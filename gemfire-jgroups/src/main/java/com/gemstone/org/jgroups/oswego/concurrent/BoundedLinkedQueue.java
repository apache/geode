/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: BoundedLinkedQueue.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  11Jun1998  dl               Create public version
  17Jul1998  dl               Simplified by eliminating wait counts
  25aug1998  dl               added peek
  10oct1999  dl               lock on node object to ensure visibility
  27jan2000  dl               setCapacity forces immediate permit reconcile
*/

package com.gemstone.org.jgroups.oswego.concurrent;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A bounded variant of 
 * LinkedQueue 
 * class. This class may be
 * preferable to 
 * BoundedBuffer 
 * because it allows a bit more
 * concurency among puts and takes,  because it does not
 * pre-allocate fixed storage for elements, and allows 
 * capacity to be dynamically reset.
 * On the other hand, since it allocates a node object
 * on each put, it can be slow on systems with slow
 * allocation and GC.
 * Also, it may be
 * preferable to 
 * LinkedQueue 
 * when you need to limit
 * the capacity to prevent resource exhaustion. This protection
 * normally does not hurt much performance-wise: When the
 * queue is not empty or full, most puts and
 * takes are still usually able to execute concurrently.
 * @see LinkedQueue 
 * @see BoundedBuffer 
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>] <p>
 **/

public class BoundedLinkedQueue implements BoundedChannel {

  /*
   * It might be a bit nicer if this were declared as
   * a subclass of LinkedQueue, or a sibling class of
   * a common abstract class. It shares much of the
   * basic design and bookkeeping fields. But too 
   * many details differ to make this worth doing.
   */



  /** 
   * Dummy header node of list. The first actual node, if it exists, is always 
   * at head_.next. After each take, the old first node becomes the head.
   **/
  protected LinkedNode head_;

  /** 
   * The last node of list. Put() appends to list, so modifies last_
   **/
  protected LinkedNode last_;


  /**
   * Helper monitor. Ensures that only one put at a time executes.
   **/

  protected final Object putGuard_ = new Object();

  /**
   * Helper monitor. Protects and provides wait queue for takes
   **/

  protected final Object takeGuard_ = new Object();


  /** Number of elements allowed **/
  protected int capacity_;

  
  /**
   * One side of a split permit count. 
   * The counts represent permits to do a put. (The queue is full when zero).
   * Invariant: putSidePutPermits_ + takeSidePutPermits_ = capacity_ - length.
   * (The length is never separately recorded, so this cannot be
   * checked explicitly.)
   * To minimize contention between puts and takes, the
   * put side uses up all of its permits before transfering them from
   * the take side. The take side just increments the count upon each take.
   * Thus, most puts and take can run independently of each other unless
   * the queue is empty or full. 
   * Initial value is queue capacity.
   **/

  protected int putSidePutPermits_; 

  /** Number of takes since last reconcile **/
  protected int takeSidePutPermits_ = 0;


  /**
   * Create a queue with the given capacity
   * @exception IllegalArgumentException if capacity less or equal to zero
   **/
  public BoundedLinkedQueue(int capacity) {
    if (capacity <= 0) throw new IllegalArgumentException();
    capacity_ = capacity;
    putSidePutPermits_ = capacity;
    head_ =  new LinkedNode(null); 
    last_ = head_;
  }

  /**
   * Create a queue with the current default capacity
   **/

  public BoundedLinkedQueue() { 
    this(DefaultChannelCapacity.get()); 
  }

  /**
   * Move put permits from take side to put side; 
   * return the number of put side permits that are available.
   * Call only under synch on puGuard_ AND this.
   **/
  protected final int reconcilePutPermits() {
    putSidePutPermits_ += takeSidePutPermits_;
    takeSidePutPermits_ = 0;
    return putSidePutPermits_;
  }


  /** Return the current capacity of this queue **/
  public synchronized int capacity() { return capacity_; }


  /** 
   * Return the number of elements in the queue.
   * This is only a snapshot value, that may be in the midst 
   * of changing. The returned value will be unreliable in the presence of
   * active puts and takes, and should only be used as a heuristic
   * estimate, for example for resource monitoring purposes.
   **/
  public synchronized int size() {
    /*
      This should ideally synch on putGuard_, but
      doing so would cause it to block waiting for an in-progress
      put, which might be stuck. So we instead use whatever
      value of putSidePutPermits_ that we happen to read.
    */
    return capacity_ - (takeSidePutPermits_ + putSidePutPermits_);
  }


  /**
   * Reset the capacity of this queue.
   * If the new capacity is less than the old capacity,
   * existing elements are NOT removed, but
   * incoming puts will not proceed until the number of elements
   * is less than the new capacity.
   * @exception IllegalArgumentException if capacity less or equal to zero
   **/

  public void setCapacity(int newCapacity) {
    if (newCapacity <= 0) throw new IllegalArgumentException();
    synchronized (putGuard_) {
      synchronized(this) {
        takeSidePutPermits_ += (newCapacity - capacity_);
        capacity_ = newCapacity;
        
        // Force immediate reconcilation.
        reconcilePutPermits();
        notifyAll();
      }
    }
  }


  /** Main mechanics for take/poll **/
  @SuppressFBWarnings(value="ML_SYNC_ON_FIELD_TO_GUARD_CHANGING_THAT_FIELD",
      justification="it is okay for another thread to synchronize on the new head and enter this code")
  protected synchronized Object extract() {
    synchronized(head_) {
      Object x = null;
      LinkedNode first = head_.next;
      if (first != null) {
        x = first.value;
        first.value = null;
        head_ = first; 
        ++takeSidePutPermits_;
        notify();
      }
      return x;
    }
  }

  public Object peek() {
    synchronized(head_) {
      LinkedNode first = head_.next;
      if (first != null) 
        return first.value;
      else
        return null;
    }
  }

  public Object take() throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    Object x = extract();
    if (x != null) 
      return x;
    else {
      synchronized(takeGuard_) {
        try {
          for (;;) {
            x = extract();
            if (x != null) {
              return x;
            }
            else {
              takeGuard_.wait(); 
            }
          }
        }
        catch(InterruptedException ex) {
          takeGuard_.notify();
          throw ex; 
        }
      }
    }
  }

  public Object poll(long msecs) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    Object x = extract();
    if (x != null) 
      return x;
    else {
      synchronized(takeGuard_) {
        try {
          long waitTime = msecs;
          long start = (msecs <= 0)? 0: System.currentTimeMillis();
          for (;;) {
            x = extract();
            if (x != null || waitTime <= 0) {
              return x;
            }
            else {
              takeGuard_.wait(waitTime); 
              waitTime = msecs - (System.currentTimeMillis() - start);
            }
          }
        }
        catch(InterruptedException ex) {
          takeGuard_.notify();
          throw ex; 
        }
      }
    }
  }

  /** Notify a waiting take if needed **/
  protected final void allowTake() {
    synchronized(takeGuard_) {
      takeGuard_.notify();
    }
  }


  /**
   * Create and insert a node.
   * Call only under synch on putGuard_
   **/
  protected void insert(Object x) { 
    --putSidePutPermits_;
    LinkedNode p = new LinkedNode(x);
    synchronized(last_) {
      last_.next = p;
//      last_ = p; GemStoneAddition synchronized on last_ in futile attempt to guard it
    }
    last_ = p; // GemStoneAddition
  }


  /* 
     put and offer(ms) differ only in policy before insert/allowTake
  */
  @SuppressFBWarnings(value="TLW_TWO_LOCK_WAIT",justification="GemFire doesn't use this class")
  public void put(Object x) throws InterruptedException {
    if (x == null) throw new IllegalArgumentException();
    if (Thread.interrupted()) throw new InterruptedException();

    synchronized(putGuard_) {

      if (putSidePutPermits_ <= 0) { // wait for permit. 
        synchronized(this) {
          if (reconcilePutPermits() <= 0) {
            try {
              for(;;) {
                wait();
                if (reconcilePutPermits() > 0) {
                  break;
                }
              }
            }
            catch (InterruptedException ex) { 
              notify(); 
              throw ex; 
            }
          }
        }
      }
      insert(x);
    }
    // call outside of lock to loosen put/take coupling
    allowTake();
  }

  @SuppressFBWarnings(value="TLW_TWO_LOCK_WAIT",justification="GemFire doesn't use this class")
  public boolean offer(Object x, long msecs) throws InterruptedException {
    if (x == null) throw new IllegalArgumentException();
    if (Thread.interrupted()) throw new InterruptedException();

    synchronized(putGuard_) {

      if (putSidePutPermits_ <= 0) {
        synchronized(this) {
          if (reconcilePutPermits() <= 0) {
            if (msecs <= 0)
              return false;
            else {
              try {
                long waitTime = msecs;
                long start = System.currentTimeMillis();
                
                for(;;) {
                  wait(waitTime);
                  if (reconcilePutPermits() > 0) {
                    break;
                  }
                  else {
                    waitTime = msecs - (System.currentTimeMillis() - start);
                    if (waitTime <= 0) {
                      return false;
                    }
                  }
                }
              }
              catch (InterruptedException ex) { 
                notify(); 
                throw ex; 
              }
            }
          }
        }
      }

      insert(x);
    }

    allowTake();
    return true;
  }

  public boolean isEmpty() {
    synchronized(head_) {
      return head_.next == null;
    }
  }    
    

}
