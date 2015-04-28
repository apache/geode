/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * @(#)GFEAbstractQueuedSynchronizer.java 
 *
 * Neeraj: GFEAbstractQueuedSynchronizer class has been adapted from
 * the AbstractQueuedSynchronizer class to suit the needs of distributed
 * transactions in GFE. The main reason why it is needed to be so blatantly
 * copied is that in GFE transactions we need to make the locks reentrant
 * not based on the thread itself but reentrant based on the TxID object.
 * For the same transactions if different function execution threads are
 * vying for the same entries then they should be allowed.
 * 
 * Even the documentation has been kept unchanged and tweaked slightly just
 * for the additional stuff we wanted
 */

package com.gemstone.gemfire.internal.cache.locks;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;

/**
 * Provides a framework for implementing blocking locks and related
 * synchronizers (semaphores, events, etc) that rely on first-in-first-out
 * (FIFO) wait queues. This class is designed to be a useful basis for most
 * kinds of synchronizers that rely on a single atomic <tt>int</tt> value to
 * represent state. Subclasses must define the protected methods that change
 * this state, and which define what that state means in terms of this object
 * being acquired or released. Given these, the other methods in this class
 * carry out all queuing and blocking mechanics. Subclasses can maintain other
 * state fields, but only the atomically updated <tt>int</tt> value manipulated
 * using methods {@link #getState}, {@link #setState} and
 * {@link #compareAndSetState} is tracked with respect to synchronization.
 * 
 * <p>
 * Subclasses should be defined as non-public internal helper classes that are
 * used to implement the synchronization properties of their enclosing class.
 * Class <tt>GFEAbstractQueuedSynchronizer</tt> does not implement any
 * synchronization interface. Instead it defines methods such as
 * {@link #acquireInterruptibly} that can be invoked as appropriate by concrete
 * locks and related synchronizers to implement their public methods.
 * 
 * <p>
 * This class supports either or both a default <em>exclusive</em> mode and a
 * <em>shared</em> mode. When acquired in exclusive mode, attempted acquires by
 * other threads cannot succeed. Shared mode acquires by multiple threads may
 * (but need not) succeed. This class does not &quot;understand&quot; these
 * differences except in the mechanical sense that when a shared mode acquire
 * succeeds, the next waiting thread (if one exists) must also determine whether
 * it can acquire as well. Threads waiting in the different modes share the same
 * FIFO queue. Usually, implementation subclasses support only one of these
 * modes, but both can come into play for example in a ReadWriteLock.
 * Subclasses that support only exclusive or only shared modes need not define
 * the methods supporting the unused mode.
 * 
 * <p>
 * This class provides inspection, instrumentation, and monitoring methods for
 * the internal queue, as well as similar methods for condition objects. These
 * can be exported as desired into classes using an
 * <tt>GFEAbstractQueuedSynchronizer</tt> for their synchronization mechanics.
 * 
 * <p>
 * Serialization of this class stores only the underlying atomic integer
 * maintaining state, so deserialized objects have empty thread queues. Typical
 * subclasses requiring serializability will define a <tt>readObject</tt> method
 * that restores this to a known initial state upon deserialization.
 * 
 * <h3>Usage</h3>
 * 
 * <p>
 * To use this class as the basis of a synchronizer, redefine the following
 * methods, as applicable, by inspecting and/or modifying the synchronization
 * state using {@link #getState}, {@link #setState} and/or
 * {@link #compareAndSetState}:
 * 
 * <ul>
 * <li> {@link #tryAcquire}
 * <li> {@link #tryRelease}
 * <li> {@link #tryAcquireShared}
 * <li> {@link #tryReleaseShared}
 * <li> {@link #isHeldExclusively}
 *</ul>
 * 
 * Each of these methods by default throws {@link UnsupportedOperationException}
 * . Implementations of these methods must be internally thread-safe, and should
 * in general be short and not block. Defining these methods is the
 * <em>only</em> supported means of using this class. All other methods are
 * declared <tt>final</tt> because they cannot be independently varied.
 * 
 * <p>
 * Even though this class is based on an internal FIFO queue, it does not
 * automatically enforce FIFO acquisition policies. The core of exclusive
 * synchronization takes the form:
 * 
 * <pre>
 * Acquire:
 *     while (!tryAcquire(arg)) {
 *        &lt;em&gt;enqueue thread if it is not already queued&lt;/em&gt;;
 *        &lt;em&gt;possibly block current thread&lt;/em&gt;;
 *     }
 * Release:
 *     if (tryRelease(arg))
 *        &lt;em&gt;unblock the first queued thread&lt;/em&gt;;
 * </pre>
 * 
 * (Shared mode is similar but may involve cascading signals.)
 * 
 * <p>
 * Because checks in acquire are invoked before enqueuing, a newly acquiring
 * thread may <em>barge</em> ahead of others that are blocked and queued.
 * However, you can, if desired, define <tt>tryAcquire</tt> and/or
 * <tt>tryAcquireShared</tt> to disable barging by internally invoking one or
 * more of the inspection methods. In particular, a strict FIFO lock can define
 * <tt>tryAcquire</tt> to immediately return <tt>false</tt> if
 * {@link #getFirstQueuedThread} does not return the current thread. A normally
 * preferable non-strict fair version can immediately return <tt>false</tt> only
 * if {@link #hasQueuedThreads} returns <tt>true</tt> and
 * <tt>getFirstQueuedThread</tt> is not the current thread; or equivalently,
 * that <tt>getFirstQueuedThread</tt> is both non-null and not the current
 * thread. Further variations are possible.
 * 
 * <p>
 * Throughput and scalability are generally highest for the default barging
 * (also known as <em>greedy</em>, <em>renouncement</em>, and
 * <em>convoy-avoidance</em>) strategy. While this is not guaranteed to be fair
 * or starvation-free, earlier queued threads are allowed to recontend before
 * later queued threads, and each recontention has an unbiased chance to succeed
 * against incoming threads. Also, while acquires do not &quot;spin&quot; in the
 * usual sense, they may perform multiple invocations of <tt>tryAcquire</tt>
 * interspersed with other computations before blocking. This gives most of the
 * benefits of spins when exclusive synchronization is only briefly held,
 * without most of the liabilities when it isn't. If so desired, you can augment
 * this by preceding calls to acquire methods with "fast-path" checks, possibly
 * prechecking {@link #hasContended} and/or {@link #hasQueuedThreads} to only do
 * so if the synchronizer is likely not to be contended.
 * 
 * <p>
 * This class provides an efficient and scalable basis for synchronization in
 * part by specializing its range of use to synchronizers that can rely on
 * <tt>int</tt> state, acquire, and release parameters, and an internal FIFO
 * wait queue. When this does not suffice, you can build synchronizers from a
 * lower level using java.util.concurrent.atomic atomic classes, your
 * own custom {@link java.util.Queue} classes, and {@link LockSupport} blocking
 * support.
 * 
 * <h3>Usage Examples</h3>
 * 
 * <p>
 * Here is a non-reentrant mutual exclusion lock class that uses the value zero
 * to represent the unlocked state, and one to represent the locked state. While
 * a non-reentrant lock does not strictly require recording of the current owner
 * thread, this class does so anyway to make usage easier to monitor. It also
 * supports conditions and exposes one of the instrumentation methods:
 * 
 * <pre>
 * class Mutex implements Lock, java.io.Serializable {
 *   // Our internal helper class
 *   private static class Sync extends GFEAbstractQueuedSynchronizer {
 *     // Report whether in locked state
 *     protected boolean isHeldExclusively() {
 *       return getState() == 1;
 *     }
 * 
 *     // Acquire the lock if state is zero
 *     public boolean tryAcquire(int acquires) {
 *       assert acquires == 1; // Otherwise unused
 *       if (compareAndSetState(0, 1)) {
 *         setExclusiveOwnerThread(Thread.currentThread());
 *         return true;
 *       }
 *       return false;
 *     }
 * 
 *     // Release the lock by setting state to zero
 *     protected boolean tryRelease(int releases) {
 *       assert releases == 1; // Otherwise unused
 *       if (getState() == 0) throw new IllegalMonitorStateException();
 *       setExclusiveOwnerThread(null);
 *       setState(0);
 *       return true;
 *     }
 * 
 *     // Provide a Condition
 *     Condition newCondition() {
 *       return new ConditionObject();
 *     }
 * 
 *     // Deserialize properly
 *     private void readObject(ObjectInputStream s) throws IOException,
 *         ClassNotFoundException {
 *       s.defaultReadObject();
 *       setState(0); // reset to unlocked state
 *     }
 *   }
 * 
 *   // The sync object does all the hard work. We just forward to it.
 *   private final Sync sync = new Sync();
 * 
 *   public void lock() {
 *     sync.acquire(1);
 *   }
 * 
 *   public boolean tryLock() {
 *     return sync.tryAcquire(1);
 *   }
 * 
 *   public void unlock() {
 *     sync.release(1);
 *   }
 * 
 *   public Condition newCondition() {
 *     return sync.newCondition();
 *   }
 * 
 *   public boolean isLocked() {
 *     return sync.isHeldExclusively();
 *   }
 * 
 *   public boolean hasQueuedThreads() {
 *     return sync.hasQueuedThreads();
 *   }
 * 
 *   public void lockInterruptibly() throws InterruptedException {
 *     sync.acquireInterruptibly(1);
 *   }
 * 
 *   public boolean tryLock(long timeout, TimeUnit unit)
 *       throws InterruptedException {
 *     return sync.tryAcquireNanos(1, unit.toNanos(timeout));
 *   }
 * }
 * </pre>
 * 
 * <p>
 * Here is a latch class that is like a {@link CountDownLatch} except that it
 * only requires a single <tt>signal</tt> to fire. Because a latch is
 * non-exclusive, it uses the <tt>shared</tt> acquire and release methods.
 * 
 * <pre>
 * class BooleanLatch {
 *   private static class Sync extends GFEAbstractQueuedSynchronizer {
 *     boolean isSignalled() {
 *       return getState() != 0;
 *     }
 * 
 *     protected int tryAcquireShared(int ignore) {
 *       return isSignalled() ? 1 : -1;
 *     }
 * 
 *     protected boolean tryReleaseShared(int ignore) {
 *       setState(1);
 *       return true;
 *     }
 *   }
 * 
 *   private final Sync sync = new Sync();
 * 
 *   public boolean isSignalled() {
 *     return sync.isSignalled();
 *   }
 * 
 *   public void signal() {
 *     sync.releaseShared(1);
 *   }
 * 
 *   public void await() throws InterruptedException {
 *     sync.acquireSharedInterruptibly(1);
 *   }
 * }
 * </pre>
 * 
 * @since 1.5
 * @author Doug Lea
 */
public abstract class GFEAbstractQueuedSynchronizer implements
    java.io.Serializable {

  private static final long serialVersionUID = 7373984972572414691L;

  private static final AtomicIntegerFieldUpdater<GFEAbstractQueuedSynchronizer> stateUpdater = AtomicIntegerFieldUpdater
      .newUpdater(GFEAbstractQueuedSynchronizer.class, "state");

  private static final AtomicIntegerFieldUpdater<Node> nodeStatusUpdater = AtomicIntegerFieldUpdater
      .newUpdater(Node.class, "waitStatus");

  private static final AtomicReferenceFieldUpdater<GFEAbstractQueuedSynchronizer, Node> headUpdater = AtomicReferenceFieldUpdater
      .newUpdater(GFEAbstractQueuedSynchronizer.class, Node.class, "head");

  private static final AtomicReferenceFieldUpdater<GFEAbstractQueuedSynchronizer, Node> tailUpdater = AtomicReferenceFieldUpdater
      .newUpdater(GFEAbstractQueuedSynchronizer.class, Node.class, "tail");

  private static final AtomicReferenceFieldUpdater<Node, Node> prevNodeUpdater = AtomicReferenceFieldUpdater
      .newUpdater(Node.class, Node.class, "prev");

  private static final AtomicReferenceFieldUpdater<Node, Node> nextNodeUpdater = AtomicReferenceFieldUpdater
      .newUpdater(Node.class, Node.class, "next");

  /**
   * Creates a new <tt>GFEAbstractQueuedSynchronizer</tt> instance with initial
   * synchronization state of zero.
   */
  protected GFEAbstractQueuedSynchronizer() {
  }

  /**
   * Wait queue node class.
   * 
   * <p>
   * The wait queue is a variant of a "CLH" (Craig, Landin, and Hagersten) lock
   * queue. CLH locks are normally used for spinlocks. We instead use them for
   * blocking synchronizers, but use the same basic tactic of holding some of
   * the control information about a thread in the predecessor of its node. A
   * "status" field in each node keeps track of whether a thread should block. A
   * node is signalled when its predecessor releases. Each node of the queue
   * otherwise serves as a specific-notification-style monitor holding a single
   * waiting thread. The status field does NOT control whether threads are
   * granted locks etc though. A thread may try to acquire if it is first in the
   * queue. But being first does not guarantee success; it only gives the right
   * to contend. So the currently released contender thread may need to rewait.
   * 
   * <p>
   * To enqueue into a CLH lock, you atomically splice it in as new tail. To
   * dequeue, you just set the head field.
   * 
   * <pre>
   *      +------+  prev +-----+       +-----+
   * head |      | &lt;---- |     | &lt;---- |     |  tail
   *      +------+       +-----+       +-----+
   * </pre>
   * 
   * <p>
   * Insertion into a CLH queue requires only a single atomic operation on
   * "tail", so there is a simple atomic point of demarcation from unqueued to
   * queued. Similarly, dequeing involves only updating the "head". However, it
   * takes a bit more work for nodes to determine who their successors are, in
   * part to deal with possible cancellation due to timeouts and interrupts.
   * 
   * <p>
   * The "prev" links (not used in original CLH locks), are mainly needed to
   * handle cancellation. If a node is cancelled, its successor is (normally)
   * relinked to a non-cancelled predecessor. For explanation of similar
   * mechanics in the case of spin locks, see the papers by Scott and Scherer at
   * http://www.cs.rochester.edu/u/scott/synchronization/
   * 
   * <p>
   * We also use "next" links to implement blocking mechanics. The thread id for
   * each node is kept in its own node, so a predecessor signals the next node
   * to wake up by traversing next link to determine which thread it is.
   * Determination of successor must avoid races with newly queued nodes to set
   * the "next" fields of their predecessors. This is solved when necessary by
   * checking backwards from the atomically updated "tail" when a node's
   * successor appears to be null. (Or, said differently, the next-links are an
   * optimization so that we don't usually need a backward scan.)
   * 
   * <p>
   * Cancellation introduces some conservatism to the basic algorithms. Since we
   * must poll for cancellation of other nodes, we can miss noticing whether a
   * cancelled node is ahead or behind us. This is dealt with by always
   * unparking successors upon cancellation, allowing them to stabilize on a new
   * predecessor.
   * 
   * <p>
   * CLH queues need a dummy header node to get started. But we don't create
   * them on construction, because it would be wasted effort if there is never
   * contention. Instead, the node is constructed and head and tail pointers are
   * set upon first contention.
   * 
   * <p>
   * Threads waiting on Conditions use the same nodes, but use an additional
   * link. Conditions only need to link nodes in simple (non-concurrent) linked
   * queues because they are only accessed when exclusively held. Upon await, a
   * node is inserted into a condition queue. Upon signal, the node is
   * transferred to the main queue. A special value of status field is used to
   * mark which queue a node is on.
   * 
   * <p>
   * Thanks go to Dave Dice, Mark Moir, Victor Luchangco, Bill Scherer and
   * Michael Scott, along with members of JSR-166 expert group, for helpful
   * ideas, discussions, and critiques on the design of this class.
   */
  static final class Node {
    /** waitStatus value to indicate thread has cancelled */
    static final int CANCELLED = 1;

    /** waitStatus value to indicate successor's thread needs unparking */
    static final int SIGNAL = -1;

    /** waitStatus value to indicate thread is waiting on condition */
    static final int CONDITION = -2;

    /** Marker to indicate a node is waiting in shared mode */
    static final Node SHARED = new Node();

    /** Marker to indicate a node is waiting in exclusive mode */
    static final Node EXCLUSIVE = null;

    /**
     * Status field, taking on only the values: SIGNAL: The successor of this
     * node is (or will soon be) blocked (via park), so the current node must
     * unpark its successor when it releases or cancels. To avoid races, acquire
     * methods must first indicate they need a signal, then retry the atomic
     * acquire, and then, on failure, block. CANCELLED: This node is cancelled
     * due to timeout or interrupt. Nodes never leave this state. In particular,
     * a thread with cancelled node never again blocks. CONDITION: This node is
     * currently on a condition queue. It will not be used as a sync queue node
     * until transferred. (Use of this value here has nothing to do with the
     * other uses of the field, but simplifies mechanics.) 0: None of the above
     * 
     * The values are arranged numerically to simplify use. Non-negative values
     * mean that a node doesn't need to signal. So, most code doesn't need to
     * check for particular values, just for sign.
     * 
     * The field is initialized to 0 for normal sync nodes, and CONDITION for
     * condition nodes. It is modified only using CAS.
     */
    volatile int waitStatus;

    /**
     * Link to predecessor node that current node/thread relies on for checking
     * waitStatus. Assigned during enqueing, and nulled out (for sake of GC)
     * only upon dequeuing. Also, upon cancellation of a predecessor, we
     * short-circuit while finding a non-cancelled one, which will always exist
     * because the head node is never cancelled: A node becomes head only as a
     * result of successful acquire. A cancelled thread never succeeds in
     * acquiring, and a thread only cancels itself, not any other node.
     */
    volatile Node prev;

    /**
     * Link to the successor node that the current node/thread unparks upon
     * release. Assigned once during enqueuing, and nulled out (for sake of GC)
     * when no longer needed. Upon cancellation, we cannot adjust this field,
     * but can notice status and bypass the node if cancelled. The enq operation
     * does not assign next field of a predecessor until after attachment, so
     * seeing a null next field does not necessarily mean that node is at end of
     * queue. However, if a next field appears to be null, we can scan prev's
     * from the tail to double-check.
     */
    volatile Node next;

    /**
     * The thread that enqueued this node. Initialized on construction and
     * nulled out after use.
     */
    volatile Thread thread;

    /**
     * Link to next node waiting on condition, or the special value SHARED.
     * Because condition queues are accessed only when holding in exclusive
     * mode, we just need a simple linked queue to hold nodes while they are
     * waiting on conditions. They are then transferred to the queue to
     * re-acquire. And because conditions can only be exclusive, we save a field
     * by using special value to indicate shared mode.
     */
    Node nextWaiter;

    /**
     * Returns true if node is waiting in shared mode
     */
    final boolean isShared() {
      return nextWaiter == SHARED;
    }

    /**
     * Returns previous node, or throws NullPointerException if null. Use when
     * predecessor cannot be null.
     * 
     * @return the predecessor of this node
     */
    final Node predecessor() throws NullPointerException {
      Node p = prev;
      if (p == null)
        throw new NullPointerException();
      else
        return p;
    }

    Node() { // Used to establish initial head or SHARED marker
    }

    Node(Thread thread, Node mode) { // Used by addWaiter
      this.nextWaiter = mode;
      this.thread = thread;
    }

    Node(Thread thread, int waitStatus) { // Used by Condition
      this.waitStatus = waitStatus;
      this.thread = thread;
    }
  }

  /**
   * Head of the wait queue, lazily initialized. Except for initialization, it
   * is modified only via method setHead. Note: If head exists, its waitStatus
   * is guaranteed not to be CANCELLED.
   */
  private transient volatile Node head;

  /**
   * Tail of the wait queue, lazily initialized. Modified only via method enq to
   * add new wait node.
   */
  private transient volatile Node tail;

  /**
   * The synchronization state.
   */
  private volatile int state;

  /**
   * Returns the current value of synchronization state. This operation has
   * memory semantics of a <tt>volatile</tt> read.
   * 
   * @return current state value
   */
  protected final int getState() {
    return state;
  }

  /**
   * Sets the value of synchronization state. This operation has memory
   * semantics of a <tt>volatile</tt> write.
   * 
   * @param newState
   *          the new state value
   */
  protected final void setState(int newState) {
    state = newState;
  }

  /**
   * Atomically sets synchronization state to the given updated value if the
   * current state value equals the expected value. This operation has memory
   * semantics of a <tt>volatile</tt> read and write.
   * 
   * @param expect
   *          the expected value
   * @param update
   *          the new value
   * @return true if successful. False return indicates that the actual value
   *         was not equal to the expected value.
   */
  protected final boolean compareAndSetState(int expect, int update) {
    // See below for intrinsics setup to support this
    // return unsafe.compareAndSwapInt(this, stateOffset, expect, update);
    return stateUpdater.compareAndSet(this, expect, update);
    // return false; //Neeraj @TODO
  }

  // Queuing utilities

  /**
   * The number of nanoseconds for which it is faster to spin rather than to use
   * timed park. A rough estimate suffices to improve responsiveness with very
   * short timeouts.
   */
  static final long spinForTimeoutThreshold = 1000L;

  /**
   * Inserts node into queue, initializing if necessary. See picture above.
   * 
   * @param node
   *          the node to insert
   * @return node's predecessor
   */
  private Node enq(final Node node) {
    for (;;) {
      Node t = tail;
      if (t == null) { // Must initialize
        Node h = new Node(); // Dummy header
        h.next = node;
        node.prev = h;
        if (compareAndSetHead(h)) {
          tail = node;
          return h;
        }
      }
      else {
        node.prev = t;
        if (compareAndSetTail(t, node)) {
          t.next = node;
          return t;
        }
      }
    }
  }

  /**
   * Creates and enqueues node for given thread and mode.
   * 
   * @param mode
   *          Node.EXCLUSIVE for exclusive, Node.SHARED for shared
   * @return the new node
   */
  private Node addWaiter(Node mode) {
    Node node = new Node(Thread.currentThread(), mode);
    // Try the fast path of enq; backup to full enq on failure
    Node pred = tail;
    if (pred != null) {
      node.prev = pred;
      if (compareAndSetTail(pred, node)) {
        pred.next = node;
        return node;
      }
    }
    enq(node);
    return node;
  }

  /**
   * Sets head of queue to be node, thus dequeuing. Called only by acquire
   * methods. Also nulls out unused fields for sake of GC and to suppress
   * unnecessary signals and traversals.
   * 
   * @param node
   *          the node
   */
  private void setHead(Node node) {
    head = node;
    node.thread = null;
    node.prev = null;
  }

  /**
   * Wakes up node's successor, if one exists.
   * 
   * @param node
   *          the node
   */
  private void unparkSuccessor(Node node) {
    /*
     * Try to clear status in anticipation of signalling.  It is
     * OK if this fails or if status is changed by waiting thread.
     */
    compareAndSetWaitStatus(node, Node.SIGNAL, 0);

    /*
     * Thread to unpark is held in successor, which is normally
     * just the next node.  But if cancelled or apparently null,
     * traverse backwards from tail to find the actual
     * non-cancelled successor.
     */
    Node s = node.next;
    if (s == null || s.waitStatus > 0) {
      s = null;
      for (Node t = tail; t != null && t != node; t = t.prev)
        if (t.waitStatus <= 0)
          s = t;
    }
    if (s != null)
      LockSupport.unpark(s.thread);
  }

  /**
   * Sets head of queue, and checks if successor may be waiting in shared mode,
   * if so propagating if propagate > 0.
   * 
   * @param node
   *          the node
   * @param propagate
   *          the return value from a tryAcquireShared
   */
  private void setHeadAndPropagate(Node node, int propagate) {
    setHead(node);
    if (propagate > 0 && node.waitStatus != 0) {
      /*
       * Don't bother fully figuring out successor.  If it
       * looks null, call unparkSuccessor anyway to be safe.
       */
      Node s = node.next;
      if (s == null || s.isShared())
        unparkSuccessor(node);
    }
  }

  // Utilities for various versions of acquire

  /**
   * Cancels an ongoing attempt to acquire.
   * 
   * @param node
   *          the node
   */
  private void cancelAcquire(Node node) {
    // Ignore if node doesn't exist
    if (node == null)
      return;

    node.thread = null;

    // Skip cancelled predecessors
    Node pred = node.prev;
    while (pred.waitStatus > 0)
      node.prev = pred = pred.prev;

    // Getting this before setting waitStatus ensures staleness
    Node predNext = pred.next;

    // Can use unconditional write instead of CAS here
    node.waitStatus = Node.CANCELLED;

    // If we are the tail, remove ourselves
    if (node == tail && compareAndSetTail(node, pred)) {
      compareAndSetNext(pred, predNext, null);
    }
    else {
      // If "active" predecessor found...
      if (pred != head
          && (pred.waitStatus == Node.SIGNAL || compareAndSetWaitStatus(pred,
              0, Node.SIGNAL)) && pred.thread != null) {

        // If successor is active, set predecessor's next link
        Node next = node.next;
        if (next != null && next.waitStatus <= 0)
          compareAndSetNext(pred, predNext, next);
      }
      else {
        unparkSuccessor(node);
      }

      node.next = node; // help GC
    }
  }

  /**
   * Checks and updates status for a node that failed to acquire. Returns true
   * if thread should block. This is the main signal control in all acquire
   * loops. Requires that pred == node.prev
   * 
   * @param pred
   *          node's predecessor holding status
   * @param node
   *          the node
   * @return {@code true} if thread should block
   */
  private static boolean shouldParkAfterFailedAcquire(Node pred, Node node) {
    int s = pred.waitStatus;
    if (s < 0)
      /*
       * This node has already set status asking a release
       * to signal it, so it can safely park
       */
      return true;
    if (s > 0) {
      /*
       * Predecessor was cancelled. Skip over predecessors and
       * indicate retry.
       */
      do {
        node.prev = pred = pred.prev;
      } while (pred.waitStatus > 0);
      pred.next = node;
    }
    else
      /*
       * Indicate that we need a signal, but don't park yet. Caller
       * will need to retry to make sure it cannot acquire before
       * parking.
       */
      compareAndSetWaitStatus(pred, 0, Node.SIGNAL);
    return false;
  }

  /**
   * Convenience method to interrupt current thread.
   */
  private static void selfInterrupt() {
    Thread.currentThread().interrupt();
  }

  /**
   * Convenience method to park and then check if interrupted
   * 
   * @return {@code true} if interrupted
   */
  private final boolean parkAndCheckInterrupt() {
    LockSupport.park();
    return Thread.interrupted();
  }

  /*
   * Various flavors of acquire, varying in exclusive/shared and
   * control modes.  Each is mostly the same, but annoyingly
   * different.  Only a little bit of factoring is possible due to
   * interactions of exception mechanics (including ensuring that we
   * cancel if tryAcquire throws exception) and other control, at
   * least not without hurting performance too much.
   */

  /**
   * Acquires in exclusive uninterruptible mode for thread already in queue.
   * Used by condition wait methods as well as acquire.
   * 
   * @param node
   *          the node
   * @param arg
   *          the acquire argument
   * @return {@code true} if interrupted while waiting
   */
  final boolean acquireQueued(final Node node, int arg, Object id) {
    try {
      boolean interrupted = false;
      for (;;) {
        final Node p = node.predecessor();
        if (p == head && tryAcquire(arg, id)) {
          setHead(node);
          p.next = null; // help GC
          return interrupted;
        }
        if (shouldParkAfterFailedAcquire(p, node) && parkAndCheckInterrupt())
          interrupted = true;
      }
    } catch (RuntimeException ex) {
      cancelAcquire(node);
      throw ex;
    }
  }

  /**
   * Acquires in exclusive interruptible mode.
   * 
   * @param arg
   *          the acquire argument
   */
  private void doAcquireInterruptibly(int arg, Object id) throws InterruptedException {
    final Node node = addWaiter(Node.EXCLUSIVE);
    try {
      for (;;) {
        final Node p = node.predecessor();
        if (p == head && tryAcquire(arg, id)) {
          setHead(node);
          p.next = null; // help GC
          return;
        }
        if (shouldParkAfterFailedAcquire(p, node) && parkAndCheckInterrupt())
          break;
      }
    } catch (RuntimeException ex) {
      cancelAcquire(node);
      throw ex;
    }
    // Arrive here only if interrupted
    cancelAcquire(node);
    throw new InterruptedException();
  }

  /**
   * Acquires in exclusive timed mode.
   * 
   * @param arg
   *          the acquire argument
   * @param nanosTimeout
   *          max wait time
   * @return {@code true} if acquired
   */
  private boolean doAcquireNanos(int arg, Object id, long nanosTimeout)
      throws InterruptedException {
    long lastTime = System.nanoTime();
    final Node node = addWaiter(Node.EXCLUSIVE);
    try {
      for (;;) {
        final Node p = node.predecessor();
        if (p == head && tryAcquire(arg, id)) {
          setHead(node);
          p.next = null; // help GC
          return true;
        }
        if (nanosTimeout <= 0) {
          cancelAcquire(node);
          return false;
        }
        if (nanosTimeout > spinForTimeoutThreshold
            && shouldParkAfterFailedAcquire(p, node))
          LockSupport.parkNanos(nanosTimeout);
        long now = System.nanoTime();
        nanosTimeout -= now - lastTime;
        lastTime = now;
        if (Thread.interrupted())
          break;
      }
    } catch (RuntimeException ex) {
      cancelAcquire(node);
      throw ex;
    }
    // Arrive here only if interrupted
    cancelAcquire(node);
    throw new InterruptedException();
  }

  /**
   * Acquires in shared uninterruptible mode.
   * 
   * @param arg
   *          the acquire argument
   */
  private void doAcquireShared(int arg, Object id) {
    final Node node = addWaiter(Node.SHARED);
    try {
      boolean interrupted = false;
      for (;;) {
        final Node p = node.predecessor();
        if (p == head) {
          int r = tryAcquireShared(arg, id);
          if (r >= 0) {
            setHeadAndPropagate(node, r);
            p.next = null; // help GC
            if (interrupted)
              selfInterrupt();
            return;
          }
        }
        if (shouldParkAfterFailedAcquire(p, node) && parkAndCheckInterrupt())
          interrupted = true;
      }
    } catch (RuntimeException ex) {
      cancelAcquire(node);
      throw ex;
    }
  }

  /**
   * Acquires in shared interruptible mode.
   * 
   * @param arg
   *          the acquire argument
   */
  private void doAcquireSharedInterruptibly(int arg, Object id)
      throws InterruptedException {
    final Node node = addWaiter(Node.SHARED);
    try {
      for (;;) {
        final Node p = node.predecessor();
        if (p == head) {
          int r = tryAcquireShared(arg, id);
          if (r >= 0) {
            setHeadAndPropagate(node, r);
            p.next = null; // help GC
            return;
          }
        }
        if (shouldParkAfterFailedAcquire(p, node) && parkAndCheckInterrupt())
          break;
      }
    } catch (RuntimeException ex) {
      cancelAcquire(node);
      throw ex;
    }
    // Arrive here only if interrupted
    cancelAcquire(node);
    throw new InterruptedException();
  }

  /**
   * Acquires in shared timed mode.
   * 
   * @param arg
   *          the acquire argument
   * @param nanosTimeout
   *          max wait time
   * @return {@code true} if acquired
   */
  private boolean doAcquireSharedNanos(int arg, Object id, long nanosTimeout)
      throws InterruptedException {

    long lastTime = System.nanoTime();
    final Node node = addWaiter(Node.SHARED);
    try {
      for (;;) {
        final Node p = node.predecessor();
        if (p == head) {
          int r = tryAcquireShared(arg, id);
          if (r >= 0) {
            setHeadAndPropagate(node, r);
            p.next = null; // help GC
            return true;
          }
        }
        if (nanosTimeout <= 0) {
          cancelAcquire(node);
          return false;
        }
        if (nanosTimeout > spinForTimeoutThreshold
            && shouldParkAfterFailedAcquire(p, node))
          LockSupport.parkNanos(nanosTimeout);
        long now = System.nanoTime();
        nanosTimeout -= now - lastTime;
        lastTime = now;
        if (Thread.interrupted())
          break;
      }
    } catch (RuntimeException ex) {
      cancelAcquire(node);
      throw ex;
    }
    // Arrive here only if interrupted
    cancelAcquire(node);
    throw new InterruptedException();
  }

  // Main exported methods

  /**
   * Attempts to acquire in exclusive mode. This method should query if the
   * state of the object permits it to be acquired in the exclusive mode, and if
   * so to acquire it.
   * 
   * <p>
   * This method is always invoked by the thread performing acquire. If this
   * method reports failure, the acquire method may queue the thread, if it is
   * not already queued, until it is signalled by a release from some other
   * thread.
   * 
   * <p>
   * The default implementation throws {@link UnsupportedOperationException}.
   * 
   * @param arg
   *          the acquire argument. This value is always the one passed to an
   *          acquire method, or is the value saved on entry to a condition
   *          wait. The value is otherwise uninterpreted and can represent
   *          anything you like.
   * @return {@code true} if successful. Upon success, this object has been
   *         acquired.
   * @throws IllegalMonitorStateException
   *           if acquiring would place this synchronizer in an illegal state.
   *           This exception must be thrown in a consistent fashion for
   *           synchronization to work correctly.
   * @throws UnsupportedOperationException
   *           if exclusive mode is not supported
   */
  protected boolean tryAcquire(int arg, Object id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Attempts to set the state to reflect a release in exclusive mode.
   * 
   * <p>
   * This method is always invoked by the thread performing release.
   * 
   * <p>
   * The default implementation throws {@link UnsupportedOperationException}.
   * 
   * @param arg
   *          the release argument. This value is always the one passed to a
   *          release method, or the current state value upon entry to a
   *          condition wait. The value is otherwise uninterpreted and can
   *          represent anything you like.
   * @return {@code true} if this object is now in a fully released state, so
   *         that any waiting threads may attempt to acquire; and {@code false}
   *         otherwise.
   * @throws IllegalMonitorStateException
   *           if releasing would place this synchronizer in an illegal state.
   *           This exception must be thrown in a consistent fashion for
   *           synchronization to work correctly.
   * @throws UnsupportedOperationException
   *           if exclusive mode is not supported
   */
  protected boolean tryRelease(int arg, Object id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Attempts to acquire in shared mode. This method should query if the state
   * of the object permits it to be acquired in the shared mode, and if so to
   * acquire it.
   * 
   * <p>
   * This method is always invoked by the thread performing acquire. If this
   * method reports failure, the acquire method may queue the thread, if it is
   * not already queued, until it is signalled by a release from some other
   * thread.
   * 
   * <p>
   * The default implementation throws {@link UnsupportedOperationException}.
   * 
   * @param arg
   *          the acquire argument. This value is always the one passed to an
   *          acquire method, or is the value saved on entry to a condition
   *          wait. The value is otherwise uninterpreted and can represent
   *          anything you like.
   * @return a negative value on failure; zero if acquisition in shared mode
   *         succeeded but no subsequent shared-mode acquire can succeed; and a
   *         positive value if acquisition in shared mode succeeded and
   *         subsequent shared-mode acquires might also succeed, in which case a
   *         subsequent waiting thread must check availability. (Support for
   *         three different return values enables this method to be used in
   *         contexts where acquires only sometimes act exclusively.) Upon
   *         success, this object has been acquired.
   * @throws IllegalMonitorStateException
   *           if acquiring would place this synchronizer in an illegal state.
   *           This exception must be thrown in a consistent fashion for
   *           synchronization to work correctly.
   * @throws UnsupportedOperationException
   *           if shared mode is not supported
   */
  protected int tryAcquireShared(int arg, Object id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Attempts to set the state to reflect a release in shared mode.
   * 
   * <p>
   * This method is always invoked by the thread performing release.
   * 
   * <p>
   * The default implementation throws {@link UnsupportedOperationException}.
   * 
   * @param arg
   *          the release argument. This value is always the one passed to a
   *          release method, or the current state value upon entry to a
   *          condition wait. The value is otherwise uninterpreted and can
   *          represent anything you like.
   * @return {@code true} if this release of shared mode may permit a waiting
   *         acquire (shared or exclusive) to succeed; and {@code false}
   *         otherwise
   * @throws IllegalMonitorStateException
   *           if releasing would place this synchronizer in an illegal state.
   *           This exception must be thrown in a consistent fashion for
   *           synchronization to work correctly.
   * @throws UnsupportedOperationException
   *           if shared mode is not supported
   */
  protected boolean tryReleaseShared(int arg, Object id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns {@code true} if synchronization is held exclusively with respect to
   * the current (calling) thread. This method is invoked upon each call to a
   * non-waiting ConditionObject method. (Waiting methods instead invoke
   * {@link #release}.)
   * 
   * <p>
   * The default implementation throws {@link UnsupportedOperationException}.
   * This method is invoked internally only within ConditionObject
   * methods, so need not be defined if conditions are not used.
   * 
   * @return {@code true} if synchronization is held exclusively; {@code false}
   *         otherwise
   * @throws UnsupportedOperationException
   *           if conditions are not supported
   */
  protected boolean isHeldExclusively() {
    throw new UnsupportedOperationException();
  }

  /**
   * Acquires in exclusive mode, ignoring interrupts. Implemented by invoking at
   * least once {@link #tryAcquire}, returning on success. Otherwise the thread
   * is queued, possibly repeatedly blocking and unblocking, invoking
   * {@link #tryAcquire} until success.
   * 
   * @param arg
   *          the acquire argument. This value is conveyed to
   *          {@link #tryAcquire} but is otherwise uninterpreted and can
   *          represent anything you like.
   */
  public final void acquire(int arg, Object id) {
    if (!tryAcquire(arg, id) && acquireQueued(addWaiter(Node.EXCLUSIVE), arg, id))
      selfInterrupt();
  }

  /**
   * Acquires in exclusive mode, aborting if interrupted. Implemented by first
   * checking interrupt status, then invoking at least once {@link #tryAcquire},
   * returning on success. Otherwise the thread is queued, possibly repeatedly
   * blocking and unblocking, invoking {@link #tryAcquire} until success or the
   * thread is interrupted.
   * 
   * @param arg
   *          the acquire argument. This value is conveyed to
   *          {@link #tryAcquire} but is otherwise uninterpreted and can
   *          represent anything you like.
   * @throws InterruptedException
   *           if the current thread is interrupted
   */
  public final void acquireInterruptibly(int arg, Object id) throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    if (!tryAcquire(arg, id))
      doAcquireInterruptibly(arg, id);
  }

  /**
   * Attempts to acquire in exclusive mode, aborting if interrupted, and failing
   * if the given timeout elapses.
   * 
   * @param arg
   *          the acquire argument. This value is conveyed to
   *          {@link #tryAcquire} but is otherwise uninterpreted and can
   *          represent anything you like.
   * @param id
   *          
   * @param nanosTimeout
   *          the maximum number of nanoseconds to wait
   * @return {@code true} if acquired; {@code false} if timed out
   * @throws InterruptedException
   *           if the current thread is interrupted
   */
  public final boolean tryAcquireNanos(int arg, Object id, long nanosTimeout)
      throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    return tryAcquire(arg, id) || doAcquireNanos(arg, id, nanosTimeout);
  }

  /**
   * Releases in exclusive mode. Implemented by unblocking one or more threads
   * 
   * @param arg
   *          the release argument. This value is conveyed to
   *          {@link #tryRelease} but is otherwise uninterpreted and can
   *          represent anything you like.
   * @return the value returned from {@link #tryRelease}
   */
  public final boolean release(int arg, Object id) {
    if (tryRelease(arg, id)) {
      Node h = head;
      if (h != null && h.waitStatus != 0)
        unparkSuccessor(h);
      return true;
    }
    return false;
  }

  /**
   * Acquires in shared mode, ignoring interrupts. Implemented by first invoking
   * at least once {@link #tryAcquireShared}, returning on success. Otherwise
   * the thread is queued, possibly repeatedly blocking and unblocking, invoking
   * {@link #tryAcquireShared} until success.
   * 
   * @param arg
   *          the acquire argument. This value is conveyed to
   *          {@link #tryAcquireShared} but is otherwise uninterpreted and can
   *          represent anything you like.
   */
  public final void acquireShared(int arg, Object id) {
    if (tryAcquireShared(arg, id) < 0)
      doAcquireShared(arg, id);
  }

  /**
   * Acquires in shared mode, aborting if interrupted. Implemented by first
   * checking interrupt status, then invoking at least once
   * {@link #tryAcquireShared}, returning on success. Otherwise the thread is
   * queued, possibly repeatedly blocking and unblocking, invoking
   * {@link #tryAcquireShared} until success or the thread is interrupted.
   * 
   * @param arg
   *          the acquire argument. This value is conveyed to
   *          {@link #tryAcquireShared} but is otherwise uninterpreted and can
   *          represent anything you like.
   * @throws InterruptedException
   *           if the current thread is interrupted
   */
  public final void acquireSharedInterruptibly(int arg, Object id)
      throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    if (tryAcquireShared(arg, id) < 0)
      doAcquireSharedInterruptibly(arg, id);
  }

  /**
   * Attempts to acquire in shared mode, aborting if interrupted, and failing if
   * the given timeout elapses. Implemented by first checking interrupt status,
   * then invoking at least once {@link #tryAcquireShared}, returning on
   * success. Otherwise, the thread is queued, possibly repeatedly blocking and
   * unblocking, invoking {@link #tryAcquireShared} until success or the thread
   * is interrupted or the timeout elapses.
   * 
   * @param arg
   *          the acquire argument. This value is conveyed to
   *          {@link #tryAcquireShared} but is otherwise uninterpreted and can
   *          represent anything you like.
   * @param nanosTimeout
   *          the maximum number of nanoseconds to wait
   * @return {@code true} if acquired; {@code false} if timed out
   * @throws InterruptedException
   *           if the current thread is interrupted
   */
  public final boolean tryAcquireSharedNanos(int arg, Object id, long nanosTimeout)
      throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
    return tryAcquireShared(arg, id) >= 0
        || doAcquireSharedNanos(arg, id, nanosTimeout);
  }

  /**
   * Releases in shared mode. Implemented by unblocking one or more threads if
   * {@link #tryReleaseShared} returns true.
   * 
   * @param arg
   *          the release argument. This value is conveyed to
   *          {@link #tryReleaseShared} but is otherwise uninterpreted and can
   *          represent anything you like.
   * @return the value returned from {@link #tryReleaseShared}
   */
  public final boolean releaseShared(int arg, Object id) {
    if (tryReleaseShared(arg, id)) {
      Node h = head;
      if (h != null && h.waitStatus != 0)
        unparkSuccessor(h);
      return true;
    }
    return false;
  }

  // Queue inspection methods

  /**
   * Queries whether any threads are waiting to acquire. Note that because
   * cancellations due to interrupts and timeouts may occur at any time, a
   * {@code true} return does not guarantee that any other thread will ever
   * acquire.
   * 
   * <p>
   * In this implementation, this operation returns in constant time.
   * 
   * @return {@code true} if there may be other threads waiting to acquire
   */
  public final boolean hasQueuedThreads() {
    return head != tail;
  }

  /**
   * Queries whether any threads have ever contended to acquire this
   * synchronizer; that is if an acquire method has ever blocked.
   * 
   * <p>
   * In this implementation, this operation returns in constant time.
   * 
   * @return {@code true} if there has ever been contention
   */
  public final boolean hasContended() {
    return head != null;
  }

  /**
   * Returns the first (longest-waiting) thread in the queue, or {@code null} if
   * no threads are currently queued.
   * 
   * <p>
   * In this implementation, this operation normally returns in constant time,
   * but may iterate upon contention if other threads are concurrently modifying
   * the queue.
   * 
   * @return the first (longest-waiting) thread in the queue, or {@code null} if
   *         no threads are currently queued
   */
  public final Thread getFirstQueuedThread() {
    // handle only fast path, else relay
    return (head == tail) ? null : fullGetFirstQueuedThread();
  }

  /**
   * Version of getFirstQueuedThread called when fastpath fails
   */
  private Thread fullGetFirstQueuedThread() {
    /*
     * The first node is normally h.next. Try to get its
     * thread field, ensuring consistent reads: If thread
     * field is nulled out or s.prev is no longer head, then
     * some other thread(s) concurrently performed setHead in
     * between some of our reads. We try this twice before
     * resorting to traversal.
     */
    Node h, s;
    Thread st;
    if (((h = head) != null && (s = h.next) != null && s.prev == head && (st = s.thread) != null)
        || ((h = head) != null && (s = h.next) != null && s.prev == head && (st = s.thread) != null))
      return st;

    /*
     * Head's next field might not have been set yet, or may have
     * been unset after setHead. So we must check to see if tail
     * is actually first node. If not, we continue on, safely
     * traversing from tail back to head to find first,
     * guaranteeing termination.
     */

    Node t = tail;
    Thread firstThread = null;
    while (t != null && t != head) {
      Thread tt = t.thread;
      if (tt != null)
        firstThread = tt;
      t = t.prev;
    }
    return firstThread;
  }

  /**
   * Returns true if the given thread is currently queued.
   * 
   * <p>
   * This implementation traverses the queue to determine presence of the given
   * thread.
   * 
   * @param thread
   *          the thread
   * @return {@code true} if the given thread is on the queue
   * @throws NullPointerException
   *           if the thread is null
   */
  public final boolean isQueued(Thread thread) {
    if (thread == null)
      throw new NullPointerException();
    for (Node p = tail; p != null; p = p.prev)
      if (p.thread == thread)
        return true;
    return false;
  }

  /**
   * Return {@code true} if the apparent first queued thread, if one exists, is
   * not waiting in exclusive mode. Used only as a heuristic in
   * ReentrantReadWriteLock.
   */
  final boolean apparentlyFirstQueuedIsExclusive() {
    Node h, s;
    return ((h = head) != null && (s = h.next) != null && s.nextWaiter != Node.SHARED);
  }

  /**
   * Return {@code true} if the queue is empty or if the given thread is at the
   * head of the queue. This is reliable only if <tt>current</tt> is actually
   * Thread.currentThread() of caller.
   */
  final boolean isFirst(Thread current) {
    Node h, s;
    return ((h = head) == null || ((s = h.next) != null && s.thread == current) || fullIsFirst(current));
  }

  final boolean fullIsFirst(Thread current) {
    // same idea as fullGetFirstQueuedThread
    Node h, s;
    Thread firstThread = null;
    if (((h = head) != null && (s = h.next) != null && s.prev == head && (firstThread = s.thread) != null))
      return firstThread == current;
    Node t = tail;
    while (t != null && t != head) {
      Thread tt = t.thread;
      if (tt != null)
        firstThread = tt;
      t = t.prev;
    }
    return firstThread == current || firstThread == null;
  }

  // Instrumentation and monitoring methods

  /**
   * Returns an estimate of the number of threads waiting to acquire. The value
   * is only an estimate because the number of threads may change dynamically
   * while this method traverses internal data structures. This method is
   * designed for use in monitoring system state, not for synchronization
   * control.
   * 
   * @return the estimated number of threads waiting to acquire
   */
  public final int getQueueLength() {
    int n = 0;
    for (Node p = tail; p != null; p = p.prev) {
      if (p.thread != null)
        ++n;
    }
    return n;
  }

  /**
   * Returns a collection containing threads that may be waiting to acquire.
   * Because the actual set of threads may change dynamically while constructing
   * this result, the returned collection is only a best-effort estimate. The
   * elements of the returned collection are in no particular order. This method
   * is designed to facilitate construction of subclasses that provide more
   * extensive monitoring facilities.
   * 
   * @return the collection of threads
   */
  public final Collection<Thread> getQueuedThreads() {
    ArrayList<Thread> list = new ArrayList<Thread>();
    for (Node p = tail; p != null; p = p.prev) {
      Thread t = p.thread;
      if (t != null)
        list.add(t);
    }
    return list;
  }

  /**
   * Returns a collection containing threads that may be waiting to acquire in
   * exclusive mode. This has the same properties as {@link #getQueuedThreads}
   * except that it only returns those threads waiting due to an exclusive
   * acquire.
   * 
   * @return the collection of threads
   */
  public final Collection<Thread> getExclusiveQueuedThreads() {
    ArrayList<Thread> list = new ArrayList<Thread>();
    for (Node p = tail; p != null; p = p.prev) {
      if (!p.isShared()) {
        Thread t = p.thread;
        if (t != null)
          list.add(t);
      }
    }
    return list;
  }

  /**
   * Returns a collection containing threads that may be waiting to acquire in
   * shared mode. This has the same properties as {@link #getQueuedThreads}
   * except that it only returns those threads waiting due to a shared acquire.
   * 
   * @return the collection of threads
   */
  public final Collection<Thread> getSharedQueuedThreads() {
    ArrayList<Thread> list = new ArrayList<Thread>();
    for (Node p = tail; p != null; p = p.prev) {
      if (p.isShared()) {
        Thread t = p.thread;
        if (t != null)
          list.add(t);
      }
    }
    return list;
  }

  /**
   * Returns a string identifying this synchronizer, as well as its state. The
   * state, in brackets, includes the String {@code "State ="} followed by the
   * current value of {@link #getState}, and either {@code "nonempty"} or
   * {@code "empty"} depending on whether the queue is empty.
   * 
   * @return a string identifying this synchronizer, as well as its state
   */
  public String toString() {
    int s = getState();
    String q = hasQueuedThreads() ? "non" : "";
    return super.toString() + "[State = " + s + ", " + q + "empty queue]";
  }

  // Internal support methods for Conditions

  /**
   * Returns true if a node, always one that was initially placed on a condition
   * queue, is now waiting to reacquire on sync queue.
   * 
   * @param node
   *          the node
   * @return true if is reacquiring
   */
  final boolean isOnSyncQueue(Node node) {
    if (node.waitStatus == Node.CONDITION || node.prev == null)
      return false;
    if (node.next != null) // If has successor, it must be on queue
      return true;
    /*
     * node.prev can be non-null, but not yet on queue because
     * the CAS to place it on queue can fail. So we have to
     * traverse from tail to make sure it actually made it.  It
     * will always be near the tail in calls to this method, and
     * unless the CAS failed (which is unlikely), it will be
     * there, so we hardly ever traverse much.
     */
    return findNodeFromTail(node);
  }

  /**
   * Returns true if node is on sync queue by searching backwards from tail.
   * Called only when needed by isOnSyncQueue.
   * 
   * @return true if present
   */
  private boolean findNodeFromTail(Node node) {
    Node t = tail;
    for (;;) {
      if (t == node)
        return true;
      if (t == null)
        return false;
      t = t.prev;
    }
  }

  /**
   * Transfers a node from a condition queue onto sync queue. Returns true if
   * successful.
   * 
   * @param node
   *          the node
   * @return true if successfully transferred (else the node was cancelled
   *         before signal).
   */
  final boolean transferForSignal(Node node) {
    /*
     * If cannot change waitStatus, the node has been cancelled.
     */
    if (!compareAndSetWaitStatus(node, Node.CONDITION, 0))
      return false;

    /*
     * Splice onto queue and try to set waitStatus of predecessor to
     * indicate that thread is (probably) waiting. If cancelled or
     * attempt to set waitStatus fails, wake up to resync (in which
     * case the waitStatus can be transiently and harmlessly wrong).
     */
    Node p = enq(node);
    int c = p.waitStatus;
    if (c > 0 || !compareAndSetWaitStatus(p, c, Node.SIGNAL))
      LockSupport.unpark(node.thread);
    return true;
  }

  /**
   * Transfers node, if necessary, to sync queue after a cancelled wait. Returns
   * true if thread was cancelled before being signalled.
   * 
   * @param node
   *          its node
   * @return true if cancelled before the node was signalled.
   */
  final boolean transferAfterCancelledWait(Node node) {
    if (compareAndSetWaitStatus(node, Node.CONDITION, 0)) {
      enq(node);
      return true;
    }
    /*
     * If we lost out to a signal(), then we can't proceed
     * until it finishes its enq().  Cancelling during an
     * incomplete transfer is both rare and transient, so just
     * spin.
     */
    while (!isOnSyncQueue(node))
      Thread.yield();
    return false;
  }

  /**
   * Invokes release with current state value; returns saved state. Cancels node
   * and throws exception on failure.
   * 
   * @param node
   *          the condition node for this wait
   * @return previous sync state
   */
//  final int fullyRelease(Node node) {
//    try {
//      int savedState = getState();
//      if (release(savedState))
//        return savedState;
//    } catch (RuntimeException ex) {
//      node.waitStatus = Node.CANCELLED;
//      throw ex;
//    }
//    // reach here if release fails
//    node.waitStatus = Node.CANCELLED;
//    throw new IllegalMonitorStateException();
//  }

  /**
   * CAS head field. Used only by enq
   */
  private final boolean compareAndSetHead(Node update) {
    // return unsafe.compareAndSwapObject(this, headOffset, null, update);
    return headUpdater.compareAndSet(this, null, update);
  }

  /**
   * CAS tail field. Used only by enq
   */
  private final boolean compareAndSetTail(Node expect, Node update) {
    // return unsafe.compareAndSwapObject(this, tailOffset, expect, update);
    return tailUpdater.compareAndSet(this, expect, update);
  }

  /**
   * CAS waitStatus field of a node.
   */
  private final static boolean compareAndSetWaitStatus(Node node, int expect,
      int update) {
    // return unsafe.compareAndSwapInt(node, waitStatusOffset,
    // expect, update);
    return nodeStatusUpdater.compareAndSet(node, expect, update);
  }

  /**
   * CAS next field of a node.
   */
  private final static boolean compareAndSetNext(Node node, Node expect,
      Node update) {
    // return unsafe.compareAndSwapObject(node, nextOffset, expect, update);
    return nextNodeUpdater.compareAndSet(node, expect, update);
  }
}
