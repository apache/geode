/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: PooledExecutor.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  19Jun1998  dl               Create public version
  29aug1998  dl               rely on ThreadFactoryUser, 
                              remove ThreadGroup-based methods
                              adjusted locking policies
   3mar1999  dl               Worker threads sense decreases in pool size
  31mar1999  dl               Allow supplied channel in constructor;
                              add methods createThreads, drain
  15may1999  dl               Allow infinite keepalives
  21oct1999  dl               add minimumPoolSize methods
   7sep2000  dl               BlockedExecutionHandler now an interface,
                              new DiscardOldestWhenBlocked policy
  12oct2000  dl               add shutdownAfterProcessingCurrentlyQueuedTasks
  13nov2000  dl               null out task ref after run 
  08apr2001  dl               declare inner class ctor protected 
  12nov2001  dl               Better shutdown support
                              Blocked exec handlers can throw IE
                              Simplify locking scheme
  25jan2001  dl               {get,set}BlockedExecutionHandler now public
  17may2002  dl               null out task var in worker run to enable GC.
  30aug2003  dl               check for new tasks when timing out
  18feb2004  dl               replace dead thread if no others left
*/

package com.gemstone.org.jgroups.oswego.concurrent;

import java.util.*;

/**
 * A tunable, extensible thread pool class. The main supported public
 * method is <code>execute(Runnable command)</code>, which can be
 * called instead of directly creating threads to execute commands.
 *
 * <p>
 * Thread pools can be useful for several, usually intertwined
 * reasons:
 *
 * <ul>
 *
 *    <li> To bound resource use. A limit can be placed on the maximum
 *    number of simultaneously executing threads.
 *
 *    <li> To manage concurrency levels. A targeted number of threads
 *    can be allowed to execute simultaneously.
 *
 *    <li> To manage a set of threads performing related tasks.
 *
 *    <li> To minimize overhead, by reusing previously constructed
 *    Thread objects rather than creating new ones.  (Note however
 *    that pools are hardly ever cure-alls for performance problems
 *    associated with thread construction, especially on JVMs that
 *    themselves internally pool or recycle threads.)  
 *
 * </ul>
 *
 * These goals introduce a number of policy parameters that are
 * encapsulated in this class. All of these parameters have defaults
 * and are tunable, either via get/set methods, or, in cases where
 * decisions should hold across lifetimes, via methods that can be
 * easily overridden in subclasses.  The main, most commonly set
 * parameters can be established in constructors.  Policy choices
 * across these dimensions can and do interact.  Be careful, and
 * please read this documentation completely before using!  See also
 * the usage examples below.
 *
 * <dl>
 *   <dt> Queueing 
 *
 *   <dd> By default, this pool uses queueless synchronous channels to
 *   to hand off work to threads. This is a safe, conservative policy
 *   that avoids lockups when handling sets of requests that might
 *   have internal dependencies. (In these cases, queuing one task
 *   could lock up another that would be able to continue if the
 *   queued task were to run.)  If you are sure that this cannot
 *   happen, then you can instead supply a queue of some sort (for
 *   example, a BoundedBuffer or LinkedQueue) in the constructor.
 *   This will cause new commands to be queued in cases where all
 *   MaximumPoolSize threads are busy. Queues are sometimes
 *   appropriate when each task is completely independent of others,
 *   so tasks cannot affect each others execution.  For example, in an
 *   http server.  <p>
 *
 *   When given a choice, this pool always prefers adding a new thread
 *   rather than queueing if there are currently fewer than the
 *   current getMinimumPoolSize threads running, but otherwise always
 *   prefers queuing a request rather than adding a new thread. Thus,
 *   if you use an unbounded buffer, you will never have more than
 *   getMinimumPoolSize threads running. (Since the default
 *   minimumPoolSize is one, you will probably want to explicitly
 *   setMinimumPoolSize.)  <p>
 *
 *   While queuing can be useful in smoothing out transient bursts of
 *   requests, especially in socket-based services, it is not very
 *   well behaved when commands continue to arrive on average faster
 *   than they can be processed.  Using bounds for both the queue and
 *   the pool size, along with run-when-blocked policy is often a
 *   reasonable response to such possibilities.  <p>
 *
 *   Queue sizes and maximum pool sizes can often be traded off for
 *   each other. Using large queues and small pools minimizes CPU
 *   usage, OS resources, and context-switching overhead, but can lead
 *   to artifically low throughput. Especially if tasks frequently
 *   block (for example if they are I/O bound), a JVM and underlying
 *   OS may be able to schedule time for more threads than you
 *   otherwise allow. Use of small queues or queueless handoffs
 *   generally requires larger pool sizes, which keeps CPUs busier but
 *   may encounter unacceptable scheduling overhead, which also
 *   decreases throughput.  <p>
 *
 *   <dt> Maximum Pool size
 *
 *   <dd> The maximum number of threads to use, when needed.  The pool
 *   does not by default preallocate threads.  Instead, a thread is
 *   created, if necessary and if there are fewer than the maximum,
 *   only when an <code>execute</code> request arrives.  The default
 *   value is (for all practical purposes) infinite --
 *   <code>Integer.MAX_VALUE</code>, so should be set in the
 *   constructor or the set method unless you are just using the pool
 *   to minimize construction overhead.  Because task handoffs to idle
 *   worker threads require synchronization that in turn relies on JVM
 *   scheduling policies to ensure progress, it is possible that a new
 *   thread will be created even though an existing worker thread has
 *   just become idle but has not progressed to the point at which it
 *   can accept a new task. This phenomenon tends to occur on some
 *   JVMs when bursts of short tasks are executed.  <p>
 *
 *   <dt> Minimum Pool size
 *
 *   <dd> The minimum number of threads to use, when needed (default
 *   1).  When a new request is received, and fewer than the minimum
 *   number of threads are running, a new thread is always created to
 *   handle the request even if other worker threads are idly waiting
 *   for work. Otherwise, a new thread is created only if there are
 *   fewer than the maximum and the request cannot immediately be
 *   queued.  <p>
 *
 *   <dt> Preallocation
 *
 *   <dd> You can override lazy thread construction policies via
 *   method createThreads, which establishes a given number of warm
 *   threads. Be aware that these preallocated threads will time out
 *   and die (and later be replaced with others if needed) if not used
 *   within the keep-alive time window. If you use preallocation, you
 *   probably want to increase the keepalive time.  The difference
 *   between setMinimumPoolSize and createThreads is that
 *   createThreads immediately establishes threads, while setting the
 *   minimum pool size waits until requests arrive.  <p>
 *
 *   <dt> Keep-alive time
 *
 *   <dd> If the pool maintained references to a fixed set of threads
 *   in the pool, then it would impede garbage collection of otherwise
 *   idle threads. This would defeat the resource-management aspects
 *   of pools. One solution would be to use weak references.  However,
 *   this would impose costly and difficult synchronization issues.
 *   Instead, threads are simply allowed to terminate and thus be
 *   GCable if they have been idle for the given keep-alive time.  The
 *   value of this parameter represents a trade-off between GCability
 *   and construction time. In most current Java VMs, thread
 *   construction and cleanup overhead is on the order of
 *   milliseconds. The default keep-alive value is one minute, which
 *   means that the time needed to construct and then GC a thread is
 *   expended at most once per minute.  
 *   <p> 
 *
 *   To establish worker threads permanently, use a <em>negative</em>
 *   argument to setKeepAliveTime.  <p>
 *
 *   <dt> Blocked execution policy
 *
 *   <dd> If the maximum pool size or queue size is bounded, then it
 *   is possible for incoming <code>execute</code> requests to
 *   block. There are four supported policies for handling this
 *   problem, and mechanics (based on the Strategy Object pattern) to
 *   allow others in subclasses: <p>
 *
 *   <dl>
 *     <dt> Run (the default)
 *     <dd> The thread making the <code>execute</code> request
 *          runs the task itself. This policy helps guard against lockup. 
 *     <dt> Wait
 *     <dd> Wait until a thread becomes available.  This
 *          policy should, in general, not be used if the minimum number of
 *          of threads is zero, in which case a thread may never become
 *          available.
 *     <dt> Abort
 *     <dd> Throw a RuntimeException
 *     <dt> Discard 
 *     <dd> Throw away the current request and return.
 *     <dt> DiscardOldest
 *     <dd> Throw away the oldest request and return.
 *   </dl>
 *
 *   Other plausible policies include raising the maximum pool size
 *   after checking with some other objects that this is OK.  <p>
 *
 *   These cases can never occur if the maximum pool size is unbounded
 *   or the queue is unbounded.  In these cases you instead face
 *   potential resource exhaustion.)  The execute method does not
 *   throw any checked exceptions in any of these cases since any
 *   errors associated with them must normally be dealt with via
 *   handlers or callbacks. (Although in some cases, these might be
 *   associated with throwing unchecked exceptions.)  You may wish to
 *   add special implementations even if you choose one of the listed
 *   policies. For example, the supplied Discard policy does not
 *   inform the caller of the drop. You could add your own version
 *   that does so.  Since choice of policies is normally a system-wide
 *   decision, selecting a policy affects all calls to
 *   <code>execute</code>.  If for some reason you would instead like
 *   to make per-call decisions, you could add variant versions of the
 *   <code>execute</code> method (for example,
 *   <code>executeIfWouldNotBlock</code>) in subclasses.  <p>
 *
 *   <dt> Thread construction parameters
 *
 *   <dd> A settable ThreadFactory establishes each new thread.  By
 *   default, it merely generates a new instance of class Thread, but
 *   can be changed to use a Thread subclass, to set priorities,
 *   ThreadLocals, etc.  <p>
 *
 *   <dt> Interruption policy
 *
 *   <dd> Worker threads check for interruption after processing each
 *   command, and terminate upon interruption.  Fresh threads will
 *   replace them if needed. Thus, new tasks will not start out in an
 *   interrupted state due to an uncleared interruption in a previous
 *   task. Also, unprocessed commands are never dropped upon
 *   interruption. It would conceptually suffice simply to clear
 *   interruption between tasks, but implementation characteristics of
 *   interruption-based methods are uncertain enough to warrant this
 *   conservative strategy. It is a good idea to be equally
 *   conservative in your code for the tasks running within pools.
 *   <p>
 *
 *   <dt> Shutdown policy
 *
 *   <dd> The interruptAll method interrupts, but does not disable the
 *   pool. Two different shutdown methods are supported for use when
 *   you do want to (permanently) stop processing tasks. Method
 *   shutdownAfterProcessingCurrentlyQueuedTasks waits until all
 *   current tasks are finished. The shutDownNow method interrupts
 *   current threads and leaves other queued requests unprocessed.
 *   <p>
 *
 *   <dt> Handling requests after shutdown
 *
 *   <dd> When the pool is shutdown, new incoming requests are handled
 *   by the blockedExecutionHandler. By default, the handler is set to
 *   discard new requests, but this can be set with an optional
 *   argument to method
 *   shutdownAfterProcessingCurrentlyQueuedTasks. <p> Also, if you are
 *   using some form of queuing, you may wish to call method drain()
 *   to remove (and return) unprocessed commands from the queue after
 *   shutting down the pool and its clients. If you need to be sure
 *   these commands are processed, you can then run() each of the
 *   commands in the list returned by drain().
 *
 * </dl>
 * <p>
 *
 * <b>Usage examples.</b>
 * <p>
 *
 * Probably the most common use of pools is in statics or singletons
 * accessible from a number of classes in a package; for example:
 *
 * <pre>
 * class MyPool {
 *   // initialize to use a maximum of 8 threads.
 *   static PooledExecutor pool = new PooledExecutor(8);
 * }
 * </pre>
 * Here are some sample variants in initialization:
 * <ol>
 *  <li> Using a bounded buffer of 10 tasks, at least 4 threads (started only
 *       when needed due to incoming requests), but allowing
 *       up to 100 threads if the buffer gets full.
 *     <pre>
 *        pool = new PooledExecutor(new BoundedBuffer(10), 100);
 *        pool.setMinimumPoolSize(4);
 *     </pre>
 *  <li> Same as (1), except pre-start 9 threads, allowing them to
 *        die if they are not used for five minutes.
 *     <pre>
 *        pool = new PooledExecutor(new BoundedBuffer(10), 100);
 *        pool.setMinimumPoolSize(4);
 *        pool.setKeepAliveTime(1000 * 60 * 5);
 *        pool.createThreads(9);
 *     </pre>
 *  <li> Same as (2) except clients abort if both the buffer is full and
 *       all 100 threads are busy:
 *     <pre>
 *        pool = new PooledExecutor(new BoundedBuffer(10), 100);
 *        pool.setMinimumPoolSize(4);
 *        pool.setKeepAliveTime(1000 * 60 * 5);
 *        pool.abortWhenBlocked();
 *        pool.createThreads(9);
 *     </pre>
 *  <li> An unbounded queue serviced by exactly 5 threads:
 *     <pre>
 *        pool = new PooledExecutor(new LinkedQueue());
 *        pool.setKeepAliveTime(-1); // live forever
 *        pool.createThreads(5);
 *     </pre>
 *  </ol>
 *
 * <p>
 * <b>Usage notes.</b>
 * <p>
 *
 * Pools do not mesh well with using thread-specific storage via
 * java.lang.ThreadLocal.  ThreadLocal relies on the identity of a
 * thread executing a particular task. Pools use the same thread to
 * perform different tasks.  <p>
 *
 * If you need a policy not handled by the parameters in this class
 * consider writing a subclass.  <p>
 *
 * Version note: Previous versions of this class relied on
 * ThreadGroups for aggregate control. This has been removed, and the
 * method interruptAll added, to avoid differences in behavior across
 * JVMs.
 *
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/

public class PooledExecutor extends ThreadFactoryUser implements Executor {

  /** 
   * The maximum pool size; used if not otherwise specified.  Default
   * value is essentially infinite (Integer.MAX_VALUE)
   **/
  public static final int  DEFAULT_MAXIMUMPOOLSIZE = Integer.MAX_VALUE;

  /** 
   * The minimum pool size; used if not otherwise specified.  Default
   * value is 1.
   **/
  public static final int  DEFAULT_MINIMUMPOOLSIZE = 1;

  /**
   * The maximum time to keep worker threads alive waiting for new
   * tasks; used if not otherwise specified. Default value is one
   * minute (60000 milliseconds).
   **/
  public static final long DEFAULT_KEEPALIVETIME = 60 * 1000;

  /** The maximum number of threads allowed in pool. **/
  protected int maximumPoolSize_ = DEFAULT_MAXIMUMPOOLSIZE;

  /** The minumum number of threads to maintain in pool. **/
  protected int minimumPoolSize_ = DEFAULT_MINIMUMPOOLSIZE;

  /**  Current pool size.  **/
  protected int poolSize_ = 0;

  /** The maximum time for an idle thread to wait for new task. **/
  protected long keepAliveTime_ = DEFAULT_KEEPALIVETIME;

  /** 
   * Shutdown flag - latches true when a shutdown method is called 
   * in order to disable queuing/handoffs of new tasks.
   **/
  protected boolean shutdown_ = false;

  /**
   * The channel used to hand off the command to a thread in the pool.
   **/
  protected final Channel handOff_;

  /**
   * The set of active threads, declared as a map from workers to
   * their threads.  This is needed by the interruptAll method.  It
   * may also be useful in subclasses that need to perform other
   * thread management chores.
   **/
  protected final Map threads_;

  /** The current handler for unserviceable requests. **/
  protected BlockedExecutionHandler blockedExecutionHandler_;

  /** 
   * Create a new pool with all default settings
   **/

  public PooledExecutor() {
    this (new SynchronousChannel(), DEFAULT_MAXIMUMPOOLSIZE);
  }

  /** 
   * Create a new pool with all default settings except
   * for maximum pool size.
   **/

  public PooledExecutor(int maxPoolSize) {
    this(new SynchronousChannel(), maxPoolSize);
  }

  /** 
   * Create a new pool that uses the supplied Channel for queuing, and
   * with all default parameter settings.
   **/

  public PooledExecutor(Channel channel) {
    this(channel, DEFAULT_MAXIMUMPOOLSIZE);
  }

  /** 
   * Create a new pool that uses the supplied Channel for queuing, and
   * with all default parameter settings except for maximum pool size.
   **/

  public PooledExecutor(Channel channel, int maxPoolSize) {
    maximumPoolSize_ = maxPoolSize;
    handOff_ = channel;
    runWhenBlocked();
    threads_ = new HashMap();
  }
  
  /** 
   * Return the maximum number of threads to simultaneously execute
   * New unqueued requests will be handled according to the current
   * blocking policy once this limit is exceeded.
   **/
  public synchronized int getMaximumPoolSize() { 
    return maximumPoolSize_; 
  }

  /** 
   * Set the maximum number of threads to use. Decreasing the pool
   * size will not immediately kill existing threads, but they may
   * later die when idle.
   * @exception IllegalArgumentException if less or equal to zero.
   * (It is
   * not considered an error to set the maximum to be less than than
   * the minimum. However, in this case there are no guarantees
   * about behavior.)
   **/
  public synchronized void setMaximumPoolSize(int newMaximum) { 
    if (newMaximum <= 0) throw new IllegalArgumentException();
    maximumPoolSize_ = newMaximum; 
  }

  /** 
   * Return the minimum number of threads to simultaneously execute.
   * (Default value is 1).  If fewer than the mininum number are
   * running upon reception of a new request, a new thread is started
   * to handle this request.
   **/
  public synchronized int getMinimumPoolSize() { 
    return minimumPoolSize_; 
  }

  /** 
   * Set the minimum number of threads to use. 
   * @exception IllegalArgumentException if less than zero. (It is not
   * considered an error to set the minimum to be greater than the
   * maximum. However, in this case there are no guarantees about
   * behavior.)
   **/
  public synchronized void setMinimumPoolSize(int newMinimum) { 
    if (newMinimum < 0) throw new IllegalArgumentException();
    minimumPoolSize_ = newMinimum; 
  }
  
  /** 
   * Return the current number of active threads in the pool.  This
   * number is just a snaphot, and may change immediately upon
   * returning
   **/
  public synchronized int getPoolSize() { 
    return poolSize_; 
  }

  /** 
   * Return the number of milliseconds to keep threads alive waiting
   * for new commands. A negative value means to wait forever. A zero
   * value means not to wait at all.
   **/
  public synchronized long getKeepAliveTime() { 
    return keepAliveTime_; 
  }

  /** 
   * Set the number of milliseconds to keep threads alive waiting for
   * new commands. A negative value means to wait forever. A zero
   * value means not to wait at all.
   **/
  public synchronized void setKeepAliveTime(long msecs) { 
    keepAliveTime_ = msecs; 
  }

  /** Get the handler for blocked execution **/
  public synchronized BlockedExecutionHandler getBlockedExecutionHandler() {
    return blockedExecutionHandler_;
  }

  /** Set the handler for blocked execution **/
  public synchronized void setBlockedExecutionHandler(BlockedExecutionHandler h) {
    blockedExecutionHandler_ = h;
  }

  /**
   * Create and start a thread to handle a new command.  Call only
   * when holding lock.
   **/
  protected void addThread(Runnable command) {
    Worker worker = new Worker(command);
    Thread thread = getThreadFactory().newThread(worker);
    threads_.put(worker, thread);
    ++poolSize_;
    thread.start();
  }

  /**
   * Create and start up to numberOfThreads threads in the pool.
   * Return the number created. This may be less than the number
   * requested if creating more would exceed maximum pool size bound.
   **/
  public int createThreads(int numberOfThreads) {
    int ncreated = 0;
    for (int i = 0; i < numberOfThreads; ++i) {
      synchronized(this) { 
        if (poolSize_ < maximumPoolSize_) {
          addThread(null);
          ++ncreated;
        }
        else 
          break;
      }
    }
    return ncreated;
  }

  /**
   * Interrupt all threads in the pool, causing them all to
   * terminate. Assuming that executed tasks do not disable (clear)
   * interruptions, each thread will terminate after processing its
   * current task. Threads will terminate sooner if the executed tasks
   * themselves respond to interrupts.
   **/
  public synchronized void interruptAll() {
    for (Iterator it = threads_.values().iterator(); it.hasNext(); ) {
      Thread t = (Thread)(it.next());
      t.interrupt();
    }
  }

  /**
   * Interrupt all threads and disable construction of new
   * threads. Any tasks entered after this point will be discarded. A
   * shut down pool cannot be restarted.
   */
  public void shutdownNow() {
    shutdownNow(new DiscardWhenBlocked());
  }

  /**
   * Interrupt all threads and disable construction of new
   * threads. Any tasks entered after this point will be handled by
   * the given BlockedExecutionHandler.  A shut down pool cannot be
   * restarted.
   */
  public synchronized void shutdownNow(BlockedExecutionHandler handler) {
    setBlockedExecutionHandler(handler);
    shutdown_ = true; // don't allow new tasks
    minimumPoolSize_ = maximumPoolSize_ = 0; // don't make new threads
    interruptAll(); // interrupt all existing threads
  }

  /**
   * Terminate threads after processing all elements currently in
   * queue. Any tasks entered after this point will be discarded. A
   * shut down pool cannot be restarted.
   **/
  public void shutdownAfterProcessingCurrentlyQueuedTasks() {
    shutdownAfterProcessingCurrentlyQueuedTasks(new DiscardWhenBlocked());
  }

  /**
   * Terminate threads after processing all elements currently in
   * queue. Any tasks entered after this point will be handled by the
   * given BlockedExecutionHandler.  A shut down pool cannot be
   * restarted.
   **/
  public synchronized void shutdownAfterProcessingCurrentlyQueuedTasks(BlockedExecutionHandler handler) {
    setBlockedExecutionHandler(handler);
    shutdown_ = true;
    if (poolSize_ == 0) // disable new thread construction when idle
      minimumPoolSize_ = maximumPoolSize_ = 0;
  }

  /** 
   * Return true if a shutDown method has succeeded in terminating all
   * threads.
   */
  public synchronized boolean isTerminatedAfterShutdown() {
    return shutdown_ && poolSize_ == 0;
  }

  /**
   * Wait for a shutdown pool to fully terminate, or until the timeout
   * has expired. This method may only be called <em>after</em>
   * invoking shutdownNow or
   * shutdownAfterProcessingCurrentlyQueuedTasks.
   *
   * @param maxWaitTime  the maximum time in milliseconds to wait
   * @return true if the pool has terminated within the max wait period
   * @exception IllegalStateException if shutdown has not been requested
   * @exception InterruptedException if the current thread has been interrupted in the course of waiting
   */
  public synchronized boolean awaitTerminationAfterShutdown(long maxWaitTime) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    if (!shutdown_)
      throw new IllegalStateException();
    if (poolSize_ == 0)
      return true;
    long waitTime = maxWaitTime;
    if (waitTime <= 0)
      return false;
    long start = System.currentTimeMillis();
    for (;;) {
      wait(waitTime);
      if (poolSize_ == 0)
        return true;
      waitTime = maxWaitTime - (System.currentTimeMillis() - start);
      if (waitTime <= 0) 
        return false;
    }
  }

  /**
   * Wait for a shutdown pool to fully terminate.  This method may
   * only be called <em>after</em> invoking shutdownNow or
   * shutdownAfterProcessingCurrentlyQueuedTasks.
   *
   * @exception IllegalStateException if shutdown has not been requested
   * @exception InterruptedException if the current thread has been interrupted in the course of waiting
   */
  public synchronized void awaitTerminationAfterShutdown() throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition in case poolSize_ is 0
    if (!shutdown_)
      throw new IllegalStateException();
    while (poolSize_ > 0)
      wait();
  }

  /**
   * Remove all unprocessed tasks from pool queue, and return them in
   * a java.util.List. Thsi method should be used only when there are
   * not any active clients of the pool. Otherwise you face the
   * possibility that the method will loop pulling out tasks as
   * clients are putting them in.  This method can be useful after
   * shutting down a pool (via shutdownNow) to determine whether there
   * are any pending tasks that were not processed.  You can then, for
   * example execute all unprocessed commands via code along the lines
   * of:
   *
   * <pre>
   *   List tasks = pool.drain();
   *   for (Iterator it = tasks.iterator(); it.hasNext();) 
   *     ( (Runnable)(it.next()) ).run();
   * </pre>
   **/
  public List drain() {
//    boolean wasInterrupted = false; GemStoneAddition
    Vector tasks = new Vector();
    for (;;) {
      boolean interrupted = Thread.interrupted(); // GemStoneAddition
      try {
        Object x = handOff_.poll(0);
        if (x == null) 
          break;
        else
          tasks.addElement(x);
      }
      catch (InterruptedException ex) {
//        wasInterrupted = true; // postpone re-interrupt until drained
        interrupted = true;
      }
      finally { // GemStoneAddition
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
//    if (wasInterrupted) Thread.currentThread().interrupt();
    return tasks;
  }
  
  /** 
   * Cleanup method called upon termination of worker thread.
   **/
  protected synchronized void workerDone(Worker w) {
    threads_.remove(w);
    if (--poolSize_ == 0 && shutdown_) { 
      maximumPoolSize_ = minimumPoolSize_ = 0; // disable new threads
      notifyAll(); // notify awaitTerminationAfterShutdown
    }

    // Create a replacement if needed
    if (poolSize_ == 0 || poolSize_ < minimumPoolSize_) {
      try {
         Runnable r = (Runnable)(handOff_.poll(0));
         if (r != null && !shutdown_) // just consume task if shut down
           addThread(r);
      } catch(InterruptedException ie) {
        return;
      }
    }
  }

  /** 
   * Get a task from the handoff queue, or null if shutting down.
   **/
  protected Runnable getTask() throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    long waitTime;
    synchronized(this) {
      if (poolSize_ > maximumPoolSize_) // Cause to die if too many threads
        return null;
      waitTime = (shutdown_)? 0 : keepAliveTime_;
    }
    if (waitTime >= 0) 
      return (Runnable)(handOff_.poll(waitTime));
    else 
      return (Runnable)(handOff_.take());
  }
  

  /**
   * Class defining the basic run loop for pooled threads.
   **/
  protected class Worker implements Runnable {
    protected Runnable firstTask_;

    protected Worker(Runnable firstTask) { firstTask_ = firstTask; }

    public void run() {
      try {
        Runnable task = firstTask_;
        firstTask_ = null; // enable GC

        if (task != null) {
          task.run();
          task = null;
        }
        
        while ( (task = getTask()) != null) {
          task.run();
          task = null;
        }
      }
      catch (InterruptedException ex) { 
        Thread.currentThread().interrupt(); // GemStoneAddition
        // fall through
      }
      finally {
        workerDone(this);
      }
    }
  }

  /**
   * Class for actions to take when execute() blocks. Uses Strategy
   * pattern to represent different actions. You can add more in
   * subclasses, and/or create subclasses of these. If so, you will
   * also want to add or modify the corresponding methods that set the
   * current blockedExectionHandler_.
   **/
  public interface BlockedExecutionHandler {
    /** 
     * Return true if successfully handled so, execute should
     * terminate; else return false if execute loop should be retried.
     **/
    boolean blockedAction(Runnable command) throws InterruptedException;
  }

  /** Class defining Run action. **/
  static/*GemStoneAddition*/ protected class RunWhenBlocked implements BlockedExecutionHandler {
    public boolean blockedAction(Runnable command) {
      command.run();
      return true;
    }
  }

  /** 
   * Set the policy for blocked execution to be that the current
   * thread executes the command if there are no available threads in
   * the pool.
   **/
  public void runWhenBlocked() {
    setBlockedExecutionHandler(new RunWhenBlocked());
  }

  /** Class defining Wait action. **/
  protected class WaitWhenBlocked implements BlockedExecutionHandler {
    public boolean blockedAction(Runnable command) throws InterruptedException{
      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
      synchronized(PooledExecutor.this) {
        if (shutdown_)
          return true;
      }
      handOff_.put(command);
      return true;
    }
  }

  /** 
   * Set the policy for blocked execution to be to wait until a thread
   * is available, unless the pool has been shut down, in which case
   * the action is discarded.
   **/
  public void waitWhenBlocked() {
    setBlockedExecutionHandler(new WaitWhenBlocked());
  }

  /** Class defining Discard action. **/
  static/*GemStoneAddition*/ protected class DiscardWhenBlocked implements BlockedExecutionHandler {
    public boolean blockedAction(Runnable command) {
      return true;
    }
  }

  /** 
   * Set the policy for blocked execution to be to return without
   * executing the request.
   **/
  public void discardWhenBlocked() {
    setBlockedExecutionHandler(new DiscardWhenBlocked());
  }


  /** Class defining Abort action. **/
  static/*GemStoneAddition*/ protected class AbortWhenBlocked implements BlockedExecutionHandler {
    public boolean blockedAction(Runnable command) {
      throw new RuntimeException("Pool is blocked");
    }
  }

  /** 
   * Set the policy for blocked execution to be to
   * throw a RuntimeException.
   **/
  public void abortWhenBlocked() {
    setBlockedExecutionHandler(new AbortWhenBlocked());
  }


  /**
   * Class defining DiscardOldest action.  Under this policy, at most
   * one old unhandled task is discarded.  If the new task can then be
   * handed off, it is.  Otherwise, the new task is run in the current
   * thread (i.e., RunWhenBlocked is used as a backup policy.)
   **/
  protected class DiscardOldestWhenBlocked implements BlockedExecutionHandler {
    public boolean blockedAction(Runnable command) throws InterruptedException{
      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition for safety
      handOff_.poll(0);
      if (!handOff_.offer(command, 0))
        command.run();
      return true;
    }
  }

  /** 
   * Set the policy for blocked execution to be to discard the oldest
   * unhandled request
   **/
  public void discardOldestWhenBlocked() {
    setBlockedExecutionHandler(new DiscardOldestWhenBlocked());
  }

  /**
   * Arrange for the given command to be executed by a thread in this
   * pool.  The method normally returns when the command has been
   * handed off for (possibly later) execution.
   **/
  public void execute(Runnable command) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    for (;;) {
      synchronized(this) { 
        if (!shutdown_) {
          int size = poolSize_;

          // Ensure minimum number of threads
          if (size < minimumPoolSize_) {
            addThread(command);
            return;
          }
          
          // Try to give to existing thread
          if (handOff_.offer(command, 0)) { 
            return;
          }
          
          // If cannot handoff and still under maximum, create new thread
          if (size < maximumPoolSize_) {
            addThread(command);
            return;
          }
        }
      }

      // Cannot hand off and cannot create -- ask for help
      if (getBlockedExecutionHandler().blockedAction(command)) {
        return;
      }
    }
  }
}
