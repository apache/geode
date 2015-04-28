/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: QueuedExecutor.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  21Jun1998  dl               Create public version
  28aug1998  dl               rely on ThreadFactoryUser, restart now public
   4may1999  dl               removed redundant interrupt detect
   7sep2000  dl               new shutdown methods
  20may2004  dl               can shutdown even if thread not created yet
*/

package com.gemstone.org.jgroups.oswego.concurrent;


/**
 * 
 * An implementation of Executor that queues incoming
 * requests until they can be processed by a single background
 * thread.
 * <p>
 * The thread is not actually started until the first 
 * <code>execute</code> request is encountered. Also, if the
 * thread is stopped for any reason (for example, after hitting
 * an unrecoverable exception in an executing task), one is started 
 * upon encountering a new request, or if <code>restart()</code> is
 * invoked.
 * <p>
 * Beware that, especially in situations
 * where command objects themselves invoke execute, queuing can
 * sometimes lead to lockups, since commands that might allow
 * other threads to terminate do not run at all when they are in the queue.
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/
public class QueuedExecutor extends ThreadFactoryUser implements Executor {


  
  /** The thread used to process commands **/
  protected Thread thread_;

  /** Special queue element to signal termination **/
  protected final/*GemStoneAddition*/ static Runnable ENDTASK = new Runnable() { public void run() {} };

  /** true if thread should shut down after processing current task **/
  protected volatile boolean shutdown_; // latches true;
  
  /**
   * Return the thread being used to process commands, or
   * null if there is no such thread. You can use this
   * to invoke any special methods on the thread, for
   * example, to interrupt it.
   **/
  public synchronized Thread getThread() { 
    return thread_;
  }

  /** set thread_ to null to indicate termination **/
  protected synchronized void clearThread() {
    thread_ = null;
  }


  /** The queue **/
  protected final Channel queue_;


  /**
   * The runloop is isolated in its own Runnable class
   * just so that the main
   * class need not implement Runnable,  which would
   * allow others to directly invoke run, which would
   * never make sense here.
   **/
  protected class RunLoop implements Runnable {
    public void run() {
      try {
        while (!shutdown_) {
          Runnable task = (Runnable)(queue_.take());
          if (task == ENDTASK) {
            shutdown_ = true;
            break;
          }
          else if (task != null) {
            task.run();
            task = null;
          }
          else
            break;
        }
      }
      catch (InterruptedException ex) {
        Thread.currentThread().interrupt(); // GemStoneAddition
      } // fallthrough
      finally {
        clearThread();
      }
    }
  }

  protected final RunLoop runLoop_;


  /**
   * Construct a new QueuedExecutor that uses
   * the supplied Channel as its queue. 
   * <p>
   * This class does not support any methods that 
   * reveal this queue. If you need to access it
   * independently (for example to invoke any
   * special status monitoring operations), you
   * should record a reference to it separately.
   **/

  public QueuedExecutor(Channel queue) {
    queue_ = queue;
    runLoop_ = new RunLoop();
  }

  /**
   * Construct a new QueuedExecutor that uses
   * a BoundedLinkedQueue with the current
   * DefaultChannelCapacity as its queue.
   **/

  public QueuedExecutor() {
    this(new BoundedLinkedQueue());
  }

  /**
   * Start (or restart) the background thread to process commands. It has
   * no effect if a thread is already running. This
   * method can be invoked if the background thread crashed
   * due to an unrecoverable exception.
   **/

  public synchronized void restart() {
    if (thread_ == null && !shutdown_) {
      thread_ = threadFactory_.newThread(runLoop_);
      thread_.start();
    }
  }


  /** 
   * Arrange for execution of the command in the
   * background thread by adding it to the queue. 
   * The method may block if the channel's put
   * operation blocks.
   * <p>
   * If the background thread
   * does not exist, it is created and started.
   **/
  public void execute(Runnable command) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    restart();
    queue_.put(command);
  }

  /**
   * Terminate background thread after it processes all
   * elements currently in queue. Any tasks entered after this point will
   * not be processed. A shut down thread cannot be restarted.
   * This method may block if the task queue is finite and full.
   * Also, this method 
   * does not in general apply (and may lead to comparator-based
   * exceptions) if the task queue is a priority queue.
   **/
  public synchronized void shutdownAfterProcessingCurrentlyQueuedTasks() {
    if (!shutdown_) {
      try { queue_.put(ENDTASK); }
      catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
  }


  /**
   * Terminate background thread after it processes the 
   * current task, removing other queued tasks and leaving them unprocessed.
   * A shut down thread cannot be restarted.
   **/
  public synchronized void shutdownAfterProcessingCurrentTask() {
    shutdown_ = true;
    try { 
      while (queue_.poll(0) != null) ; // drain
      queue_.put(ENDTASK); 
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }


  /**
   * Terminate background thread even if it is currently processing
   * a task. This method uses Thread.interrupt, so relies on tasks
   * themselves responding appropriately to interruption. If the
   * current tasks does not terminate on interruption, then the 
   * thread will not terminate until processing current task.
   * A shut down thread cannot be restarted.
   **/
  public synchronized void shutdownNow() {
    shutdown_ = true;
    Thread t = thread_;
    if (t != null) 
      t.interrupt();
    shutdownAfterProcessingCurrentTask();
  }
}
