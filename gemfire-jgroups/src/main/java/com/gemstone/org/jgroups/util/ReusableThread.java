/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ReusableThread.java,v 1.7 2005/01/16 01:04:52 belaban Exp $

package com.gemstone.org.jgroups.util;


import com.gemstone.org.jgroups.util.GemFireTracer;



/**
 * Reusable thread class. Instead of creating a new thread per task, this instance can be reused
 * to run different tasks in turn. This is done by looping and assigning the Runnable task objects
 * whose <code>run</code> method is then called.<br>
 * Tasks are Runnable objects and should be prepared to terminate when they receive an
 * InterruptedException. This is thrown by the stop() method.<br>
 * <p/>
 * The following situations have to be tested:
 * <ol>
 * <li>ReusableThread is started. Then, brefore assigning a task, it is stopped again
 * <li>ReusableThread is started, assigned a long running task. Then, before task is done,
 * stop() is called
 * <li>ReusableThread is started, assigned a task. Then waitUntilDone() is called, then stop()
 * <li>ReusableThread is started, assigned a number of tasks (waitUntilDone() called between tasks),
 * then stopped
 * <li>ReusableThread is started, assigned a task
 * </ol>
 *
 * @author Bela Ban
 */
public class ReusableThread implements Runnable {
    volatile Thread thread=null;  // thread that works on the task
    Runnable task=null;    // task assigned to thread
    String thread_name="ReusableThread";
    volatile boolean suspended=false;
    protected static final GemFireTracer log=GemFireTracer.getLog(ReusableThread.class);
    final long TASK_JOIN_TIME=3000; // wait 3 secs for an interrupted thread to terminate


    public ReusableThread() {
    }


    public ReusableThread(String thread_name) {
        this.thread_name=thread_name;
    }


    public boolean done() {
        return task == null;
    }

    public boolean available() {
        return done();
    }

    public boolean isAlive() {
        synchronized(this) {
            return thread != null && thread.isAlive();
        }
    }


    /**
     * Will always be called from synchronized method, no need to do our own synchronization
     */
    public void start() {
        if(thread == null || (thread != null && !thread.isAlive())) {
            thread=new Thread(this, thread_name);
            thread.setDaemon(true);
            thread.start();
        }
    }


    /**
     * Stops the thread by setting thread=null and interrupting it. The run() method catches the
     * InterruptedException and checks whether thread==null. If this is the case, it will terminate
     */
    public void stop() {
        Thread tmp=null;

        if(log.isTraceEnabled()) log.trace("entering THIS");
        synchronized(this) {
            if(log.isTraceEnabled())
                log.trace("entered THIS (thread=" + printObj(thread) +
                        ", task=" + printObj(task) + ", suspended=" + suspended + ')');
            if(thread != null && thread.isAlive()) {
                tmp=thread;
                thread=null; // signals the thread to stop
                task=null;
                if(log.isTraceEnabled()) log.trace("notifying thread");
                notifyAll();
                if(log.isTraceEnabled()) log.trace("notifying thread completed");
            }
            thread=null;
            task=null;
        }

        if(tmp != null && tmp.isAlive()) {
            long s1=System.currentTimeMillis(), s2=0;
            if(log.isTraceEnabled()) log.trace("join(" + TASK_JOIN_TIME + ')');

            tmp.interrupt();

            try {
                tmp.join(TASK_JOIN_TIME);
            }
            catch(InterruptedException e) {
              Thread.currentThread().interrupt(); // GemStoneAddition -- propagate to caller
            }
            s2=System.currentTimeMillis();
            if(log.isTraceEnabled()) log.trace("join(" + TASK_JOIN_TIME + ") completed in " + (s2 - s1));
            if(tmp.isAlive())
                if(log.isErrorEnabled()) log.error(ExternalStrings.ReusableThread_THREAD_IS_STILL_ALIVE);
            tmp=null;
        }
    }


    /**
     * Suspends the thread. Does nothing if already suspended. If a thread is waiting to be assigned a task, or
     * is currently running a (possibly long-running) task, then it will be suspended the next time it
     * waits for suspended==false (second wait-loop in run())
     */

    public void suspend() {
        synchronized(this) {
            if(log.isTraceEnabled()) log.trace("suspended=" + suspended + ", task=" + printObj(task));
            if(suspended)
                return; // already suspended
            else
                suspended=true;
        }
    }


    /**
     * Resumes the thread. Noop if not suspended
     */
    public void resume() {
        synchronized(this) {
            suspended=false;
            notifyAll();  // notifies run(): the wait on suspend() is released
        }
    }


    /**
     * Assigns a task to the thread. If the thread is not running, it will be started. It it is
     * already working on a task, it will reject the new task. Returns true if task could be
     * assigned auccessfully
     */
    public boolean assignTask(Runnable t) {
        synchronized(this) {
            start(); // creates and starts the thread if not yet running
            if(task == null) {
                task=t;
                notifyAll();	    // signals run() to start working (first wait-loop)
                return true;
            }
            else {
                if(log.isErrorEnabled())
                    log.error("already working on a thread: current_task=" + task + ", new task=" + t +
                            ", thread=" + thread + ", is alive=" + (thread != null ? "" + thread.isAlive() : "null"));
                return false;
            }
        }
    }


    /**
     * Delicate piece of code (means very important :-)). Works as follows: loops until stop is true.
     * Waits in a loop until task is assigned. Then runs the task and notifies waiters that it's done
     * when task is completed. Then returns to the first loop to wait for more work. Does so until
     * stop() is called, which sets stop=true and interrupts the thread. If waiting for a task, the
     * thread terminates. If running a task, the task is interrupted, and the thread terminates. If the
     * task is not interrupible, the stop() method will wait for 3 secs (join on the thread), then return.
     * This means that the run() method of the task will complete and only then will the thread be
     * garbage-collected.
     */
    public void run() {
        while(thread != null) {     // Stop sets thread=null
            try {
                if(log.isTraceEnabled()) log.trace("entering ASSIGN");
                synchronized(this) {
                    if(log.isTraceEnabled())
                        log.trace("entered ASSIGN (task=" + printObj(task) + ", thread=" + printObj(thread) + ')');

                    while(task == null && thread != null) { // first wait-loop: wait for task to be assigned (assignTask())
                        if(log.isTraceEnabled()) log.trace("wait ASSIGN");
                        wait();
                        if(log.isTraceEnabled()) log.trace("wait ASSIGN completed");
                    }
                }
            }
            catch(InterruptedException ex) {  // on assignTask()
                if(log.isTraceEnabled()) log.trace("interrupt on ASSIGN");
                break; // GemStoneAddition exit loop and thread
            }
            if(thread == null) return;  // we need to terminate

            try {
                if(log.isTraceEnabled()) log.trace("entering SUSPEND");
                synchronized(this) {
                    if(log.isTraceEnabled())
                        log.trace("entered SUSPEND (suspended=" + suspended + ", task=" + printObj(task) + ')');
                    while(suspended && thread != null) {    // second wait-loop: wait for thread to resume (resume())
                        if(log.isTraceEnabled()) log.trace("wait SUSPEND");
                        wait();
                        if(log.isTraceEnabled()) log.trace("wait SUSPEND completed");
                    }
                }
            }
            catch(InterruptedException ex) {  // on resume()
                if(log.isTraceEnabled()) log.trace("interrupt on RESUME");
                break; // GemStoneAddition exit loop and thread
            }
            if(thread == null) return; // we need to terminate

            if(task != null) {
                if(log.isTraceEnabled()) log.trace("running task");
                try {
                    task.run(); //here we are actually running the task
                }
                catch(RuntimeException ex) {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.ReusableThread_FAILED_RUNNING_TASK, ex);
                }
                if(log.isTraceEnabled()) log.trace("task completed");
            }

            if(log.isTraceEnabled()) log.trace("entering THIS");
            synchronized(this) {
                if(log.isTraceEnabled()) log.trace("entered THIS");
                task=null;
                if(log.isTraceEnabled()) log.trace("notify THIS");
                notifyAll();
                if(log.isTraceEnabled()) log.trace("notify THIS completed");
            }
        }
        if(log.isTraceEnabled()) log.trace("terminated");
    }


    String printObj(Object obj) {
        if(obj == null)
            return "null";
        else
            return "non-null";
    }


    public void waitUntilDone() {

        if(log.isTraceEnabled()) log.trace("entering THIS");
        synchronized(this) {
            if(log.isTraceEnabled()) log.trace("entered THIS (task=" + printObj(task) + ')');
            while(task != null) {
                boolean interrupted = Thread.interrupted(); // GemStoneAddition
                try {
                    if(log.isTraceEnabled()) log.trace("wait THIS");
                    wait();
                    if(log.isTraceEnabled()) log.trace("wait THIS completed");
                }
                catch(InterruptedException e) {
                  interrupted = true; // GemStoneAddition
                  break; // GemStoneAddition
                }
                finally { // GemStoneAddition
                  if (interrupted) {
                    Thread.currentThread().interrupt();
                  }
                }
            }
        }
    }


    @Override // GemStoneAddition
    public String toString() {
        return "suspended=" + suspended;
    }


}







