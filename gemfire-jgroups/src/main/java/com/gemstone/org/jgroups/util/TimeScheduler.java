/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: TimeScheduler.java,v 1.11 2005/12/23 12:04:01 belaban Exp $

package com.gemstone.org.jgroups.util;


import com.gemstone.org.jgroups.util.GemFireTracer;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;


/**
 * Fixed-delay & fixed-rate single thread scheduler
 * <p/>
 * The scheduler supports varying scheduling intervals by asking the task
 * every time for its next preferred scheduling interval. Scheduling can
 * either be <i>fixed-delay</i> or <i>fixed-rate</i>. The notions are
 * borrowed from <tt>java.util.Timer</tt> and retain the same meaning.
 * I.e. in fixed-delay scheduling, the task's new schedule is calculated
 * as:<br>
 * new_schedule = time_task_starts + scheduling_interval
 * <p/>
 * In fixed-rate scheduling, the next schedule is calculated as:<br>
 * new_schedule = time_task_was_supposed_to_start + scheduling_interval
 * <p/>
 * The scheduler internally holds a queue of tasks sorted in ascending order
 * according to their next execution time. A task is removed from the queue
 * if it is cancelled, i.e. if <tt>TimeScheduler.Task.isCancelled()</tt>
 * returns true.
 * <p/>
 * The scheduler internally uses a <tt>java.util.SortedSet</tt> to keep tasks
 * sorted. <tt>java.util.Timer</tt> uses an array arranged as a binary heap
 * that doesn't shrink. It is likely that the latter arrangement is faster.
 * <p/>
 * Initially, the scheduler is in <tt>SUSPEND</tt>ed mode, <tt>start()</tt>
 * need not be called: if a task is added, the scheduler gets started
 * automatically. Calling <tt>start()</tt> starts the scheduler if it's
 * suspended or stopped else has no effect. Once <tt>stop()</tt> is called,
 * added tasks will not restart it: <tt>start()</tt> has to be called to
 * restart the scheduler.
 */
public class TimeScheduler  {
    /**
     * The interface that submitted tasks must implement
     */
    public interface Task {
        /**
         * @return true if task is cancelled and shouldn't be scheduled
         *         again
         */
        boolean cancelled();

        /**
         * @return the next schedule interval
         */
        long nextInterval();

        /**
         * Execute the task
         */
        void run();
    }

    public interface CancellableTask extends Task {
        /**
         * Cancels the task. After calling this, {@link #cancelled()} return true. If the task was already cancelled,
         * this is a no-op
         */
        void cancel();
    }


    /**
     * Internal task class.
     */
    private static class IntTask implements Comparable {
        /**
         * The user task
         */
        public final Task task;
        /**
         * The next execution time
         */
        public long sched;
        /**
         * Whether this task is scheduled fixed-delay or fixed-rate
         */
        public final boolean relative;

        /**
         * @param task     the task to schedule & execute
         * @param sched    the next schedule
         * @param relative whether scheduling for this task is soft or hard
         *                 (see <tt>TimeScheduler.add()</tt>)
         */
        public IntTask(Task task, long sched, boolean relative) {
            this.task=task;
            this.sched=sched;
            this.relative=relative;
        }

        /**
         * @param obj the object to compare against
         *            <p/>
         *            <pre>
         *            If obj is not instance of <tt>IntTask</tt>, then return -1
         *            If obj is instance of <tt>IntTask</tt>, compare the
         *            contained tasks' next execution times. If these times are equal,
         *            then order them randomly <b>but</b> consistently!: return the diff
         *            of their <tt>hashcode()</tt> values
         *            </pre>
         */
        public int compareTo(Object obj) {
            IntTask other;

            if(!(obj instanceof IntTask)) return (-1);
            other=(IntTask)obj;
//            System.out.println("comparing times " + sched + " and " + other.sched);
            if(sched < other.sched) return (-1);
            if(sched > other.sched) return (1);
            // GemStoneAddition - fix for bug #45711, loss of UNICAST message
            // where two tasks had the same hashCode() so the new one
            // was not put into the queue.  Current JGroups releases handle
            // this by keeping a list of colliding tasks in the scheduled
            // IntTask
            int myHash = System.identityHashCode(this.task);
            int otherHash = System.identityHashCode(other.task);
            int diff = myHash - otherHash;
            if (diff == 0) {
              if (this.task != other.task) {
                // compareTo is only used in putting tasks into order in the
                // queue.  If two tasks have the same scheduled time we don't
                // really care what order they're in wrt executing them.
                return 1;
              }
            }
            return diff;
        }
        
        @Override
        public boolean equals(Object o) { // GemStoneAddition
          if (o == null || !(o instanceof IntTask)) return false;
          return this.task == ((IntTask)o).task;  // GemStoneAddition - was using compareTo(0)==0.  Bug #45711
        }

        @Override
        public int hashCode() { // GemStoneAddition
          return System.identityHashCode(task); // GemStoneAddition - this was using task.hashCode()+sched.  Bug #45711
        }
        
        @Override // GemStoneAddition
        public String toString() {
            if(task == null)
                return "<unnamed>";
            else
                return "("+task.toString()+"; sched="+sched+")"+"@"+Long.toHexString(task.hashCode()); //task.getClass().getName();
        }
    }


    /**
     * The scheduler thread's main loop
     */
    protected/*GemStoneAddition*/ class Loop implements Runnable {
        public void run() {
            try {
                _run();
            }
            catch (VirtualMachineError err) { // GemStoneAddition
              // If this ever returns, rethrow the error.  We're poisoned
              // now, so don't let this thread continue.
              throw err;
            }
            catch(Throwable t) {
                log.error(ExternalStrings.TimeScheduler_EXCEPTION_IN_LOOP, t);
            }
        }
    }


    /**
     * The task queue used by the scheduler. Tasks are ordered in increasing
     * order of their next execution time
     */
    private static class TaskQueue  {
        /**
         * Sorted list of <tt>IntTask</tt>s
         */
        private final SortedSet set;

        public TaskQueue() {
            super();
            set=new TreeSet();
        }

        // GemStoneAddition - return true if the task was added, false if already present
        public boolean add(IntTask t) {
            return set.add(t);
        }

        public void remove(IntTask t) {
            set.remove(t);
        }

        public IntTask getFirst() {
            return ((IntTask)set.first());
        }

        public void removeFirst() {
            Iterator it=set.iterator();
            it.next();
            it.remove();
        }

        public void rescheduleFirst(long sched) {
            Iterator it=set.iterator();
            IntTask t=(IntTask)it.next();
            it.remove();
            t.sched=sched;
            set.add(t);
        }

        public boolean isEmpty() {
            return (set.isEmpty());
        }

        public void clear() {
            set.clear();
        }

        public int size() {
            return set.size();
        }

        @Override // GemStoneAddition
        public String toString() {
            return set.toString();
        }

        public void toString(StringBuffer sb) {
          sb.append(set.toString());
        }
    }


    public void toString(StringBuffer sb) {
      synchronized(this.queue) {
        this.queue.toString(sb);
      }
    }

    /**
     * Default suspend interval (ms)
     */
    private static final long SUSPEND_INTERVAL=30000;


    /** if it takes more than this to run a task, we emit a warning */
    private static final long MAX_EXECUTION_TIME=6000;


    /**
     * Thread is running
     * <p/>
     * A call to <code>start()</code> has no effect on the thread<br>
     * A call to <code>stop()</code> will stop the thread<br>
     * A call to <code>add()</code> has no effect on the thread
     */
    private static final int RUN=0;
    /**
     * Thread is suspended
     * <p/>
     * A call to <code>start()</code> will recreate the thread<br>
     * A call to <code>stop()</code> will switch the state from suspended
     * to stopped<br>
     * A call to <code>add()</code> will recreate the thread <b>only</b>
     * if it is suspended
     */
    private static final int SUSPEND=1;
    /**
     * A shutdown of the thread is in progress
     * <p/>
     * A call to <code>start()</code> has no effect on the thread<br>
     * A call to <code>stop()</code> has no effect on the thread<br>
     * A call to <code>add()</code> has no effect on the thread<br>
     */
    private static final int STOPPING=2;
    /**
     * Thread is stopped
     * <p/>
     * A call to <code>start()</code> will recreate the thread<br>
     * A call to <code>stop()</code> has no effect on the thread<br>
     * A call to <code>add()</code> has no effect on the thread<br>
     */
    private static final int STOP=3;

    /**
     * TimeScheduler thread name
     */
    private static final String THREAD_NAME="TimeScheduler.Thread";


    /**
     * This is a synchronization object to ensure that visibility of
     * {@link #thread} is properly acquired and released.
     */
    private final Object thread_mutex = new Object();
    
    /**
     * The scheduler thread
     * 
     * Updates to this field must be synchronized via {@link #thread_mutex}.
     */
    volatile private Thread thread=null;
    /**
     * The thread's running state
     * GemStoneAddition -- All updates must be synchronized on {@link #queue}
     */
    private volatile int thread_state=SUSPEND;  // GemStoneAddition - volatile
    /**
     * Time that task queue is empty before suspending the scheduling
     * thread
     */
    private long suspend_interval=SUSPEND_INTERVAL;
    /**
     * The task queue ordered according to task's next execution time
     */
    private final TaskQueue queue;

    protected static final GemFireTracer log=GemFireTracer.getLog(TimeScheduler.class);

    /**
     * Just ensure that this class gets loaded.
     * 
     * @see SystemFailure#loadEmergencyClasses()
     */
    static public void loadEmergencyClasses() {
      // Only using java.lang.Thread; we're done
    }

    /**
     * @see SystemFailure#emergencyClose()
     * @see #stop()
     */
    public void emergencyClose() {
      thread_state = STOP; // update WITHOUT synchronization
      Thread thr = this.thread; // volatile fetch
      if (thr != null) {
        thr.interrupt();
      }
    }
    
    private void startThread() {
      // only start if not yet running
      synchronized (thread_mutex) {
        if(thread == null || !thread.isAlive()) {
          thread=new Thread(new Loop(), THREAD_NAME);
          thread.setDaemon(true);
          thread.start();
      }
    }
    }

    /**
     * Set the thread state to running, create and start the thread
     * 
     * GemStoneAddition -- caller must be synchronized on {@link #queue}.
     */
    private void _start() {
        thread_state=RUN;
        startThread();
      }

    /**
     * Restart the suspended thread
     * 
     * GemStoneAddition -- caller must be synchronized on {@link #queue}.
     */
    private void _unsuspend() {
        thread_state=RUN;

        startThread();
    }

    /**
     * Set the thread state to suspended
     * 
     * GemStoneAddition -- caller must be synchronized on {@link #queue}
     */
    private void _suspend() {
        thread_state=SUSPEND;
        synchronized (thread_mutex) {
          thread=null;
        }
    }

    /**
     * Set the thread state to stopping
     * 
     * GemStoneAddition -- caller must be synchronized on {@link #queue}
     */
    private void _stopping() {
        thread_state=STOPPING;
    }

    /**
     * Set the thread state to stopped
     * 
     * GemStoneAddition -- caller must be synchronized on {@link #queue}
     */
    private void _stop() {
        thread_state=STOP;
        synchronized (thread_mutex) {
          thread=null;
        }
    }


    /**
     * If the task queue is empty, sleep until a task comes in or if slept
     * for too long, suspend the thread.
     * <p/>
     * Get the first task, if the running time hasn't been
     * reached then wait a bit and retry. Else reschedule the task and then
     * run it.
     */
    protected/*GemStoneAddition*/ void _run() {
        IntTask intTask;
        Task task;
        long currTime, execTime, waitTime, intervalTime, schedTime;

        while(true) {
          Thread thr = thread; // volatile fetch
          if(thr == null || thr.isInterrupted()) return;

            synchronized(queue) {
                while(true) {
                    // GemStoneAddition -- fault tolerance.  Look for the
                    // interrupt bit, even though it _should_ be caught
                    // by the wait() below.
                    thr = thread; // volatile fetch
                    if (thr == null || thr.isInterrupted())
                      return;

                    // GemStoneAddition -- if we are stopped, terminate
                    // regardless of contents of the queue
                    if (thread_state == STOP)
                      return;
                    
                    if(!queue.isEmpty()) break;
                    
                    // GemStoneAddition: exit if queue is empty and we are stopping.
                    if (thread_state == STOPPING)
                      return;
                    
                    try {
                        queue.wait(suspend_interval);
                    }
                    catch(InterruptedException ex) {
                        return; // exit thread
                    }
                    if(!queue.isEmpty()) break;

                    // GemStoneAddition: exit if queue is empty and we are stopping.
                    if (thread_state == STOPPING)
                      return;
                    
                    _suspend();
                    return;
                } // while

                intTask=queue.getFirst();
                synchronized(intTask) {
                    task=intTask.task;
                    if(task.cancelled()) {
                        queue.removeFirst();
                        continue;
                    }
                    currTime=System.currentTimeMillis();
                    execTime=intTask.sched;
                    if((waitTime=execTime - currTime) <= 0) {
                        // Reschedule the task
                        intervalTime=task.nextInterval();
                        schedTime=intTask.relative ?
                                currTime + intervalTime : execTime + intervalTime;
                        queue.rescheduleFirst(schedTime);
                    }
                } // synchronized(intTask)
                if(waitTime > 0) {
                    //try queue.wait(Math.min(waitTime, TICK_INTERVAL));
                    try {
                        queue.wait(waitTime);
                    }
                    catch(InterruptedException ex) {
                        return; // exit thread
                    }
                    continue;
                }
            } // synchronized(queue)

            long start=System.currentTimeMillis();
            try {
                task.run();
                if(log.isWarnEnabled()) {
                  long stop, diff;
                  stop=System.currentTimeMillis();
                  diff=stop-start;
                  if(diff >= MAX_EXECUTION_TIME) {
                    log.warn("task " + task + " took " + diff + "ms to execute, " +
                             "please check why it is taking so long. It is delaying other tasks");
                  }
                }
            }
            catch (VirtualMachineError err) { // GemStoneAddition
              // If this ever returns, rethrow the error.  We're poisoned
              // now, so don't let this thread continue.
              throw err;
            }
            catch(Throwable ex) {
                log.error(ExternalStrings.TimeScheduler_FAILED_RUNNING_TASK__0, task, ex);
            }
        } // while
    }


    /**
     * Create a scheduler that executes tasks in dynamically adjustable
     * intervals
     *
     * @param suspend_interval the time that the scheduler will wait for
     *                         at least one task to be placed in the task queue before suspending
     *                         the scheduling thread
     */
    public TimeScheduler(long suspend_interval) {
        super();
        queue=new TaskQueue();
        this.suspend_interval=suspend_interval;
    }

    /**
     * Create a scheduler that executes tasks in dynamically adjustable
     * intervals
     */
    public TimeScheduler() {
        this(SUSPEND_INTERVAL);
    }


    public void setSuspendInterval(long s) {
        this.suspend_interval=s;
    }

    public long getSuspendInterval() {
        return suspend_interval;
    }

    public String dumpTaskQueue() {
        return queue != null? queue.toString() : "<empty>";
    }


    /**
     * Add a task for execution at adjustable intervals
     *
     * @param t        the task to execute
     * @param relative scheduling scheme:
     *                 <p/>
     *                 <tt>true</tt>:<br>
     *                 Task is rescheduled relative to the last time it <i>actually</i>
     *                 started execution
     *                 <p/>
     *                 <tt>false</tt>:<br>
     *                 Task is scheduled relative to its <i>last</i> execution schedule. This
     *                 has the effect that the time between two consecutive executions of
     *                 the task remains the same.
     */
    public void add(Task t, boolean relative) {
      add(t, relative, false, 0L);
    }
    
    public void add(Task t, boolean relative,
        boolean useTimeForTest, long msTimeForTest) { // GemStoneAddition - test hook
        long interval, sched;

        if((interval=t.nextInterval()) < 0) return;
        long time = useTimeForTest? msTimeForTest : System.currentTimeMillis();
        sched = time + interval;

        synchronized(queue) {
//          Set<Task> copy = new HashSet(queue.getSet());
            IntTask itask = new IntTask(t, sched, relative);
            boolean added = queue.add(itask);

            // GemStoneAddition - following assertion was added for bug 45711 but it
            // is possible that there will be an attempt to add the same task multiple
            // times - see bug #49991
//            assert added : "Time scheduler lost a task: " + itask;

//            if (copy.size() >= queue.size()) {  debugging code for #45711
//              StringBuffer sb = new StringBuffer(5000);
//              sb.append("added=").append(added).append("; task=").append(itask);
//              sb.append("\nqueue=");
//              queue.toString(sb);
//              sb.append("\ncopy=").append(copy);
//              log.getLogWriterI18n().severe(JGroupsStrings.DEBUG, "TimeScheduler queue did not grow in size: "+sb.toString());
//              System.exit(1);
//            }
            switch(thread_state) {
                case RUN:
                    queue.notifyAll();
                    break;
                case SUSPEND:
                    _unsuspend();
                    break;
                case STOPPING:
                    break;
                case STOP:
                    break;
            }
        } // synchronized
    }

    /**
     * Add a task for execution at adjustable intervals
     *
     * @param t the task to execute
     */
    public void add(Task t) {
        add(t, true);
    }

    /**
     * Answers the number of tasks currently in the queue.
     * @return The number of tasks currently in the queue.
     */
    public int size() {
        return queue.size();
    }


    /**
     * Start the scheduler, if it's suspended or stopped
     */
    public void start() {
        synchronized(queue) {
            switch(thread_state) {
                case RUN:
                    break;
                case SUSPEND:
                    _unsuspend();
                    break;
                case STOPPING:
                    break;
                case STOP:
                    _start();
                    break;
            }
        } // synchronized
    }


    /**
     * Stop the scheduler if it's running. Switch to stopped, if it's
     * suspended. Clear the task queue.
     *
     * @throws InterruptedException if interrupted while waiting for thread
     *                              to return
     */
    public void stop() throws InterruptedException {
// i. Switch to STOPPING, interrupt thread
// ii. Wait until thread ends
// iii. Clear the task queue, switch to STOPPED,
      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
        synchronized(queue) {
            switch(thread_state) {
                case RUN:
                    _stopping();
                    break;
                case SUSPEND:
                    _stop();
                    return;
                case STOPPING:
                    return;
                case STOP:
                    return;
            }
            thread.interrupt();
        } // synchronized

        // Grab current thread field under lock, then wait for it to die.
        Thread myThread;
        synchronized (thread_mutex) {
          myThread = thread;
        }
        if (myThread != null) {
          for (int i=0; i<20 && myThread.isAlive(); i++) {
            myThread.join(500);
            if (myThread.isAlive() && !myThread.isInterrupted()) {
              // log at info level to avoid sending a weird alert
              log.getLogWriter().warning(ExternalStrings.TimeScheduler_TIMESCHEDULER_THREAD_ATE_AN_INTERRUPT_DURING_SHUTDOWN__RETRYING);
              myThread.interrupt();
            }
          }
          if (myThread.isAlive()) {
            // log at info level so we don't generate an alert during shutdown
            log.getLogWriter().info(ExternalStrings.TimeScheduler_TIMESCHEDULER_GIVING_UP_ON_STOPPING_SCHEDULER_THREAD_AFTER_WAITING_10_SECONDS);
          }
        }

        synchronized(queue) {
            queue.clear();
            _stop();
        }
    }
}
