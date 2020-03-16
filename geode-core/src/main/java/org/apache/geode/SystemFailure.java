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
package org.apache.geode;

import org.jgroups.annotations.GuardedBy;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.SystemFailureTestHook;
import org.apache.geode.internal.admin.remote.RemoteGfManagerAgent;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 *
 * Catches and responds to JVM failure
 * <p>
 * This class represents a catastrophic failure of the system, especially the Java virtual machine.
 * Any class may, at any time, indicate that a system failure has occurred by calling
 * {@link #initiateFailure(Error)} (or, less commonly, {@link #setFailure(Error)}).
 * <p>
 * In practice, the most common type of failure that is likely to be reported by an otherwise
 * healthy JVM is {@link OutOfMemoryError}. However, GemFire will report any occurrence of
 * {@link VirtualMachineError} as a JVM failure.
 * <p>
 * When a failure is reported, you must assume that the JVM has <em>broken its fundamental execution
 * contract</em> with your application. No programming invariant can be assumed to be true, and your
 * entire application must be regarded as corrupted.
 * <h1>Failure Hooks</h1> GemFire uses this class to disable its distributed system (group
 * communication) and any open caches. It also provides a hook for you to respond to after GemFire
 * disables itself.
 * <h1>Failure WatchDog</h1> When {@link #startThreads()} is called, a "watchdog" {@link Thread} is
 * started that periodically checks to see if system corruption has been reported. When system
 * corruption is detected, this thread proceeds to:
 * <p>
 * <ol>
 * <li><em>Close GemFire</em> -- Group communication is ceased (this cache member recuses itself
 * from the distributed system) and the cache is further poisoned (it is pointless to try to cleanly
 * close it at this point.).
 * <p>
 * After this has successfully ended, we launch a</li>
 * <li><em>failure action</em>, a user-defined Runnable {@link #setFailureAction(Runnable)}. By
 * default, this Runnable performs nothing. If you feel you need to perform an action before exiting
 * the JVM, this hook gives you a means of attempting some action. Whatever you attempt should be
 * extremely simple, since your Java execution environment has been corrupted.
 * <p>
 * GemStone recommends that you employ
 * <a href="http://wrapper.tanukisoftware.org/doc/english/introduction.html"> Java Service
 * Wrapper</a> to detect when your JVM exits and to perform appropriate failure and restart actions.
 * </li>
 * <li>Finally, if the application has granted the watchdog permission to exit the JVM (via
 * {@link #setExitOK(boolean)}), the watchdog calls {@link System#exit(int)} with an argument of 1.
 * If you have not granted this class permission to close the JVM, you are <em>strongly</em> advised
 * to call it in your failure action (in the previous step).</li>
 * </ol>
 * <p>
 * Each of these actions will be run exactly once in the above described order. However, if either
 * step throws any type of error ({@link Throwable}), the watchdog will assume that the JVM is still
 * under duress (esp. an {@link OutOfMemoryError}), will wait a bit, and then retry the failed
 * action.
 * <p>
 * It bears repeating that you should be very cautious of any Runnables you ask this class to run.
 * By definition the JVM is <em>very sick</em> when failure has been signalled.
 * <p>
 * <h1>Failure Proctor</h1> In addition to the failure watchdog, {@link #startThreads()} creates a
 * second thread (the "proctor") that monitors free memory. It does this by examining
 * {@link Runtime#freeMemory() free memory}, {@link Runtime#totalMemory() total memory} and
 * {@link Runtime#maxMemory() maximum memory}. If the amount of available memory stays below a given
 * {@link #setFailureMemoryThreshold(long) threshold}, for more than {@link #WATCHDOG_WAIT} seconds,
 * the watchdog is notified.
 * <p>
 * Note that the proctor can be effectively disabled by
 * {@link SystemFailure#setFailureMemoryThreshold(long) setting} the failure memory threshold to a
 * negative value.
 * <p>
 * The proctor is a second line of defense, attempting to detect OutOfMemoryError conditions in
 * circumstances where nothing alerted the watchdog. For instance, a third-party jar might
 * incorrectly handle this error and leave your virtual machine in a "stuck" state.
 * <p>
 * Note that the proctor does not relieve you of the obligation to follow the best practices in the
 * next section.
 * <h1>Best Practices</h1>
 * <h2>Catch and Handle VirtualMachineError</h2> If you feel obliged to catch <em>either</em>
 * {@link Error}, or {@link Throwable}, you <em>must</em>also check for {@link VirtualMachineError}
 * like so:
 * <p>
 *
 * <pre>
        catch (VirtualMachineError err) {
          SystemFailure.{@link #initiateFailure(Error) initiateFailure}(err);
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
 * </pre>
 *
 * <h2>Periodically Check For Errors</h2> Check for serious system errors at appropriate points in
 * your algorithms. You may elect to use the {@link #checkFailure()} utility function, but you are
 * not required to (you could just see if {@link SystemFailure#getFailure()} returns a non-null
 * result).
 * <p>
 * A job processing loop is a good candidate, for instance, in
 * org.apache.org.jgroups.protocols.UDP#run(), which implements {@link Thread#run}:
 * <p>
 *
 * <pre>
         for (;;)  {
           SystemFailure.{@link #checkFailure() checkFailure}();
           if (mcast_recv_sock == null || mcast_recv_sock.isClosed()) break;
           if (Thread.currentThread().isInterrupted()) break;
          ...
 * </pre>
 *
 * <p>
 * <h2>Catches of Error and Throwable Should Check for Failure</h2> Keep in mind that peculiar or
 * flat-out<em>impossible</em> exceptions may ensue after a VirtualMachineError has been thrown
 * <em>anywhere</em> in your virtual machine. Whenever you catch {@link Error} or {@link Throwable},
 * you should also make sure that you aren't dealing with a corrupted JVM:
 * <p>
 *
 * <pre>
       catch (Throwable t) {
         // Whenever you catch Error or Throwable, you must also
         // catch VirtualMachineError (see above).  However, there is
         // _still_ a possibility that you are dealing with a cascading
         // error condition, so you also need to check to see if the JVM
         // is still usable:
         SystemFailure.{@link #checkFailure() checkFailure}();
         ...
       }
 * </pre>
 *
 * @since GemFire 5.1
 *
 * @deprecated since Geode 1.11 because it is potentially counterproductive to try
 *             to mitigate a VirtualMachineError since the JVM (spec) makes no guarantees about the
 *             soundness of the JVM after such an error. In the presence of a VirtualMachineError,
 *             the simplest solution is really the only solution: exit the JVM as soon as possible.
 *
 */
@Deprecated
@edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "DM_GC",
    justification = "This class performs System.gc as last ditch effort during out-of-memory condition.")
public final class SystemFailure {

  /**
   * Time to wait during stopWatchdog and stopProctor. Not final for tests
   */
  @MutableForTesting
  static int SHUTDOWN_WAIT = 1000;
  /**
   * Preallocated error messages may use memory (in the form of an iterator) so we
   * must get the translated messages in advance.
   **/
  static final String JVM_CORRUPTION =
      "JVM corruption has been detected";
  private static final String CALLING_SYSTEM_EXIT =
      "Since this is a dedicated cache server and the JVM has been corrupted, this process will now terminate. Permission to call System#exit(int) was given in the following context.";

  /**
   * the underlying failure
   *
   * This is usually an instance of {@link VirtualMachineError}, but it is not required to be such.
   *
   * @see #getFailure()
   * @see #initiateFailure(Error)
   */
  @MakeNotStatic
  protected static volatile Error failure = null;

  /**
   * user-defined runnable to run last
   *
   * @see #setFailureAction(Runnable)
   */
  @MakeNotStatic
  private static volatile Runnable failureAction = () -> {
    System.err.println(JVM_CORRUPTION);
    failure.printStackTrace();
  };

  /**
   * @see #setExitOK(boolean)
   */
  @MakeNotStatic
  private static volatile boolean exitOK = false;

  /**
   * If we're going to exit the JVM, I want to be accountable for who told us it was OK.
   */
  @MakeNotStatic
  private static volatile Throwable exitExcuse;

  /**
   * Indicate whether it is acceptable to call {@link System#exit(int)} after failure processing has
   * completed.
   * <p>
   * This may be dynamically modified while the system is running.
   *
   * @param newVal true if it is OK to exit the process
   * @return the previous value
   */
  public static boolean setExitOK(boolean newVal) {
    boolean result = exitOK;
    exitOK = newVal;
    if (exitOK) {
      exitExcuse = new Throwable("SystemFailure exitOK set");
    } else {
      exitExcuse = null;
    }
    return result;
  }

  /**
   * Returns true if the given Error is a fatal to the JVM and it should be shut down. Code should
   * call {@link #initiateFailure(Error)} or {@link #setFailure(Error)} if this returns true.
   */
  public static boolean isJVMFailureError(Error err) {
    return err instanceof OutOfMemoryError || err instanceof UnknownError;
  }

  /**
   * Disallow instance creation
   */
  private SystemFailure() {

  }

  /**
   * Synchronizes access to state variables, used to notify the watchdog when to run
   *
   * @see #notifyWatchDog()
   * @see #startProctor()
   * @see #startWatchDog()
   */
  private static final Object failureSync = new Object();

  /**
   * True if we have closed GemFire
   *
   * @see #emergencyClose()
   */
  @MakeNotStatic
  private static volatile boolean gemfireCloseCompleted = false;

  /**
   * True if we have completed the user-defined failure action
   *
   * @see #setFailureAction(Runnable)
   */
  @MakeNotStatic
  private static volatile boolean failureActionCompleted = false;

  /**
   * This is the amount of time, in seconds, the watchdog periodically awakens to see if the system
   * has been corrupted.
   * <p>
   * The watchdog will be explicitly awakened by calls to {@link #setFailure(Error)} or
   * {@link #initiateFailure(Error)}, but it will awaken of its own accord periodically to check for
   * failure even if the above calls do not occur.
   * <p>
   * This can be set with the system property <code>gemfire.WATCHDOG_WAIT</code>. The default is 15
   * sec.
   */
  private static final int WATCHDOG_WAIT =
      Integer.getInteger(GeodeGlossary.GEMFIRE_PREFIX + "WATCHDOG_WAIT", 15);

  /**
   * This is the watchdog thread
   */
  @GuardedBy("failureSync")
  @MakeNotStatic
  private static Thread watchDog;

  @MakeNotStatic
  private static volatile boolean isCacheClosing = false;

  /**
   * Should be invoked when GemFire cache is being created.
   */
  public static void signalCacheCreate() {
    isCacheClosing = false;
  }

  /**
   * Should be invoked when GemFire cache is closing or closed.
   */
  public static void signalCacheClose() {
    isCacheClosing = true;
    if (proctor != null) {
      proctor.interrupt();
    }
    if (watchDog != null) {
      watchDog.interrupt();
    }
  }

  /**
   * Start the watchdog thread, if it isn't already running.
   */
  private static void startWatchDog() {
    if (failureActionCompleted) {
      return;
    }
    synchronized (failureSync) {
      if (watchDog != null && watchDog.isAlive()) {
        return;
      }
      watchDog = new LoggingThread("SystemFailure WatchDog", SystemFailure::runWatchDog);
      watchDog.start();
    }
  }

  private static void stopWatchDog() {
    Thread watchDogSnapshot = null;
    synchronized (failureSync) {
      stopping = true;
      if (watchDog != null && watchDog.isAlive()) {
        failureSync.notifyAll();
        watchDogSnapshot = watchDog;
      }
    }
    if (watchDogSnapshot != null) {
      try {
        watchDogSnapshot.join(100);
      } catch (InterruptedException ignore) {
      }
      if (watchDogSnapshot.isAlive()) {
        watchDogSnapshot.interrupt();
        try {
          watchDogSnapshot.join(SHUTDOWN_WAIT);
        } catch (InterruptedException ignore) {
        }
      }
    }
  }

  /**
   * This is the run loop for the watchdog thread.
   */
  private static void runWatchDog() {

    boolean warned = false;

    logFine(WATCHDOG_NAME, "Starting");
    while (!stopping) {
      try {
        if (isCacheClosing) {
          break;
        }
        // Sleep or get notified...
        synchronized (failureSync) {
          if (stopping) {
            return;
          }
          logFine(WATCHDOG_NAME, "Waiting for disaster");
          try {
            failureSync.wait(WATCHDOG_WAIT * 1000L);
          } catch (InterruptedException e) {
            // Ignore
          }
          if (stopping) {
            return;
          }
        }

        // Perform watchdog sentinel duties.

        if (failureActionCompleted) {
          logInfo(WATCHDOG_NAME, "all actions completed; exiting");
        }
        if (failure == null) {
          logFine(WATCHDOG_NAME, "no failure detected");
          continue;
        }
        if (!warned) {
          warned = logWarning(WATCHDOG_NAME, "failure detected", failure);
        }

        if (!gemfireCloseCompleted) {
          logInfo(WATCHDOG_NAME, "closing GemFire");
          try {
            emergencyClose();
          } catch (Throwable t) {
            logWarning(WATCHDOG_NAME, "trouble closing GemFire", t);
            continue;
          }
          gemfireCloseCompleted = true;
        }

        if (!failureActionCompleted) {
          // avoid potential race condition setting the runnable
          Runnable r = failureAction;
          if (r != null) {
            logInfo(WATCHDOG_NAME, "running user's runnable");
            try {
              r.run();
            } catch (Throwable t) {
              logWarning(WATCHDOG_NAME, "trouble running user's runnable", t);
              continue;
            }
          }
          failureActionCompleted = true;
        }

        stopping = true;
        stopProctor();

        if (exitOK) {
          logWarning(WATCHDOG_NAME, CALLING_SYSTEM_EXIT, exitExcuse);

          // ATTENTION: there are VERY FEW places in GemFire where it is
          // acceptable to call System.exit. This is one of those
          // places...
          ExitCode.FATAL.doSystemExit();
        }

        logInfo(WATCHDOG_NAME, "exiting");
        return;
      } catch (Throwable t) {
        logWarning(WATCHDOG_NAME, "thread encountered a problem: " + t, t);
      }
    }
  }

  /**
   * Spies on system statistics looking for low memory threshold
   *
   * @see #minimumMemoryThreshold
   */
  @GuardedBy("failureSync")
  @MakeNotStatic
  private static Thread proctor;

  /**
   * This mutex controls access to {@link #firstStarveTime} and {@link #minimumMemoryThreshold}.
   * <p>
   * I'm hoping that a fat lock is never created here, so that an object allocation isn't necessary
   * to acquire this mutex. You'd have to have A LOT of contention on this mutex in order for a fat
   * lock to be created, which indicates IMHO a serious problem in your applications.
   */
  private static final Object memorySync = new Object();

  /**
   * This is the minimum amount of memory that the proctor will tolerate before declaring a system
   * failure.
   *
   * @see #setFailureMemoryThreshold(long)
   */
  @GuardedBy("memorySync")
  @MakeNotStatic
  private static long minimumMemoryThreshold = Long.getLong(
      GeodeGlossary.GEMFIRE_PREFIX + "SystemFailure.chronic_memory_threshold", 1048576);

  /**
   * This is the interval, in seconds, that the proctor thread will awaken and poll system free
   * memory.
   *
   * The default is 1 sec. This can be set using the system property
   * <code>gemfire.SystemFailure.MEMORY_POLL_INTERVAL</code>.
   *
   * @see #setFailureMemoryThreshold(long)
   */
  private static final long MEMORY_POLL_INTERVAL =
      Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "SystemFailure.MEMORY_POLL_INTERVAL", 1);

  /**
   * This is the maximum amount of time, in seconds, that the proctor thread will tolerate seeing
   * free memory stay below {@link #setFailureMemoryThreshold(long)}, after which point it will
   * declare a system failure.
   *
   * The default is 15 sec. This can be set using the system property
   * <code>gemfire.SystemFailure.MEMORY_MAX_WAIT</code>.
   *
   * @see #setFailureMemoryThreshold(long)
   */
  public static final long MEMORY_MAX_WAIT =
      Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "SystemFailure.MEMORY_MAX_WAIT", 15);

  /**
   * Flag that determines whether or not we monitor memory on our own. If this flag is set, we will
   * check freeMemory, invoke GC if free memory gets low, and start throwing our own
   * OutOfMemoryException if
   *
   * The default is false, so this monitoring is turned off. This monitoring has been found to be
   * unreliable in non-Sun VMs when the VM is under stress or behaves in unpredictable ways.
   *
   * @since GemFire 6.5
   */
  private static final boolean MONITOR_MEMORY =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "SystemFailure.MONITOR_MEMORY");

  /**
   * Start the proctor thread, if it isn't already running.
   *
   * @see #proctor
   */
  private static void startProctor() {
    if (failure != null) {
      notifyWatchDog();
      return;
    }
    synchronized (failureSync) {
      if (proctor != null && proctor.isAlive()) {
        return;
      }
      proctor = new LoggingThread("SystemFailure Proctor", SystemFailure::runProctor);
      proctor.start();
    }
  }

  private static void stopProctor() {
    Thread proctorSnapshot;
    synchronized (failureSync) {
      stopping = true;
      proctorSnapshot = proctor;
    }
    if (proctorSnapshot != null && proctorSnapshot.isAlive()) {
      proctorSnapshot.interrupt();
      try {
        proctorSnapshot.join(SHUTDOWN_WAIT);
      } catch (InterruptedException ignore) {
      }
    }
  }

  /**
   * Symbolic representation of an invalid starve time
   */
  private static final long NEVER_STARVED = Long.MAX_VALUE;

  /**
   * this is the last time we saw memory starvation
   */
  @GuardedBy("memorySync")
  @MakeNotStatic
  private static long firstStarveTime = NEVER_STARVED;

  /**
   * This is the previous measure of total memory. If it changes, we reset the proctor's starve
   * statistic.
   */
  @MakeNotStatic
  private static long lastTotalMemory = 0;

  /**
   * This is the run loop for the proctor thread
   */
  private static void runProctor() {
    // Note that the javadocs say this can return Long.MAX_VALUE.
    final long maxMemory = Runtime.getRuntime().maxMemory();

    // Allocate this error in advance, since it's too late once it's been detected!
    final OutOfMemoryError oome = new OutOfMemoryError(
        String.format(
            "%s : memory has remained chronically below %s bytes (out of a maximum of %s ) for %s sec.",
            PROCTOR_NAME, minimumMemoryThreshold, maxMemory, WATCHDOG_WAIT));

    logFine(PROCTOR_NAME,
        "Starting, threshold = " + minimumMemoryThreshold + "; max = " + maxMemory);
    while (!isCacheClosing) {
      if (stopping) {
        return;
      }

      try {
        try {
          Thread.sleep(MEMORY_POLL_INTERVAL * 1000);
        } catch (InterruptedException e) {
          // ignore
        }

        if (stopping) {
          return;
        }

        if (failureActionCompleted) {
          return;
        }
        if (failure != null) {
          notifyWatchDog();
          logFine(PROCTOR_NAME, "Failure has been reported, exiting");
          return;
        }

        if (!MONITOR_MEMORY) {
          continue;
        }

        long totalMemory = Runtime.getRuntime().totalMemory();
        if (totalMemory < maxMemory) {
          if (DEBUG) {
            logFine(PROCTOR_NAME,
                "totalMemory (" + totalMemory + ") < maxMemory (" + maxMemory + ")");
          }
          firstStarveTime = NEVER_STARVED;
          continue;
        }
        if (lastTotalMemory < totalMemory) {
          lastTotalMemory = totalMemory;
          firstStarveTime = NEVER_STARVED;
          continue;
        }
        lastTotalMemory = totalMemory;

        long freeMemory = Runtime.getRuntime().freeMemory();
        if (freeMemory == 0) {
          // This is to workaround X bug #41821 in JRockit. Often, Jrockit returns 0 from
          // Runtime.getRuntime().freeMemory() Allocating this one object and calling again seems to
          // workaround the problem.
          new Object();
          freeMemory = Runtime.getRuntime().freeMemory();
        }
        // Grab the threshold and starve time once, under mutex, because
        // it's publicly modifiable.
        long curThreshold;
        long lastStarveTime;
        synchronized (memorySync) {
          curThreshold = minimumMemoryThreshold;
          lastStarveTime = firstStarveTime;
        }

        if (freeMemory >= curThreshold || curThreshold == 0) {
          // Memory is FINE, reset everything
          if (DEBUG) {
            logFine(PROCTOR_NAME, "Current free memory is: " + freeMemory);
          }

          if (lastStarveTime != NEVER_STARVED) {
            logFine(PROCTOR_NAME, "...low memory has self-corrected.");
          }
          synchronized (memorySync) {
            firstStarveTime = NEVER_STARVED;
          }
          continue;
        }

        // Memory is low
        long now = System.currentTimeMillis();
        if (lastStarveTime == NEVER_STARVED) {
          if (DEBUG) {
            logFine(PROCTOR_NAME,
                "Noting current memory " + freeMemory + " is less than threshold " + curThreshold);
          } else {
            logWarning(PROCTOR_NAME,
                "Noting that current memory available is less than the currently designated threshold",
                null);
          }

          synchronized (memorySync) {
            firstStarveTime = now;
          }
          System.gc(); // Attempt to free memory and avoid overflow
          continue;
        }

        if (now - lastStarveTime < MEMORY_MAX_WAIT * 1000) {
          if (DEBUG) {
            logFine(PROCTOR_NAME, "...memory is still below threshold: " + freeMemory);
          } else {
            logWarning(PROCTOR_NAME,
                "Noting that current memory available is still below currently designated threshold",
                null);

          }
          continue;
        }

        logWarning(PROCTOR_NAME, "Memory is chronically low; setting failure!", null);
        SystemFailure.setFailure(oome);
        notifyWatchDog();
        return;
      } catch (Throwable t) {
        logWarning(PROCTOR_NAME, "thread encountered a problem", t);
      }
    }
  }

  /**
   * Enables some fine logging
   */
  private static final boolean DEBUG = false;

  private static final String WATCHDOG_NAME = "SystemFailure Watchdog";

  private static final String PROCTOR_NAME = "SystemFailure Proctor";

  /**
   * Since it requires object memory to unpack a jar file, make sure this JVM has loaded the classes
   * necessary for closure <em>before</em> it becomes necessary to use them.
   * <p>
   * Note that just touching the class in order to load it is usually sufficient, so all an
   * implementation needs to do is to reference the same classes used in {@link #emergencyClose()}.
   * Just make sure to do it while you still have memory to succeed!
   */
  public static void loadEmergencyClasses() {
    startThreads();
  }

  /**
   * Attempt to close any and all GemFire resources.
   *
   * The contract of this method is that it should not acquire any synchronization mutexes nor
   * create any objects.
   * <p>
   * The former is because the system is in an undefined state and attempting to acquire the mutex
   * may cause a hang.
   * <p>
   * The latter is because the likelihood is that we are invoking this method due to memory
   * exhaustion, so any attempt to create an object will also cause a hang.
   * <p>
   * This method is not meant to be called directly (but, well, I guess it could). It is public to
   * document the contract that is implemented by <code>emergencyClose</code> in other parts of the
   * system.
   */
  public static void emergencyClose() {
    GemFireCacheImpl.emergencyClose();

    RemoteGfManagerAgent.emergencyClose();

    // If memory was the problem, make an explicit attempt at this point to clean up.
    System.gc();
  }

  /**
   * Throw the system failure.
   *
   * This method does not return normally.
   * <p>
   * Unfortunately, attempting to create a new Throwable at this point may cause the thread to hang
   * (instead of generating another OutOfMemoryError), so we have to make do with whatever Error we
   * have, instead of wrapping it with one pertinent to the current context. See bug 38394.
   *
   */
  private static void throwFailure() throws Error {
    if (failure != null)
      throw failure;
  }

  /**
   * Notifies the watchdog thread (assumes that {@link #failure} has been set)
   */
  private static void notifyWatchDog() {
    startWatchDog();
    synchronized (failureSync) {
      failureSync.notifyAll();
    }
  }

  /**
   * Utility function to check for failures. If a failure is detected, this methods throws an
   * AssertionFailure.
   *
   * @see #initiateFailure(Error)
   * @throws InternalGemFireError if the system has been corrupted
   * @throws Error if the system has been corrupted and a thread-specific AssertionError cannot be
   *         allocated
   */
  public static void checkFailure() throws InternalGemFireError, Error {
    if (failure == null) {
      return;
    }
    notifyWatchDog();
    throwFailure();
  }

  /**
   * Signals that a system failure has occurred and then throws an AssertionError.
   *
   * @param f the failure to set
   * @throws IllegalArgumentException if f is null
   * @throws InternalGemFireError always; this method does not return normally.
   * @throws Error if a thread-specific AssertionError cannot be allocated.
   */
  public static void initiateFailure(Error f) throws InternalGemFireError, Error {
    SystemFailure.setFailure(f);
    throwFailure();
  }

  /**
   * Set the underlying system failure, if not already set.
   * <p>
   * This method does not generate an error, and should only be used in circumstances where
   * execution needs to continue, such as when re-implementing
   * {@link ThreadGroup#uncaughtException(Thread, Throwable)}.
   *
   * @param failure the system failure
   * @throws IllegalArgumentException if you attempt to set the failure to null
   */
  public static void setFailure(Error failure) {
    if (failure == null) {
      throw new IllegalArgumentException(
          "You are not permitted to un-set a system failure.");
    }
    if (SystemFailureTestHook.errorIsExpected(failure)) {
      return;
    }
    SystemFailure.failure = failure;
    notifyWatchDog();
  }

  /**
   * Returns the catastrophic system failure, if any.
   * <p>
   * This is usually (though not necessarily) an instance of {@link VirtualMachineError}.
   * <p>
   * A return value of null indicates that no system failure has yet been detected.
   * <p>
   * Object synchronization can implicitly require object creation (fat locks in JRockit for
   * instance), so the underlying value is not synchronized (it is a volatile). This means the
   * return value from this call is not necessarily the <em>first</em> failure reported by the JVM.
   * <p>
   * Note that even if it <em>were</em> synchronized, it would only be a proximal indicator near the
   * time that the JVM crashed, and may not actually reflect the underlying root cause that
   * generated the failure. For instance, if your JVM is running short of memory, this Throwable is
   * probably an innocent victim and <em>not</em> the actual allocation (or series of allocations)
   * that caused your JVM to exhaust memory.
   * <p>
   * If this function returns a non-null value, keep in mind that the JVM is very limited. In
   * particular, any attempt to allocate objects may fail if the original failure was an
   * OutOfMemoryError.
   *
   * @return the failure, if any
   */
  public static Error getFailure() {
    return failure;
  }

  /**
   * Sets a user-defined action that is run in the event that failure has been detected.
   * <p>
   * This action is run <em>after</em> the GemFire cache has been shut down. If it throws any error,
   * it will be reattempted indefinitely until it succeeds. This action may be dynamically modified
   * while the system is running.
   * <p>
   * The default action prints the failure stack trace to System.err.
   *
   * @see #initiateFailure(Error)
   * @param action the Runnable to use
   * @return the previous action
   */
  public static Runnable setFailureAction(Runnable action) {
    Runnable old = SystemFailure.failureAction;
    SystemFailure.failureAction = action;
    return old;
  }

  /**
   * Set the memory threshold under which system failure will be notified.
   *
   * This value may be dynamically modified while the system is running. The default is 1048576
   * bytes. This can be set using the system property
   * <code>gemfire.SystemFailure.chronic_memory_threshold</code>.
   *
   * @param newVal threshold in bytes
   * @return the old threshold
   * @see Runtime#freeMemory()
   */
  public static long setFailureMemoryThreshold(long newVal) {
    long result;
    synchronized (memorySync) {
      result = minimumMemoryThreshold;
      minimumMemoryThreshold = newVal;
      firstStarveTime = NEVER_STARVED;
    }
    startProctor();
    return result;
  }

  private static boolean logStdErr(String kind, String name, String s, Throwable t) {
    try {
      System.err.print(name);
      System.err.print(": [");
      System.err.print(kind);
      System.err.print("] ");
      System.err.println(s);
      if (t != null) {
        t.printStackTrace();
      }
      return true;
    } catch (Throwable t2) {
      // out of luck
      return false;
    }
  }

  /**
   * Logging can require allocation of objects, so we wrap the logger so that failures are silently
   * ignored.
   *
   * @param s string to print
   * @param t the call stack, if any
   * @return true if the warning got printed
   */
  protected static boolean logWarning(String name, String s, Throwable t) {
    return logStdErr("warning", name, s, t);
  }

  /**
   * Logging can require allocation of objects, so we wrap the logger so that failures are silently
   * ignored.
   *
   * @param s string to print
   */
  protected static void logInfo(String name, String s) {
    logStdErr("info", name, s, null);
  }

  /**
   * Logging can require allocation of objects, so we wrap the logger so that failures are silently
   * ignored.
   *
   * @param s string to print
   */
  protected static void logFine(String name, String s) {
    if (DEBUG) {
      logStdErr("fine", name, s, null);
    }
  }

  @MakeNotStatic
  private static volatile boolean stopping;

  /**
   * This starts up the watchdog and proctor threads. This method is called when a Cache is created.
   */
  public static void startThreads() {
    stopping = false;
    startWatchDog();
    startProctor();
  }

  /**
   * This stops the threads that implement this service. This method is called when a Cache is
   * closed.
   */
  public static void stopThreads() {
    // this method fixes bug 45409
    stopping = true;
    stopProctor();
    stopWatchDog();
  }

  static Thread getWatchDogForTest() {
    return watchDog;
  }

  static Thread getProctorForTest() {
    return proctor;
  }
}
