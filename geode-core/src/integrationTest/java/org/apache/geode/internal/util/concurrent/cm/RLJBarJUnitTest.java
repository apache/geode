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
// Yet another contended object monitor throughput test
// adapted from bug reports
package org.apache.geode.internal.util.concurrent.cm;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

import org.apache.geode.util.JSR166TestCase;

public class RLJBarJUnitTest extends JSR166TestCase { // TODO: reformat

  public static final int ITERS = 10;
  public static boolean OneKey = false; // alloc once or once per iteration

  public static boolean UseBar = false;
  public static int nThreads = 100;
  public static int nUp = 0;
  public static int nDead = 0;
  public static ReentrantLock bar = new ReentrantLock();
  public static Condition barCondition = bar.newCondition();
  public static long epoch;
  public static ReentrantLock DeathRow = new ReentrantLock();
  public static ReentrantLock End = new ReentrantLock();
  public static int quiesce = 0;
  public static Condition EndCondition = End.newCondition();

  @Test
  public void testRLJBar() throws Exception {
    main(new String[0]);
  }

  public static void main(String[] args) {
    int argix = 0;
    if (argix < args.length && args[argix].equals("-o")) {
      ++argix;
      OneKey = true;
      System.out.println("OneKey");
    }
    if (argix < args.length && args[argix].equals("-b")) {
      ++argix;
      UseBar = true;
      System.out.println("UseBar");
    }
    if (argix < args.length && args[argix].equals("-q")) {
      ++argix;
      if (argix < args.length) {
        quiesce = Integer.parseInt(args[argix++]);
        System.out.println("Quiesce " + quiesce + " msecs");
      }
    }
    for (int k = 0; k < ITERS; ++k) {
      oneRun();
    }
  }

  public static void oneRun() {
    DeathRow = new ReentrantLock();
    End = new ReentrantLock();
    EndCondition = End.newCondition();

    nDead = nUp = 0;
    long cyBase = System.currentTimeMillis();
    DeathRow.lock();
    try {
      for (int i = 1; i <= nThreads; i++) {
        new Producer("Producer" + i).start();
      }
      try {
        End.lock();
        try {
          while (nDead != nThreads) {
            EndCondition.await();
          }
        } finally {
          End.unlock();
        }
      } catch (Exception ex) {
        System.out.println("Exception in End: " + ex);
      }
    } finally {
      DeathRow.unlock();
    }
    System.out.println("Outer time: " + (System.currentTimeMillis() - cyBase));

    // Let workers quiesce/exit.
    try {
      Thread.sleep(1000);
    } catch (Exception ex) {
    } ;
  }
}


class Producer extends Thread {
  // private static Hashtable buddiesOnline = new Hashtable();
  private static final Map buddiesOnline;

  public Producer(String name) {
    super(name);
  }

  static {
    try {
      buddiesOnline = (Map) JSR166TestCase.MAP_CLASS.newInstance();
    } catch (Exception ex) {
      throw new ExceptionInInitializerError(ex);
    }
  }

  @Override
  public void run() {
    Object key = null;
    final ReentrantLock dr = RLJBarJUnitTest.DeathRow;
    final ReentrantLock bar = RLJBarJUnitTest.bar;
    final ReentrantLock end = RLJBarJUnitTest.End;
    final Condition endCondition = RLJBarJUnitTest.EndCondition;
    if (RLJBarJUnitTest.OneKey) {
      key = new Integer(0); // per-thread v. per iteration
    }

    // The barrier has a number of interesting effects:
    // 1. It enforces full LWP provisioning on T1.
    // (nearly all workers park concurrently).
    // 2. It gives the C2 compiler thread(s) a chance to run.
    // By transiently quiescing the workings the C2 threads
    // might avoid starvation.
    //

    try {
      bar.lock();
      try {
        ++RLJBarJUnitTest.nUp;
        if (RLJBarJUnitTest.nUp == RLJBarJUnitTest.nThreads) {
          if (RLJBarJUnitTest.quiesce != 0) {
            RLJBarJUnitTest.barCondition.await(RLJBarJUnitTest.quiesce * 1000000,
                TimeUnit.NANOSECONDS);
          }
          RLJBarJUnitTest.epoch = System.currentTimeMillis();
          RLJBarJUnitTest.barCondition.signalAll();
          // System.out.print ("Consensus ") ;
        }
        if (RLJBarJUnitTest.UseBar) {
          while (RLJBarJUnitTest.nUp != RLJBarJUnitTest.nThreads) {
            RLJBarJUnitTest.barCondition.await();
          }
        }
      } finally {
        bar.unlock();
      }
    } catch (Exception ex) {
      System.out.println("Exception in barrier: " + ex);
    }

    // Main execution time ... the code being timed ...
    // HashTable.get() is highly contended (serial).
    for (int loop = 1; loop < 100000; loop++) {
      if (!RLJBarJUnitTest.OneKey) {
        key = new Integer(0);
      }
      buddiesOnline.get(key);
    }

    // Mutator epilog:
    // The following code determines if the test will/wont include (measure)
    // thread death time.

    end.lock();
    try {
      ++RLJBarJUnitTest.nDead;
      if (RLJBarJUnitTest.nDead == RLJBarJUnitTest.nUp) {
        // System.out.print((System.currentTimeMillis()-RLJBar.epoch) + " ms") ;
        endCondition.signalAll();
      }
    } finally {
      end.unlock();
    }
    dr.lock();
    dr.unlock();
  }
}
