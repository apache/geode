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

/*
 * Written by members of JCP JSR-166 Expert Group and released to the
 * public domain. Use, modify, and redistribute this code in any way
 * without acknowledgement. Other contributors include Andrew Wright,
 * Jeffrey Hayes, Pat Fischer, Mike Judd.
 */
package com.gemstone.gemfire.util;

import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.ProtectionDomain;

import junit.framework.TestCase;

/**
 * Base class for JSR166 Junit TCK tests.  Defines some constants,
 * utility methods and classes, as well as a simple framework for
 * helping to make sure that assertions failing in generated threads
 * cause the associated test that generated them to itself fail (which
 * JUnit doe not otherwise arrange).  The rules for creating such
 * tests are:
 *
 * <ol>
 *
 * <li> All assertions in code running in generated threads must use
 * the forms threadFail, threadAssertTrue, 
 * threadAssertEquals, or threadAssertNull, (not
 * <tt>fail</tt>, <tt>assertTrue</tt>, etc.) It is OK (but not
 * particularly recommended) for other code to use these forms too.
 * Only the most typically used JUnit assertion methods are defined
 * this way, but enough to live with.</li>
 *
 * <li> If you override {@link junit.framework.TestCase#setUp} or {@link junit.framework.TestCase#tearDown}, make sure
 * to invoke <tt>super.setUp</tt> and <tt>super.tearDown</tt> within
 * them. These methods are used to clear and check for thread
 * assertion failures.</li>
 *
 * <li>All delays and timeouts must use one of the constants <tt>
 * SHORT_DELAY_MS</tt>, <tt> SMALL_DELAY_MS</tt>, <tt> MEDIUM_DELAY_MS</tt>,
 * <tt> LONG_DELAY_MS</tt>. The idea here is that a SHORT is always
 * discriminable from zero time, and always allows enough time for the
 * small amounts of computation (creating a thread, calling a few
 * methods, etc) needed to reach a timeout point. Similarly, a SMALL
 * is always discriminable as larger than SHORT and smaller than
 * MEDIUM.  And so on. These constants are set to conservative values,
 * but even so, if there is ever any doubt, they can all be increased
 * in one spot to rerun tests on slower platforms</li>
 *
 * <li> All threads generated must be joined inside each test case
 * method (or <tt>fail</tt> to do so) before returning from the
 * method. The <tt> joinPool</tt> method can be used to do this when
 * using Executors.</li>
 *
 * </ol>
 *
 * <p> <b>Other notes</b>
 * <ul>
 *
 * <li> Usually, there is one testcase method per JSR166 method
 * covering "normal" operation, and then as many exception-testing
 * methods as there are exceptions the method can throw. Sometimes
 * there are multiple tests per JSR166 method when the different
 * "normal" behaviors differ significantly. And sometimes testcases
 * cover multiple methods when they cannot be tested in
 * isolation.</li>
 * 
 * <li> The documentation style for testcases is to provide as javadoc
 * a simple sentence or two describing the property that the testcase
 * method purports to test. The javadocs do not say anything about how
 * the property is tested. To find out, read the code.</li>
 *
 * <li> These tests are "conformance tests", and do not attempt to
 * test throughput, latency, scalability or other performance factors
 * (see the separate "jtreg" tests for a set intended to check these
 * for the most central aspects of functionality.) So, most tests use
 * the smallest sensible numbers of threads, collection sizes, etc
 * needed to check basic conformance.</li>
 *
 * <li>The test classes currently do not declare inclusion in
 * any particular package to simplify things for people integrating
 * them in TCK test suites.</li>
 *
 * <li> As a convenience, the <tt>main</tt> of this class (JSR166TestCase)
 * runs all JSR166 unit tests.</li>
 *
 * </ul>
 *
 * @author Eric Zoerner
 *
 * @version Based on JSR166 TCK version 1.8
 */
public class JSR166TestCase extends TestCase {
    public static long SHORT_DELAY_MS;
    public static long SMALL_DELAY_MS;
    public static long MEDIUM_DELAY_MS;
    public static long LONG_DELAY_MS;

    public static Class<?> MAP_CLASS =
        com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap.class;

    public JSR166TestCase(String name) {
      super(name);
    } 
    
    /**
     * Return the shortest timed delay. This could
     * be reimplmented to use for example a Property.
     */ 
    protected long getShortDelay() {
        return 100;
    }


    /**
     * Set delays as multiples of SHORT_DELAY.
     */
    protected  void setDelays() {
        SHORT_DELAY_MS = getShortDelay();
        SMALL_DELAY_MS = SHORT_DELAY_MS * 5;
        MEDIUM_DELAY_MS = SHORT_DELAY_MS * 10;
        LONG_DELAY_MS = SHORT_DELAY_MS * 50;
    }

    /**
     * Flag set true if any threadAssert methods fail
     */
    volatile boolean threadFailed;

    /**
     * Initialize test to indicate that no thread assertions have failed
     */
    protected void setUp() throws Exception { 
        setDelays();
        threadFailed = false;  
    }

    /**
     * Trigger test case failure if any thread assertions have failed
     */
    protected void tearDown() throws Exception { 
        assertFalse(threadFailed);  
    }

    /**
     * Fail, also setting status to indicate current testcase should fail
     */ 
    public void threadFail(String reason) {
        threadFailed = true;
        fail(reason);
    }

    /**
     * If expression not true, set status to indicate current testcase
     * should fail
     */ 
    public void threadAssertTrue(boolean b) {
        if (!b) {
            threadFailed = true;
            assertTrue(b);
        }
    }

    /**
     * If expression not false, set status to indicate current testcase
     * should fail
     */ 
    public void threadAssertFalse(boolean b) {
        if (b) {
            threadFailed = true;
            assertFalse(b);
        }
    }

    /**
     * If argument not null, set status to indicate current testcase
     * should fail
     */ 
    public void threadAssertNull(Object x) {
        if (x != null) {
            threadFailed = true;
            assertNull(x);
        }
    }

    /**
     * If arguments not equal, set status to indicate current testcase
     * should fail
     */ 
    public void threadAssertEquals(long x, long y) {
        if (x != y) {
            threadFailed = true;
            assertEquals(x, y);
        }
    }

    /**
     * If arguments not equal, set status to indicate current testcase
     * should fail
     */ 
    public void threadAssertEquals(Object x, Object y) {
        if (x != y && (x == null || !x.equals(y))) {
            threadFailed = true;
            assertEquals(x, y);
        }
    }

    /**
     * threadFail with message "should throw exception"
     */ 
    public void threadShouldThrow() {
        threadFailed = true;
        fail("should throw exception");
    }

    /**
     * threadFail with message "Unexpected exception"
     */
    public void threadUnexpectedException() {
        threadFailed = true;
        fail("Unexpected exception");
    }


    /**
     * Wait out termination of a thread pool or fail doing so
     */
    /*
    public void joinPool(ExecutorService exec) {
        try {
            exec.shutdown();
            assertTrue(exec.awaitTermination(LONG_DELAY_MS, TimeUnit.MILLISECONDS));
        } catch(InterruptedException ie) {
            fail("Unexpected exception");
        }
    }
*/

    /**
     * fail with message "should throw exception"
     */ 
    public void shouldThrow() {
        fail("Should throw exception");
    }

    /**
     * fail with message "Unexpected exception"
     */
    public void unexpectedException() {
        fail("Unexpected exception");
    }


    /**
     * The number of elements to place in collections, arrays, etc.
     */
    public static final int SIZE = 20;

    // Some convenient Integer constants

    public static final Integer zero = new Integer(0);
    public static final Integer one = new Integer(1);
    public static final Integer two = new Integer(2);
    public static final Integer three  = new Integer(3);
    public static final Integer four  = new Integer(4);
    public static final Integer five  = new Integer(5);
    public static final Integer six = new Integer(6);
    public static final Integer seven = new Integer(7);
    public static final Integer eight = new Integer(8);
    public static final Integer nine = new Integer(9);
    public static final Integer m1  = new Integer(-1);
    public static final Integer m2  = new Integer(-2);
    public static final Integer m3  = new Integer(-3);
    public static final Integer m4 = new Integer(-4);
    public static final Integer m5 = new Integer(-5);
    public static final Integer m10 = new Integer(-10);


    /**
     * A security policy where new permissions can be dynamically added
     * or all cleared.
     */
    static class AdjustablePolicy extends java.security.Policy {
        Permissions perms = new Permissions();
        AdjustablePolicy() { }
        void addPermission(Permission perm) { perms.add(perm); }
        void clearPermissions() { perms = new Permissions(); }
	public PermissionCollection getPermissions(CodeSource cs) {
	    return perms;
	}
	public PermissionCollection getPermissions(ProtectionDomain pd) {
	    return perms;
	}
	public boolean implies(ProtectionDomain pd, Permission p) {
	    return perms.implies(p);
	}
	public void refresh() {}
    }


    // Some convenient Runnable classes

    static class NoOpRunnable implements Runnable {
        public void run() {}
    }

    /*
    static class NoOpCallable implements Callable {
        public Object call() { return Boolean.TRUE; }
    }
     */

    class ShortRunnable implements Runnable {
        public void run() {
            try {
                Thread.sleep(SHORT_DELAY_MS);
            }
            catch(Exception e) {
                threadUnexpectedException();
            }
        }
    }

    class ShortInterruptedRunnable implements Runnable {
        public void run() {
            try {
                Thread.sleep(SHORT_DELAY_MS);
                threadShouldThrow();
            }
            catch(InterruptedException success) {
            }
        }
    }

    class SmallRunnable implements Runnable {
        public void run() {
            try {
                Thread.sleep(SMALL_DELAY_MS);
            }
            catch(Exception e) {
                threadUnexpectedException();
            }
        }
    }

    class SmallPossiblyInterruptedRunnable implements Runnable {
        public void run() {
            try {
                Thread.sleep(SMALL_DELAY_MS);
            }
            catch(Exception e) {
            }
        }
    }
    
    /*
    class SmallCallable implements Callable {
        public Object call() {
            try {
                Thread.sleep(SMALL_DELAY_MS);
            }
            catch(Exception e) {
                threadUnexpectedException();
            }
            return Boolean.TRUE;
        }
    }
     */

    class SmallInterruptedRunnable implements Runnable {
        public void run() {
            try {
                Thread.sleep(SMALL_DELAY_MS);
                threadShouldThrow();
            }
            catch(InterruptedException success) {
            }
        }
    }


    class MediumRunnable implements Runnable {
        public void run() {
            try {
                Thread.sleep(MEDIUM_DELAY_MS);
            }
            catch(Exception e) {
                threadUnexpectedException();
            }
        }
    }

    class MediumInterruptedRunnable implements Runnable {
        public void run() {
            try {
                Thread.sleep(MEDIUM_DELAY_MS);
                threadShouldThrow();
            }
            catch(InterruptedException success) {
            }
        }
    }

    class MediumPossiblyInterruptedRunnable implements Runnable {
        public void run() {
            try {
                Thread.sleep(MEDIUM_DELAY_MS);
            }
            catch(InterruptedException success) {
            }
        }
    }

    /**
     * For use as ThreadFactory in constructors
     */
    /*
    static class SimpleThreadFactory implements ThreadFactory{
        public Thread newThread(Runnable r){
            return new Thread(r);
        }   
    }
     */

    static class TrackedShortRunnable implements Runnable {
        volatile boolean done = false;
        public void run() {
            try {
                Thread.sleep(SMALL_DELAY_MS);
                done = true;
            } catch(Exception e){
            }
        }
    }

    static class TrackedMediumRunnable implements Runnable {
        volatile boolean done = false;
        public void run() {
            try {
                Thread.sleep(MEDIUM_DELAY_MS);
                done = true;
            } catch(Exception e){
            }
        }
    }

    static class TrackedLongRunnable implements Runnable {
        volatile boolean done = false;
        public void run() {
            try {
                Thread.sleep(LONG_DELAY_MS);
                done = true;
            } catch(Exception e){
            }
        }
    }

    static class TrackedNoOpRunnable implements Runnable {
        volatile boolean done = false;
        public void run() {
            done = true;
        }
    }
}
