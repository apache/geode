/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain. Use, modify, and
 * redistribute this code in any way without acknowledgement.
 */

package com.gemstone.gemfire.internal.util;

/**
 * A task that returns a result and may throw an exception.
 * Implementors define a single method with no arguments called
 * <tt>call</tt>.
 *
 * <p>The <tt>Callable</tt> interface is similar to {@link
 * java.lang.Runnable}, in that both are designed for classes whose
 * instances are potentially executed by another thread.  A
 * <tt>Runnable</tt>, however, does not return a result and cannot
 * throw a checked exception.
 *
 * @author Eric Zoerner
 *
 * @version based on JSR166 Callable version 1.6.
 */
public interface Callable {
    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    Object call() throws Exception;
}
