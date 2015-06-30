/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import com.gemstone.gemfire.cache.*;

import util.TestException;
import dunit.*;
import junit.framework.AssertionFailedError;

/**
 * A helper class that provides the {@link SerializableRunnable}
 * class, but uses a {@link #run2} method instead that throws {@link
 * CacheException}.  This way, we don't need to have a lot of
 * try/catch code in the tests.
 *
 * @author David Whitlock
 *
 * @since 3.0
 */
public abstract class CacheSerializableRunnable
  extends SerializableRunnable 
  implements RepeatableRunnable 
{

  /**
   * Creates a new <code>CacheSerializableRunnable</code> with the
   * given name
   */
  public CacheSerializableRunnable(String name) {
    super(name);
  }

  /**
   * Invokes the {@link #run2} method and will wrap any {@link
   * CacheException} thrown by <code>run2</code> in a {@link
   * CacheSerializableRunnableException}. 
   */
  public final void run() {
    try {
      run2();

    } catch (CacheException ex) {
      String s = "While invoking \"" + this + "\"";
      throw new CacheSerializableRunnableException(s, ex);
    }
  }
  
  /**
   * Invokes the {@link #run} method.  If AssertionFailedError is thrown,
   * and repeatTimeoutMs is >0, then repeat the {@link #run} method until
   * it either succeeds or repeatTimeoutMs milliseconds have passed.  The
   * AssertionFailedError is only thrown to the caller if the last run
   * still throws it.
   */
  public final void runRepeatingIfNecessary(long repeatTimeoutMs) {
    long start = System.currentTimeMillis();
    AssertionFailedError lastErr = null;
    do {
      try {
        lastErr = null;
        this.run();
        CacheFactory.getAnyInstance().getLogger().fine("Completed " + this);
      } catch (AssertionFailedError err) {
        CacheFactory.getAnyInstance().getLogger().fine("Repeating " + this);
        lastErr = err;
        try {
          Thread.sleep(50);
        } catch (InterruptedException ex) {
          throw new TestException("interrupted", ex);
        }
      }
    } while (lastErr != null && System.currentTimeMillis() - start < repeatTimeoutMs);
    if (lastErr != null) throw lastErr;
  }

  /**
   * A {@link SerializableRunnable#run run} method that may throw a
   * {@link CacheException}.
   */
  public abstract void run2() throws CacheException;

  /////////////////////////  Inner Classes  /////////////////////////

  /**
   * An exception that wraps a {@link CacheException}
   */
  public static class CacheSerializableRunnableException 
    extends CacheRuntimeException {

    public CacheSerializableRunnableException(String message,
                                              Throwable cause) {
      super(message, cause);
    }

  }

}
