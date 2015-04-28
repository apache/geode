/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: NullSync.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  1Aug1998  dl               Create public version
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * A No-Op implementation of Sync. Acquire never blocks,
 * Attempt always succeeds, Release has no effect.
 * However, acquire and release do detect interruption
 * and throw InterruptedException. Also, the methods
 * are synchronized, so preserve memory barrier properties
 * of Syncs.
 * <p>
 * NullSyncs can be useful in optimizing classes when
 * it is found that locking is not strictly necesssary.
 *
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
**/


public class NullSync implements Sync {

  public synchronized void acquire() throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
  }

  public synchronized boolean attempt(long msecs) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    return true;
  }

  public synchronized void release() {}


}


