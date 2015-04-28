/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: LayeredSync.java

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
 * A class that can be used to compose Syncs.
 * A LayeredSync object manages two other Sync objects,
 * <em>outer</em> and <em>inner</em>. The acquire operation
 * invokes <em>outer</em>.acquire() followed by <em>inner</em>.acquire(),
 * but backing out of outer (via release) upon an exception in inner.
 * The other methods work similarly.
 * <p>
 * LayeredSyncs can be used to compose arbitrary chains
 * by arranging that either of the managed Syncs be another
 * LayeredSync.
 *
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
**/


public class LayeredSync implements Sync {

  protected final Sync outer_;
  protected final Sync inner_;

  /** 
   * Create a LayeredSync managing the given outer and inner Sync
   * objects
   **/

  public LayeredSync(Sync outer, Sync inner) {
    outer_ = outer;
    inner_ = inner;
  }

  public void acquire() throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    outer_.acquire();
    try {
      inner_.acquire();
    }
    catch (InterruptedException ex) {
      outer_.release();
      throw ex;
    }
  }

  public boolean attempt(long msecs) throws InterruptedException {

    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition - for safety
    long start = (msecs <= 0)? 0 : System.currentTimeMillis();
    long waitTime = msecs;

    if (outer_.attempt(waitTime)) {
      try {
        if (msecs > 0)
          waitTime = msecs - (System.currentTimeMillis() - start);
        if (inner_.attempt(waitTime))
          return true;
        else {
          outer_.release();
          return false;
        }
      }
      catch (InterruptedException ex) {
        outer_.release();
        throw ex;
      }
    }
    else
      return false;
  }

  public void release() {
    inner_.release();
    outer_.release();
  }

}


