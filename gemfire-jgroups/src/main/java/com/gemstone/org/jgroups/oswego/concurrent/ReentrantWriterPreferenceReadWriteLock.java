/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: ReentrantWriterPreferenceReadWriteLock.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  26aug1998  dl                 Create public version
   7sep2000  dl                 Readers are now also reentrant
  19jan2001  dl                 Allow read->write upgrades if the only reader 
  10dec2002  dl                 Throw IllegalStateException on extra release
*/

package com.gemstone.org.jgroups.oswego.concurrent;
import java.util.*;

/** 
 * A writer-preference ReadWriteLock that allows both readers and 
 * writers to reacquire
 * read or write locks in the style of a ReentrantLock.
 * Readers are not allowed until all write locks held by
 * the writing thread have been released.
 * Among other applications, reentrancy can be useful when
 * write locks are held during calls or callbacks to methods that perform
 * reads under read locks.
 * <p>
 * <b>Sample usage</b>. Here is a code sketch showing how to exploit
 * reentrancy to perform lock downgrading after updating a cache:
 * <pre>
 * class CachedData {
 *   Object data;
 *   volatile boolean cacheValid;
 *   ReentrantWriterPreferenceReadWriteLock rwl = ...
 *
 *   void processCachedData() {
 *     rwl.readLock().acquire();
 *     if (!cacheValid) {
 *
 *        // upgrade lock:
 *        rwl.readLock().release();   // must release first to obtain writelock
 *        rwl.writeLock().acquire();
 *        if (!cacheValid) { // recheck
 *          data = ...
 *          cacheValid = true;
 *        }
 *        // downgrade lock
 *        rwl.readLock().acquire();  // reacquire read without giving up lock
 *        rwl.writeLock().release(); // release write, still hold read
 *     }
 *
 *     use(data);
 *     rwl.readLock().release();
 *   }
 * }
 * </pre>
 *
 * 
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 * @see ReentrantLock
 **/

public class ReentrantWriterPreferenceReadWriteLock extends WriterPreferenceReadWriteLock {

  /** Number of acquires on write lock by activeWriter_ thread **/
  protected long writeHolds_ = 0;  

  /** Number of acquires on read lock by any reader thread **/
  protected HashMap readers_ = new HashMap();

  /** cache/reuse the special Integer value one to speed up readlocks **/
  protected static final Integer IONE = Integer.valueOf(1);


  @Override // GemStoneAddition
  protected boolean allowReader() {
    return (activeWriter_ == null && waitingWriters_ == 0) ||
      activeWriter_ == Thread.currentThread();
  }

  @Override
  protected synchronized boolean startRead() {
    Thread t = Thread.currentThread();
    Object c = readers_.get(t);
    if (c != null) { // already held -- just increment hold count
      readers_.put(t, Integer.valueOf(((Integer)(c)).intValue()+1));
      ++activeReaders_;
      return true;
    }
    else if (allowReader()) {
      readers_.put(t, IONE);
      ++activeReaders_;
      return true;
    }
    else
      return false;
  }

  @Override
  protected synchronized boolean startWrite() {
    if (activeWriter_ == Thread.currentThread()) { // already held; re-acquire
      ++writeHolds_;
      return true;
    }
    else if (writeHolds_ == 0) {
      if (activeReaders_ == 0 || 
          (readers_.size() == 1 && 
           readers_.get(Thread.currentThread()) != null)) {
        activeWriter_ = Thread.currentThread();
        writeHolds_ = 1;
        return true;
      }
      else
        return false;
    }
    else
      return false;
  }


  @Override
  protected synchronized Signaller endRead() {
    Thread t = Thread.currentThread();
    Object c = readers_.get(t);
    if (c == null)
      throw new IllegalStateException();
    --activeReaders_;
    if (c != IONE) { // more than one hold; decrement count
      int h = ((Integer)(c)).intValue()-1;
      Integer ih = (h == 1)? IONE : Integer.valueOf(h);
      readers_.put(t, ih);
      return null;
    }
    else {
      readers_.remove(t);
    
      if (writeHolds_ > 0) // a write lock is still held by current thread
        return null;
      else if (activeReaders_ == 0 && waitingWriters_ > 0)
        return writerLock_;
      else
        return null;
    }
  }

  @Override
  protected synchronized Signaller endWrite() {
    --writeHolds_;
    if (writeHolds_ > 0)   // still being held
      return null;
    else {
      activeWriter_ = null;
      if (waitingReaders_ > 0 && allowReader())
        return readerLock_;
      else if (waitingWriters_ > 0)
        return writerLock_;
      else
        return null;
    }
  }

}

