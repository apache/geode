/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: FIFOReadWriteLock.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  11Jun1998  dl               Create public version
  23nov2001  dl               Replace main algorithm with fairer
                              version based on one by Alexander Terekhov
*/

package com.gemstone.org.jgroups.oswego.concurrent;


/**
 * This class implements a policy for reader/writer locks in which
 * threads contend in a First-in/First-out manner for access (modulo
 * the limitations of FIFOSemaphore, which is used for queuing).  This
 * policy does not particularly favor readers or writers.  As a
 * byproduct of the FIFO policy, the <tt>attempt</tt> methods may
 * return <tt>false</tt> even when the lock might logically be
 * available, but, due to contention, cannot be accessed within the
 * given time bound.  <p>
 *
 * This lock is <em>NOT</em> reentrant. Current readers and
 * writers should not try to re-obtain locks while holding them.
 * <p>
 *
 * [<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>] <p>
 *
 * @see FIFOSemaphore
**/

public class FIFOReadWriteLock implements ReadWriteLock {

  /** 
   * Fair Semaphore serving as a kind of mutual exclusion lock.
   * Writers acquire on entry, and hold until rwlock exit.
   * Readers acquire and release only during entry (but are
   * blocked from doing so if there is an active writer).
   **/
  protected final FIFOSemaphore entryLock = new FIFOSemaphore(1);

  /** 
   * Number of threads that have entered read lock.  Note that this is
   * never reset to zero. Incremented only during acquisition of read
   * lock while the "entryLock" is held, but read elsewhere, so is
   * declared volatile.
   **/
  protected volatile int readers;

  /** 
   * Number of threads that have exited read lock.  Note that this is
   * never reset to zero. Accessed only in code protected by
   * synchronized(this). When exreaders != readers, the rwlock is
   * being used for reading. Else if the entry lock is held, it is
   * being used for writing (or in transition). Else it is free.
   * Note: To distinguish these states, we assume that fewer than 2^32
   * reader threads can simultaneously execute.
   **/
  protected int exreaders;

  protected void acquireRead() throws InterruptedException {
//    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition not necessary checked in acquire
    entryLock.acquire();
    ++readers; 
    entryLock.release();
  }

  protected synchronized void releaseRead() {
    /*
      If this is the last reader, notify a possibly waiting writer.
      Because waits occur only when entry lock is held, at most one
      writer can be waiting for this notification.  Because increments
      to "readers" aren't protected by "this" lock, the notification
      may be spurious (when an incoming reader in in the process of
      updating the field), but at the point tested in acquiring write
      lock, both locks will be held, thus avoiding false alarms. And
      we will never miss an opportunity to send a notification when it
      is actually needed.
    */

    if (++exreaders == readers) 
      notify(); 
  }

  protected void acquireWrite() throws InterruptedException {
//    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition not necessary checked in acquire
    // Acquiring entryLock first forces subsequent entering readers
    // (as well as writers) to block.
    entryLock.acquire();
    
    // Only read "readers" once now before loop.  We know it won't
    // change because we hold the entry lock needed to update it.
    int r = readers;
    
    try {
      synchronized(this) {
        while (exreaders != r) 
          wait();
      }
    }
    catch (InterruptedException ie) {
      entryLock.release();
      throw ie;
    }
  }

  protected void releaseWrite() {
    entryLock.release();
  }

  protected boolean attemptRead(long msecs) throws InterruptedException {
//    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition not necessary checked in attempt
    if (!entryLock.attempt(msecs)) 
      return false;

    ++readers; 
    entryLock.release();
    return true;
  }

  protected boolean attemptWrite(long msecs) throws InterruptedException {
//    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition not necessary checked in attempt
    long startTime = (msecs <= 0)? 0 : System.currentTimeMillis();

    if (!entryLock.attempt(msecs)) 
      return false;

    int r = readers;

    try {
      synchronized(this) {
        while (exreaders != r) {
          long timeLeft = (msecs <= 0)? 0:
            msecs - (System.currentTimeMillis() - startTime);
          
          if (timeLeft <= 0) {
            entryLock.release();
            return false;
          }
          
          wait(timeLeft);
        }
        return true;
      }
    }
    catch (InterruptedException ie) {
      entryLock.release();
      throw ie;
    }
  }

  // support for ReadWriteLock interface

  protected class ReaderSync implements Sync {
    public void acquire() throws InterruptedException {
//      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition not necessary checked in acquireRead
      acquireRead();
    }
    public  void release()  { 
      releaseRead();
    }
    public boolean attempt(long msecs) throws InterruptedException {
//      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition not necessary checked in attemptRead
      return attemptRead(msecs);
    }
  }

  protected class WriterSync implements Sync {
    public void acquire() throws InterruptedException {
//      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition not necessary checked in acquireWrite
      acquireWrite();
    }
    public  void release()  { 
      releaseWrite();
    }
    public boolean attempt(long msecs) throws InterruptedException {
//      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition not necessary checked in attemptWrite
      return attemptWrite(msecs);
    }
  }

  protected final Sync readerSync = new ReaderSync();
  protected final Sync writerSync = new WriterSync();

  public Sync writeLock() { return writerSync; }
  public Sync readLock() { return readerSync; }

}
