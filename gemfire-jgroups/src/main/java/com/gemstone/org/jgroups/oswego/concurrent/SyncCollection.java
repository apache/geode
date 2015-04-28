/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: SyncCollection.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
   1Aug1998  dl               Create public version
*/

package com.gemstone.org.jgroups.oswego.concurrent;
import java.util.*;

/**
 * SyncCollections wrap Sync-based control around java.util.Collections.
 * They are similar in operation to those provided
 * by java.util.Collection.synchronizedCollection, but have
 * several extended capabilities.
 * <p>
 * The Collection interface is conceptually broken into two
 * parts for purposes of synchronization control. The  purely inspective
 * reader operations are:
 * <ul>
 *  <li> size
 *  <li> isEmpty
 *  <li> toArray
 *  <li> contains
 *  <li> containsAll
 *  <li> iterator
 * </ul>
 * The possibly mutative writer operations (which are also
 * the set of operations that are allowed to throw 
 * UnsupportedOperationException) are:
 * <ul>
 *  <li> add
 *  <li> addAll
 *  <li> remove
 *  <li> clear
 *  <li> removeAll
 *  <li> retainAll
 * </ul>
 *  
 * <p>
 * SyncCollections can be used with either Syncs or ReadWriteLocks.
 * When used with
 * single Syncs, the same lock is used as both the reader and writer lock.
 * The SyncCollection class cannot itself guarantee that using
 * a pair of read/write locks will always correctly protect objects, since
 * Collection implementations are not precluded from internally
 * performing hidden unprotected state changes within conceptually read-only
 * operations. However, they do work with current java.util implementations.
 * (Hopefully, implementations that do not provide this natural
 * guarantee will be clearly documentented as such.)
 * <p>
 * This class provides a straight implementation of Collections interface.
 * In order to conform to this interface, sync failures
 * due to interruption do NOT result in InterruptedExceptions. 
 * Instead, upon detection of interruption, 
 * <ul>
 *  <li> All mutative operations convert the interruption to
 *    an UnsupportedOperationException, while also propagating
 *    the interrupt status of the thread. Thus, unlike normal
 *    java.util.Collections, SyncCollections can <em>transiently</em>
 *    behave as if mutative operations are not supported.
 *  <li> All read-only operations 
 *     attempt to return a result even upon interruption. In some contexts,
 *     such results will be meaningless due to interference, but 
 *     provide best-effort status indications that can be useful during
 *     recovery. The cumulative number of synchronization failures encountered
 *     during such operations is accessible using method 
 *     <code>synchronizationFailures()</code>.  
 *     Non-zero values may indicate serious program errors.
 * </ul>
 * <p>
 * The iterator() method returns a SyncCollectionIterator with 
 * properties and methods that are analogous to those of SyncCollection
 * itself: hasNext and next are read-only, and remove is mutative.
 * These methods allow fine-grained controlled access, but do <em>NOT</em>
 * preclude concurrent modifications from being interleaved with traversals,
 * which may lead to ConcurrentModificationExceptions.
 * However, the class also supports method <code>unprotectedIterator</code>
 * that can be used in conjunction with the <code>readerSync</code> or
 * <code>writerSync</code> methods to perform locked traversals. For example,
 * to protect a block of reads:
 * <pre>
 *    Sync lock = coll.readerSync();
 *    try {
 *      lock.acquire();
 *      try {
 *        Iterator it = coll.unprotectedIterator();
 *        while (it.hasNext()) 
 *          System.out.println(it.next());
 *      }
 *      finally {
 *        lock.release();
 *      }
 *   }
 *   catch (InterruptedException ex) { ... }
 * </pre>
 * If you need to protect blocks of writes, you must use some
 * form of <em>reentrant</em> lock (for example <code>ReentrantLock</code>
 * or <code>ReentrantWriterPreferenceReadWriteLock</code>) as the Sync 
 * for the collection in order to allow mutative methods to proceed
 * while the current thread holds the lock. For example, you might
 * need to hold a write lock during an initialization sequence:
 * <pre>
 *   Collection c = new SyncCollection(new ArrayList(), 
 *                                     new ReentrantWriterPreferenceReadWriteLock());
 *   // ...
 *   c.writeLock().acquire();
 *   try {
 *     for (...) {
 *       Object x = someStream.readObject();
 *       c.add(x); // would block if writeLock not reentrant
 *     }
 *   }
 *   catch (IOException iox) {
 *     ...
 *   }
 *   finally {
 *     c.writeLock().release();
 *   }
 *   catch (InterruptedException ex) { ... }
 * </pre>
 * <p>
 * (It would normally be better practice here to not make the
 * collection accessible until initialization is complete.)
 * <p>
 * This class does not specifically support use of
 * timed synchronization through the attempt method. However,
 * you can obtain this effect via
 * the TimeoutSync class. For example:
 * <pre>
 * Mutex lock = new Mutex();
 * TimeoutSync timedLock = new TimeoutSync(lock, 1000); // 1 sec timeouts
 * Collection c = new SyncCollection(new HashSet(), timedlock);
 * </pre>
 * <p>
 * The same can be done with read-write locks:
 * <pre>
 * ReadWriteLock rwl = new WriterPreferenceReadWriteLock();
 * Sync rlock = new TimeoutSync(rwl.readLock(), 100);
 * Sync wlock = new TimeoutSync(rwl.writeLock(), 100);
 * Collection c = new SyncCollection(new HashSet(), rlock, wlock);
 * </pre>
 * <p>
 * In addition to synchronization control, SyncCollections
 * may be useful in any context requiring before/after methods
 * surrounding collections. For example, you can use ObservableSync
 * to arrange notifications on method calls to collections, as in:
 * <pre>
 * class X {
 *   Collection c;
 *
 *   static class CollectionObserver implements ObservableSync.SyncObserver {
 *     public void onAcquire(Object arg) {
 *       Collection coll = (Collection) arg;
 *       System.out.println("Starting operation on" + coll);
 *       // Other plausible responses include performing integrity
 *       //   checks on the collection, updating displays, etc
 *     }
 *     public void onRelease(Object arg) {
 *       Collection coll = (Collection) arg;
 *       System.out.println("Finished operation on" + coll);
 *     }
 *   }
 *
 *   X() {
 *     ObservableSync s = new ObservableSync();
 *     c = new SyncCollection(new HashSet(), s);
 *     s.setNotificationArgument(c);
 *     CollectionObserver obs = new CollectionObserver();
 *     s.attach(obs);
 *   }
 *   ...
 * }
 * </pre>
 *
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 * @see LayeredSync
 * @see TimeoutSync
**/


public class SyncCollection implements Collection {
  protected final Collection c_;	   // Backing Collection
  protected final Sync rd_;  //  sync for read-only methods
  protected final Sync wr_;  //  sync for mutative methods

  protected final SynchronizedLong syncFailures_ = new SynchronizedLong(0);

  /**
   * Create a new SyncCollection protecting the given collection,
   * and using the given sync to control both reader and writer methods.
   * Common, reasonable choices for the sync argument include
   * Mutex, ReentrantLock, and Semaphores initialized to 1.
   * <p>
   * <b>Sample Usage</b>
   * <pre>
   * Collection c = new SyncCollection(new ArrayList(), new Mutex()); 
   * </pre>
   **/
  public SyncCollection(Collection collection, Sync sync) {
    this (collection, sync, sync);
  }


  /**
   * Create a new SyncCollection protecting the given collection,
   * and using the given ReadWriteLock to control reader and writer methods.
   * <p>
   * <b>Sample Usage</b>
   * <pre>
   * Collection c = new SyncCollection(new HashSet(), 
   *                                   new WriterPreferenceReadWriteLock());
   * </pre>
   **/
  public SyncCollection(Collection collection, ReadWriteLock rwl) {
    this (collection, rwl.readLock(), rwl.writeLock());
  }

  /**
   * Create a new SyncCollection protecting the given collection,
   * and using the given pair of locks to control reader and writer methods.
   **/
  public SyncCollection(Collection collection, Sync readLock, Sync writeLock) {
    c_ = collection; 
    rd_ = readLock;
    wr_ = writeLock;
  }

  /** 
   * Return the Sync object managing read-only operations
   **/
      
  public Sync readerSync() {
    return rd_;
  }

  /** 
   * Return the Sync object managing mutative operations
   **/

  public Sync writerSync() {
    return wr_;
  }

  /**
   * Return the number of synchronization failures for read-only operations
   **/
  public long syncFailures() {
    return syncFailures_.get();
  }


  /** Try to acquire sync before a reader operation; record failure **/
  protected boolean beforeRead() {
    try {
      rd_.acquire();
      return false;
    }
    catch (InterruptedException ex) { 
      Thread.currentThread().interrupt(); // GemStoneAddition
      syncFailures_.increment();
      return true; 
    }
  }

  /** Clean up after a reader operation **/
  protected void afterRead(boolean wasInterrupted) {
    if (wasInterrupted) {
      Thread.currentThread().interrupt();
    }
    else
      rd_.release();
  }



  public int size() {
    boolean wasInterrupted = beforeRead();
    try {
      return c_.size();
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public boolean isEmpty() {
    boolean wasInterrupted = beforeRead();
    try {
      return c_.isEmpty();
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public boolean contains(Object o) {
    boolean wasInterrupted = beforeRead();
    try {
      return c_.contains(o);
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public Object[] toArray() {
    boolean wasInterrupted = beforeRead();
    try {
      return c_.toArray();
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public Object[] toArray(Object[] a) {
    boolean wasInterrupted = beforeRead();
    try {
      return c_.toArray(a);
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public boolean containsAll(Collection coll) {
    boolean wasInterrupted = beforeRead();
    try {
      return c_.containsAll(coll);
    }
    finally {
      afterRead(wasInterrupted);
    }
  }


  public boolean add(Object o) {
    try {
      wr_.acquire();
      try {
        return c_.add(o);
      }
      finally {
        wr_.release();
      }
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new UnsupportedOperationException();
    }
  }

  public boolean remove(Object o) {
    try {
      wr_.acquire();
      try {
        return c_.remove(o);
      }
      finally {
        wr_.release();
      }
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new UnsupportedOperationException();
    }
  }

  public boolean addAll(Collection coll) {
    try {
      wr_.acquire();
      try {
        return c_.addAll(coll);
      }
      finally {
        wr_.release();
      }
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new UnsupportedOperationException();
    }
  }

  public boolean removeAll(Collection coll) {
    try {
      wr_.acquire();
      try {
        return c_.removeAll(coll);
      }
      finally {
        wr_.release();
      }
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new UnsupportedOperationException();
    }
  }


  public boolean retainAll(Collection coll) {
    try {
      wr_.acquire();
      try {
        return c_.retainAll(coll);
      }
      finally {
        wr_.release();
      }
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new UnsupportedOperationException();
    }
  }

	
  public void clear() {
    try {
      wr_.acquire();
      try {
        c_.clear();
      }
      finally {
        wr_.release();
      }
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new UnsupportedOperationException();
    }
  }


  /** Return the base iterator of the underlying collection **/
  public Iterator unprotectedIterator() {
    boolean wasInterrupted = beforeRead();
    try {
      return c_.iterator(); 
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public Iterator iterator() {
    boolean wasInterrupted = beforeRead();
    try {
      return new SyncCollectionIterator(c_.iterator()); 
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public class SyncCollectionIterator implements Iterator {
    protected final Iterator baseIterator_;

    SyncCollectionIterator(Iterator baseIterator) {
      baseIterator_ = baseIterator;
    }

    public boolean hasNext() {
      boolean wasInterrupted = beforeRead();
      try {
        return baseIterator_.hasNext();
      }
      finally {
        afterRead(wasInterrupted);
      }
    }

    public Object next() {
      boolean wasInterrupted = beforeRead();
      try {
        return baseIterator_.next();
      }
      finally {
        afterRead(wasInterrupted);
      }
    }

    public void remove() {
      try {
        wr_.acquire();
        try {
           baseIterator_.remove();
        }
        finally {
          wr_.release();
        }
      }
      catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new UnsupportedOperationException();
      }
    }

  }
}


