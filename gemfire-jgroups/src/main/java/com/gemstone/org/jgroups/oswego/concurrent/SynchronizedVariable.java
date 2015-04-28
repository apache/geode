/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: SynchronizedVariable.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  30Jun1998  dl               Create public version
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * Base class for simple,  small classes 
 * maintaining single values that are always accessed
 * and updated under synchronization. Since defining them for only
 * some types seemed too arbitrary, they exist for all basic types,
 * although it is hard to imagine uses for some.
 * <p>
 *   These classes mainly exist so that you do not have to go to the
 *   trouble of writing your own miscellaneous classes and methods
 *   in situations  including:
 *  <ul>
 *   <li> When  you need or want to offload an instance 
 *    variable to use its own synchronization lock.
 *    When these objects are used to replace instance variables, they
 *    should almost always be declared as <code>final</code>. This
 *    helps avoid the need to synchronize just to obtain the reference
 *    to the synchronized variable itself.
 *
 *    <li> When you need methods such as set, commit, or swap.
 *    Note however that
 *    the synchronization for these variables is <em>independent</em>
 *    of any other synchronization perfromed using other locks. 
 *    So, they are not
 *    normally useful when accesses and updates among 
 *    variables must be coordinated.
 *    For example, it would normally be a bad idea to make
 *    a Point class out of two SynchronizedInts, even those
 *    sharing a lock.
 *
 *    <li> When defining <code>static</code> variables. It almost
 *    always works out better to rely on synchronization internal
 *    to these objects, rather  than class locks.
 *  </ul>
 * <p>
 * While they cannot, by nature, share much code,
 * all of these classes work in the same way.
 * <p>
 * <b>Construction</b> <br>
 * Synchronized variables are always constructed holding an
 * initial value of the associated type. Constructors also
 * establish the lock to use for all methods:
 * <ul>
 *   <li> By default, each variable uses itself as the
 *        synchronization lock. This is the most common
 *        choice in the most common usage contexts in which
 *        SynchronizedVariables are used to split off
 *        synchronization locks for independent attributes
 *        of a class.
 *   <li> You can specify any other Object to use as the
 *        synchronization lock. This allows you to
 *        use various forms of `slave synchronization'. For
 *        example, a variable that is always associated with a
 *        particular object can use that object's lock.
 * </ul>
 * <p>
 * <b>Update methods</b><br>
 * Each class supports several kinds of update methods:
 * <ul>
 *   <li> A <code>set</code> method that sets to a new value and returns 
 *    previous value. For example, for a SynchronizedBoolean b,
 *    <code>boolean old = b.set(true)</code> performs a test-and-set.
 * <p>
 *   <li> A  <code>commit</code> method that sets to new value only
 *    if currently holding a given value.
 * 
 * For example, here is a class that uses an optimistic update
 * loop to recompute a count variable represented as a 
 * SynchronizedInt. 
 *  <pre>
 *  class X {
 *    private final SynchronizedInt count = new SynchronizedInt(0);
 * 
 *    static final int MAX_RETRIES = 1000;
 *
 *    public boolean recomputeCount() throws InterruptedException {
 *      for (int i = 0; i &lt; MAX_RETRIES; ++i) {
 *        int current = count.get();
 *        int next = compute(current);
 *        if (count.commit(current, next))
 *          return true;
 *        else if (Thread.interrupted()) 
 *          throw new InterruptedException();
 *      }
 *      return false;
 *    }
 *    int compute(int l) { ... some kind of computation ...  }
 *  }
 * </pre>
 * <p>
 *   <li>A <code>swap</code> method that atomically swaps with another 
 *    object of the same class using a deadlock-avoidance strategy.
 * <p>
 *    <li> Update-in-place methods appropriate to the type. All
 *    numerical types support:
 *     <ul>
 *       <li> add(x) (equivalent to return value += x)
 *       <li> subtract(x) (equivalent to return value -= x)
 *       <li> multiply(x) (equivalent to return value *= x)
 *       <li> divide(x) (equivalent to return value /= x)
 *     </ul>
 *   Integral types also support:
 *     <ul>
 *       <li> increment() (equivalent to return ++value)
 *       <li> decrement() (equivalent to return --value)
 *     </ul>
 *    Boolean types support:
 *     <ul>
 *       <li> or(x) (equivalent to return value |= x)
 *       <li> and(x) (equivalent to return value &amp;= x)
 *       <li> xor(x) (equivalent to return value ^= x)
 *       <li> complement() (equivalent to return x = !x)
 *     </ul>
 *    These cover most, but not all of the possible operators in Java.
 *    You can add more compute-and-set methods in subclasses. This
 *    is often a good way to avoid the need for ad-hoc synchronized
 *    blocks surrounding expressions.
 *  </ul>
 * <p>
 * <b>Guarded methods</b> <br>
 *   All <code>Waitable</code> subclasses provide notifications on
 *   every value update, and support guarded methods of the form
 *   <code>when</code><em>predicate</em>, that wait until the
 *   predicate hold,  then optionally run any Runnable action
 *   within the lock, and then return. All types support:
 *     <ul>
 *       <li> whenEqual(value, action)
 *       <li> whenNotEqual(value, action)
 *     </ul>
 *   (If the action argument is null, these return immediately
 *   after the predicate holds.)
 *   Numerical types also support 
 *     <ul>
 *       <li> whenLess(value, action)
 *       <li> whenLessEqual(value, action)
 *       <li> whenGreater(value, action)
 *       <li> whenGreaterEqual(value, action)
 *     </ul>
 *   The Waitable classes are not always spectacularly efficient since they
 *   provide notifications on all value changes.  They are
 *   designed for use in contexts where either performance is not an
 *   overriding issue, or where nearly every update releases guarded
 *   waits anyway.
 *  <p>
 * <b>Other methods</b> <br>
 *   This class implements Executor, and provides an <code>execute</code>
 *   method that runs the runnable within the lock.
 *   <p>
 *   All classes except SynchronizedRef and WaitableRef implement
 *   <code>Cloneable</code> and <code>Comparable</code>.
 *   Implementations of the corresponding
 *   methods either use default mechanics, or use methods that closely
 *   correspond to their java.lang analogs. SynchronizedRef does not
 *   implement any of these standard interfaces because there are
 *   many cases where it would not make sense. However, you can
 *   easily make simple subclasses that add the appropriate declarations.
 *
 *  <p>
 *
 *
 *
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/

public class SynchronizedVariable implements Executor {

  protected final Object lock_;

  /** Create a SynchronizedVariable using the supplied lock **/
  public SynchronizedVariable(Object lock) { lock_ = lock; }

  /** Create a SynchronizedVariable using itself as the lock **/
  public SynchronizedVariable() { lock_ = this; }

  /**
   * Return the lock used for all synchronization for this object
   **/
  public Object getLock() { return lock_; }

  /**
   * If current thread is not interrupted, execute the given command 
   * within this object's lock
   **/

  public void execute(Runnable command) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    synchronized (lock_) { 
      command.run();
    }
  }
}
