/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: Channel.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  11Jun1998  dl               Create public version
  25aug1998  dl               added peek
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/** 
 * Main interface for buffers, queues, pipes, conduits, etc.
 * <p>
 * A Channel represents anything that you can put items
 * into and take them out of. As with the Sync 
 * interface, both
 * blocking (put(x), take),
 * and timeouts (offer(x, msecs), poll(msecs)) policies
 * are provided. Using a
 * zero timeout for offer and poll results in a pure balking policy.
 * <p>
 * To aid in efforts to use Channels in a more typesafe manner,
 * this interface extends Puttable and Takable. You can restrict
 * arguments of instance variables to this type as a way of
 * guaranteeing that producers never try to take, or consumers put.
 * for example:
 * <pre>
 * class Producer implements Runnable {
 *   final Puttable chan;
 *   Producer(Puttable channel) { chan = channel; }
 *   public void run() {
 *     try {
 *       for(;;) { chan.put(produce()); }
 *     }
 *     catch (InterruptedException ex) {}
 *   }
 *   Object produce() { ... }
 * }
 *
 *
 * class Consumer implements Runnable {
 *   final Takable chan;
 *   Consumer(Takable channel) { chan = channel; }
 *   public void run() {
 *     try {
 *       for(;;) { consume(chan.take()); }
 *     }
 *     catch (InterruptedException ex) {}
 *   }
 *   void consume(Object x) { ... }
 * }
 *
 * class Setup {
 *   void main() {
 *     Channel chan = new SomeChannelImplementation();
 *     Producer p = new Producer(chan);
 *     Consumer c = new Consumer(chan);
 *     new Thread(p).start();
 *     new Thread(c).start();
 *   }
 * }
 * </pre>
 * <p>
 * A given channel implementation might or might not have bounded
 * capacity or other insertion constraints, so in general, you cannot tell if
 * a given put will block. However,
 * Channels that are designed to 
 * have an element capacity (and so always block when full)
 * should implement the 
 * BoundedChannel 
 * subinterface.
 * <p>
 * Channels may hold any kind of item. However,
 * insertion of null is not in general supported. Implementations
 * may (all currently do) throw IllegalArgumentExceptions upon attempts to
 * insert null. 
 * <p>
 * By design, the Channel interface does not support any methods to determine
 * the current number of elements being held in the channel.
 * This decision reflects the fact that in
 * concurrent programming, such methods are so rarely useful
 * that including them invites misuse; at best they could 
 * provide a snapshot of current
 * state, that could change immediately after being reported.
 * It is better practice to instead use poll and offer to try
 * to take and put elements without blocking. For example,
 * to empty out the current contents of a channel, you could write:
 * <pre>
 *  try {
 *    for (;;) {
 *       Object item = channel.poll(0);
 *       if (item != null)
 *         process(item);
 *       else
 *         break;
 *    }
 *  }
 *  catch(InterruptedException ex) { ... }
 * </pre>
 * <p>
 * However, it is possible to determine whether an item
 * exists in a Channel via <code>peek</code>, which returns
 * but does NOT remove the next item that can be taken (or null
 * if there is no such item). The peek operation has a limited
 * range of applicability, and must be used with care. Unless it
 * is known that a given thread is the only possible consumer
 * of a channel, and that no time-out-based <code>offer</code> operations
 * are ever invoked, there is no guarantee that the item returned
 * by peek will be available for a subsequent take.
 * <p>
 * When appropriate, you can define an isEmpty method to
 * return whether <code>peek</code> returns null.
 * <p>
 * Also, as a compromise, even though it does not appear in interface,
 * implementation classes that can readily compute the number
 * of elements support a <code>size()</code> method. This allows careful
 * use, for example in queue length monitors, appropriate to the
 * particular implementation constraints and properties.
 * <p>
 * All channels allow multiple producers and/or consumers.
 * They do not support any kind of <em>close</em> method
 * to shut down operation or indicate completion of particular
 * producer or consumer threads. 
 * If you need to signal completion, one way to do it is to
 * create a class such as
 * <pre>
 * class EndOfStream { 
 *    // Application-dependent field/methods
 * }
 * </pre>
 * And to have producers put an instance of this class into
 * the channel when they are done. The consumer side can then
 * check this via
 * <pre>
 *   Object x = aChannel.take();
 *   if (x instanceof EndOfStream) 
 *     // special actions; perhaps terminate
 *   else
 *     // process normally
 * </pre>
 * <p>
 * In time-out based methods (poll(msecs) and offer(x, msecs), 
 * time bounds are interpreted in
 * a coarse-grained, best-effort fashion. Since there is no
 * way in Java to escape out of a wait for a synchronized
 * method/block, time bounds can sometimes be exceeded when
 * there is a lot contention for the channel. Additionally,
 * some Channel semantics entail a ``point of
 * no return'' where, once some parts of the operation have completed,
 * others must follow, regardless of time bound.
 * <p>
 * Interruptions are in general handled as early as possible
 * in all methods. Normally, InterruptionExceptions are thrown
 * in put/take and offer(msec)/poll(msec) if interruption
 * is detected upon entry to the method, as well as in any
 * later context surrounding waits. 
 * <p>
 * If a put returns normally, an offer
 * returns true, or a put or poll returns non-null, the operation
 * completed successfully. 
 * In all other cases, the operation fails cleanly -- the
 * element is not put or taken.
 * <p>
 * As with Sync classes, spinloops are not directly supported,
 * are not particularly recommended for routine use, but are not hard 
 * to construct. For example, here is an exponential backoff version:
 * <pre>
 * Object backOffTake(Channel q) throws InterruptedException {
 *   long waitTime = 0;
 *   for (;;) {
 *      Object x = q.poll(0);
 *      if (x != null)
 *        return x;
 *      else {
 *        Thread.sleep(waitTime);
 *        waitTime = 3 * waitTime / 2 + 1;
 *      }
 *    }
 * </pre>
 * <p>
 * <b>Sample Usage</b>. Here is a producer/consumer design
 * where the channel is used to hold Runnable commands representing
 * background tasks.
 * <pre>
 * class Service {
 *   private final Channel channel = ... some Channel implementation;
 *  
 *   private void backgroundTask(int taskParam) { ... }
 *
 *   public void action(final int arg) {
 *     Runnable command = 
 *       new Runnable() {
 *         public void run() { backgroundTask(arg); }
 *       };
 *     try { channel.put(command) }
 *     catch (InterruptedException ex) {
 *       Thread.currentThread().interrupt(); // ignore but propagate
 *     }
 *   }
 * 
 *   public Service() {
 *     Runnable backgroundLoop = 
 *       new Runnable() {
 *         public void run() {
 *           for (;;) {
 *             try {
 *               Runnable task = (Runnable)(channel.take());
 *               task.run();
 *             }
 *             catch (InterruptedException ex) { return; }
 *           }
 *         }
 *       };
 *     new Thread(backgroundLoop).start();
 *   }
 * }
 *    
 * </pre>
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 * @see Sync 
 * @see BoundedChannel 
**/

public interface Channel extends Puttable, Takable {

  /** 
   * Place item in the channel, possibly waiting indefinitely until
   * it can be accepted. Channels implementing the BoundedChannel
   * subinterface are generally guaranteed to block on puts upon
   * reaching capacity, but other implementations may or may not block.
   * @param item the element to be inserted. Should be non-null.
   * @exception InterruptedException if the current thread has
   * been interrupted at a point at which interruption
   * is detected, in which case the element is guaranteed not
   * to be inserted. Otherwise, on normal return, the element is guaranteed
   * to have been inserted.
  **/
  public void put(Object item) throws InterruptedException;

  /** 
   * Place item in channel only if it can be accepted within
   * msecs milliseconds. The time bound is interpreted in
   * a coarse-grained, best-effort fashion. 
   * @param item the element to be inserted. Should be non-null.
   * @param msecs the number of milliseconds to wait. If less than
   * or equal to zero, the method does not perform any timed waits,
   * but might still require
   * access to a synchronization lock, which can impose unbounded
   * delay if there is a lot of contention for the channel.
   * @return true if accepted, else false
   * @exception InterruptedException if the current thread has
   * been interrupted at a point at which interruption
   * is detected, in which case the element is guaranteed not
   * to be inserted (i.e., is equivalent to a false return).
  **/
  public boolean offer(Object item, long msecs) throws InterruptedException;

  /** 
   * Return and remove an item from channel, 
   * possibly waiting indefinitely until
   * such an item exists.
   * @return  some item from the channel. Different implementations
   *  may guarantee various properties (such as FIFO) about that item
   * @exception InterruptedException if the current thread has
   * been interrupted at a point at which interruption
   * is detected, in which case state of the channel is unchanged.
   *
  **/
  public Object take() throws InterruptedException;


  /** 
   * Return and remove an item from channel only if one is available within
   * msecs milliseconds. The time bound is interpreted in a coarse
   * grained, best-effort fashion.
   * @param msecs the number of milliseconds to wait. If less than
   *  or equal to zero, the operation does not perform any timed waits,
   * but might still require
   * access to a synchronization lock, which can impose unbounded
   * delay if there is a lot of contention for the channel.
   * @return some item, or null if the channel is empty.
   * @exception InterruptedException if the current thread has
   * been interrupted at a point at which interruption
   * is detected, in which case state of the channel is unchanged
   * (i.e., equivalent to a null return).
  **/

  public Object poll(long msecs) throws InterruptedException;

  /**
   * Return, but do not remove object at head of Channel,
   * or null if it is empty.
   **/

  public Object peek();


}

