/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Queue2.java,v 1.5 2004/12/31 14:10:40 belaban Exp $

package com.gemstone.org.jgroups.util;


import com.gemstone.org.jgroups.oswego.concurrent.CondVar;
import com.gemstone.org.jgroups.oswego.concurrent.Mutex;
import com.gemstone.org.jgroups.oswego.concurrent.Sync;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.TimeoutException;

import java.util.Vector;




/**
 * Elements are added at the tail and removed from the head. Class is thread-safe in that
 * 1 producer and 1 consumer may add/remove elements concurrently. The class is not
 * explicitely designed for multiple producers or consumers. Implemented as a linked
 * list, so that removal of an element at the head does not cause a right-shift of the
 * remaining elements (as in a Vector-based implementation).
 * <p>Implementation is based on util.concurrent.* classes
 * @author Bela Ban
 * @author Filip Hanik
 */
public class Queue2  {
    /*head and the tail of the list so that we can easily add and remove objects*/
    Element head=null, tail=null;

    /*flag to determine the state of the queue*/
    boolean closed=false;

    /*current size of the queue*/
    int     size=0;

    /* Lock object for synchronization. Is notified when element is added */
    final Sync    mutex=new Mutex();

    /** Signals to listeners when an element has been added */
    final CondVar add_condvar=new CondVar(mutex);

    /** Signals to listeners when an element has been removed */
    final CondVar remove_condvar=new CondVar(mutex);

    /*the number of end markers that have been added*/
    int     num_markers=0;

    protected static final GemFireTracer log=GemFireTracer.getLog(Queue2.class);


    /**
     * if the queue closes during the runtime
     * an endMarker object is added to the end of the queue to indicate that
     * the queue will close automatically when the end marker is encountered
     * This allows for a "soft" close.
     * @see Queue#close
     */
    private static final Object endMarker=new Object();

    /**
     * the class Element indicates an object in the queue.
     * This element allows for the linked list algorithm by always holding a
     * reference to the next element in the list.
     * if Element.next is null, then this element is the tail of the list.
     */
    static/*GemStoneAddition*/ class Element {
        /*the actual value stored in the queue*/
        Object obj=null;
        /*pointer to the next item in the (queue) linked list*/
        Element next=null;

        /**
         * creates an Element object holding its value
         * @param o - the object to be stored in the queue position
         */
        Element(Object o) {
            obj=o;
        }

        /**
         * prints out the value of the object
         */
        @Override // GemStoneAddition
        public String toString() {
            return obj != null? obj.toString() : "null";
        }
    }


    /**
     * creates an empty queue
     */
    public Queue2() {
    }


    /**
     * Returns the first element. Returns null if no elements are available.
     */
    public Object getFirst() {
        return head != null? head.obj : null;
    }

    /**
     * Returns the last element. Returns null if no elements are available.
     */
    public Object getLast() {
        return tail != null? tail.obj : null;
    }


    /**
     * returns true if the Queue has been closed
     * however, this method will return false if the queue has been closed
     * using the close(true) method and the last element has yet not been received.
     * @return true if the queue has been closed
     */
    public boolean closed() {
        return closed;
    }

    /**
     * adds an object to the tail of this queue
     * If the queue has been closed with close(true) no exception will be
     * thrown if the queue has not been flushed yet.
     * @param obj - the object to be added to the queue
     * @exception QueueClosedException exception if closed() returns true
     */
    public void add(Object obj) throws QueueClosedException {
        if(obj == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.Queue2_ARGUMENT_MUST_NOT_BE_NULL);
            return;
        }
        if(closed)
            throw new QueueClosedException();
        if(this.num_markers > 0)
            throw new QueueClosedException("Queue2.add(): queue has been closed. You can not add more elements. " +
                                           "Waiting for removal of remaining elements.");

        try {
            mutex.acquire();

            /*create a new linked list element*/
            Element el=new Element(obj);
            /*check the first element*/
            if(head == null) {
                /*the object added is the first element*/
                /*set the head to be this object*/
                head=el;
                /*set the tail to be this object*/
                tail=head;
                /*set the size to be one, since the queue was empty*/
                size=1;
            }
            else {
                /*add the object to the end of the linked list*/
                tail.next=el;
                /*set the tail to point to the last element*/
                tail=el;
                /*increase the size*/
                size++;
            }
            /*wake up all the threads that are waiting for the lock to be released*/
            add_condvar.broadcast(); // todo: maybe signal is all we need ?
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
        }
        finally {
            mutex.release();
        }
    }


    /**
     * Adds a new object to the head of the queue
     * basically (obj.equals(queue.remove(queue.add(obj)))) returns true
     * If the queue has been closed with close(true) no exception will be
     * thrown if the queue has not been flushed yet.
     * @param obj - the object to be added to the queue
     * @exception QueueClosedException exception if closed() returns true
     *
     */
    public void addAtHead(Object obj) throws QueueClosedException {
        if(obj == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.Queue2_ARGUMENT_MUST_NOT_BE_NULL);
            return;
        }
        if(closed)
            throw new QueueClosedException();
        if(this.num_markers > 0)
            throw new QueueClosedException("Queue2.addAtHead(): queue has been closed. You can not add more elements. " +
                                           "Waiting for removal of remaining elements.");

        try {
            mutex.acquire();
            Element el=new Element(obj);
            /*check the head element in the list*/
            if(head == null) {
                /*this is the first object, we could have done add(obj) here*/
                head=el;
                tail=head;
                size=1;
            }
            else {
                /*set the head element to be the child of this one*/
                el.next=head;
                /*set the head to point to the recently added object*/
                head=el;
                /*increase the size*/
                size++;
            }
            /*wake up all the threads that are waiting for the lock to be released*/
            add_condvar.broadcast();
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
        }
        finally {
            mutex.release();
        }
    }


    /**
     * Removes 1 element from head or <B>blocks</B>
     * until next element has been added or until queue has been closed
     * @return the first element to be taken of the queue
     */
    public Object remove() throws QueueClosedException {
        /*initialize the return value*/
        Object retval=null;

        try {
            mutex.acquire();
            /*wait as long as the queue is empty. return when an element is present or queue is closed*/
            while(size == 0) {
                if(closed)
                    throw new QueueClosedException();
                boolean interrupted = Thread.interrupted();
                try {
                    add_condvar.await();
                }
                catch(InterruptedException ex) {
                  interrupted = true; // GemStoneAddition
                }
                finally { // GemStoneAddition
                  if (interrupted) {
                    Thread.currentThread().interrupt();
                  }
                }
            }

            if(closed)
                throw new QueueClosedException();

            /*remove the head from the queue, if we make it to this point, retval should not be null !*/
            retval=removeInternal();
            if(retval == null)
                if(log.isErrorEnabled()) log.error(ExternalStrings.Queue2_ELEMENT_WAS_NULL_SHOULD_NEVER_BE_THE_CASE);
        }
        catch(InterruptedException e) {
            Thread.currentThread().interrupt(); // GemStoneAddition
        }
        finally {
            mutex.release();
        }

        /*
         * we ran into an Endmarker, which means that the queue was closed before
         * through close(true)
         */
        if(retval == endMarker) {
            close(false); // mark queue as closed
            throw new QueueClosedException();
        }

        /*return the object*/
        return retval;
    }


    /**
     * Removes 1 element from the head.
     * If the queue is empty the operation will wait for timeout ms.
     * if no object is added during the timeout time, a Timout exception is thrown
     * @param timeout - the number of milli seconds this operation will wait before it times out
     * @return the first object in the queue
     */
    public Object remove(long timeout) throws QueueClosedException, TimeoutException {
        Object retval=null;

        try {
            mutex.acquire();
                      /*if the queue size is zero, we want to wait until a new object is added*/
            if(size == 0) {
                if(closed)
                    throw new QueueClosedException();
                try {
                    /*release the mutex lock and wait no more than timeout ms*/
                    add_condvar.timedwait(timeout);
                }
                catch(InterruptedException ex) {
                  Thread.currentThread().interrupt(); // GemStoneAddition
                }
            }
            /*we either timed out, or got notified by the mutex lock object*/

            /*check to see if the object closed*/
            if(closed)
                throw new QueueClosedException();

            /*get the next value*/
            retval=removeInternal();
            /*null result means we timed out*/
            if(retval == null) throw new TimeoutException();

            /*if we reached an end marker we are going to close the queue*/
            if(retval == endMarker) {
                close(false);
                throw new QueueClosedException();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
        }
        finally {
            mutex.release();
        }

        /*at this point we actually did receive a value from the queue, return it*/
        return retval;
    }


    /**
     * removes a specific object from the queue.
     * the object is matched up using the Object.equals method.
     * @param   obj the actual object to be removed from the queue
     */
    public void removeElement(Object obj) throws QueueClosedException {
        Element el, tmp_el;
        boolean removed=false;

        if(obj == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.Queue2_ARGUMENT_MUST_NOT_BE_NULL);
            return;
        }

        try {
            mutex.acquire();
            el=head;

            /*the queue is empty*/
            if(el == null) return;

            /*check to see if the head element is the one to be removed*/
            if(el.obj.equals(obj)) {
                /*the head element matched we will remove it*/
                head=el.next;
                el.next=null;
                /*check if we only had one object left
                 *at this time the queue becomes empty
                 *this will set the tail=head=null
                 */
                if(size == 1)
                    tail=head;  // null
                decrementSize();
                removed=true;

                /*and end the operation, it was successful*/
                return;
            }

            /*look through the other elements*/
            while(el.next != null) {
                if(el.next.obj.equals(obj)) {
                    tmp_el=el.next;
                    if(tmp_el == tail) // if it is the last element, move tail one to the left (bela Sept 20 2002)
                        tail=el;
                    el.next=el.next.next;  // point to the el past the next one. can be null.
                    tmp_el.next=null;
                    decrementSize();
                    removed=true;
                    break;
                }
                el=el.next;
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
        }
        finally {
            if(removed)
                remove_condvar.broadcast();
            mutex.release();
        }
    }


    /**
     * returns the first object on the queue, without removing it.
     * If the queue is empty this object blocks until the first queue object has
     * been added
     * @return the first object on the queue
     */
    public Object peek() throws QueueClosedException {
        Object retval=null;

        try {
            mutex.acquire();
            while(size == 0) {
                if(closed)
                    throw new QueueClosedException();
                boolean interrupted = Thread.interrupted(); // GemStoneAddition
                try {
                    add_condvar.await();
                }
                catch(InterruptedException ex) {
                  interrupted = true; // GemStoneAddition
                }
                finally {
                  if (interrupted) {
                    Thread.currentThread().interrupt();
                  }
                }
            }

            if(closed)
                throw new QueueClosedException();

            retval=(head != null)? head.obj : null;

            // @remove:
            if(retval == null) {
                // print some diagnostics
                if(log.isErrorEnabled()) log.error("retval is null: head=" + head + ", tail=" + tail + ", size()=" + size() +
                        ", num_markers=" + num_markers + ", closed()=" + closed());
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
        }
        finally {
            mutex.release();
        }

        if(retval == endMarker) {
            close(false); // mark queue as closed
            throw new QueueClosedException();
        }

        return retval;
    }


    /**
     * returns the first object on the queue, without removing it.
     * If the queue is empty this object blocks until the first queue object has
     * been added or the operation times out
     * @param timeout how long in milli seconds will this operation wait for an object to be added to the queue
     *        before it times out
     * @return the first object on the queue
     */

    public Object peek(long timeout) throws QueueClosedException, TimeoutException {
        Object retval=null;

        try {
            mutex.acquire();
            if(size == 0) {
                if(closed)
                    throw new QueueClosedException();
                try {
                    mutex.wait(timeout);
                }
                catch(InterruptedException ex) {
                  Thread.currentThread().interrupt(); // GemStoneAddition
                }
            }
            if(closed)
                throw new QueueClosedException();

            retval=head != null? head.obj : null;

            if(retval == null) throw new TimeoutException();

            if(retval == endMarker) {
                close(false);
                throw new QueueClosedException();
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
        }
        finally {
            mutex.release();
        }
        return retval;
    }


    /**
     Marks the queues as closed. When an <code>add</code> or <code>remove</code> operation is
     attempted on a closed queue, an exception is thrown.
     @param flush_entries When true, a end-of-entries marker is added to the end of the queue.
     Entries may be added and removed, but when the end-of-entries marker
     is encountered, the queue is marked as closed. This allows to flush
     pending messages before closing the queue.
     */
    public void close(boolean flush_entries) {
        if(flush_entries) {
            try {
                add(endMarker); // add an end-of-entries marker to the end of the queue
                num_markers++;
            }
            catch(QueueClosedException closed) {
            }
            return;
        }

        try {
            mutex.acquire();
            closed=true;
            try {
                add_condvar.broadcast();
                remove_condvar.broadcast();
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.Queue2_EXCEPTION_0, e);
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
        }
        finally {
            mutex.release();
        }
    }


    /**
     * resets the queue.
     * This operation removes all the objects in the queue and marks the queue open
     */
    public void reset() {
        num_markers=0;
        if(!closed)
            close(false);

        try {
            mutex.acquire();
            size=0;
            head=null;
            tail=null;
            closed=false;
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
        }
        finally {
            mutex.release();
        }
    }


    /**
     * returns the number of objects that are currently in the queue
     */
    public int size() {
        return size - num_markers;
    }

    /**
     * prints the size of the queue
     */
    @Override // GemStoneAddition
    public String toString() {
        return "Queue2 (" + size() + ") messages";
    }

    /**
     * Dumps internal state @remove
     */
    public String debug() {
        return toString() + ", head=" + head + ", tail=" + tail + ", closed()=" + closed() + ", contents=" + getContents();
    }


    /**
     * returns a vector with all the objects currently in the queue
     */
    public Vector getContents() {
        Vector retval=new Vector();
        Element el;

        try {
            mutex.acquire();
            el=head;
            while(el != null) {
                retval.addElement(el.obj);
                el=el.next;
            }
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
        }
        finally {
            mutex.release();
        }
        return retval;
    }


    /**
     * Blocks until the queue has no elements left. If the queue is empty, the call will return
     * immediately
     * @param timeout Call returns if timeout has elapsed (number of milliseconds). 0 means to wait forever
     * @throws QueueClosedException Thrown if queue has been closed
     * @throws TimeoutException Thrown if timeout has elapsed
     */
    public void waitUntilEmpty(long timeout) throws QueueClosedException, TimeoutException {
        long time_to_wait=timeout >=0? timeout : 0; // eliminate negative values

        try {
            mutex.acquire();

            if(timeout == 0) {
                while(size > 0 && closed == false) {
                    remove_condvar.await();
                }
            }
            else {
                long start_time=System.currentTimeMillis();

                while(time_to_wait > 0 && size > 0 && closed == false) {
                    boolean interrupted = Thread.interrupted(); // GemStoneAddition
                    try {
                        remove_condvar.timedwait(time_to_wait);
                    }
                    catch(InterruptedException ex) {
                      interrupted = true; // GemStoneAddition
                    }
                    finally { // GemStoneAddition
                      if (interrupted) {
                        Thread.currentThread().interrupt();
                      }
                    }
                    time_to_wait=timeout - (System.currentTimeMillis() - start_time);
                }

                if(size > 0)
                    throw new TimeoutException("queue has " + size + " elements");
            }

            if(closed)
                throw new QueueClosedException();
        }
        catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
        }
        finally {
            mutex.release();
        }
    }


    /* ------------------------------------- Private Methods ----------------------------------- */


    /**
     * Removes the first element. Returns null if no elements in queue.
     * Always called with mutex locked (we don't have to lock mutex ourselves)
     */
    private Object removeInternal() {
        Element retval;

        /*if the head is null, the queue is empty*/
        if(head == null)
            return null;

        retval=head;       // head must be non-null now

        head=head.next;
        if(head == null)
            tail=null;

        decrementSize();

        remove_condvar.broadcast(); // todo: correct ?

        if(head != null && head.obj == endMarker) {
            closed=true;
        }

        retval.next=null;
        return retval.obj;
    }


    void decrementSize() {
        size--;
        if(size < 0)
            size=0;
    }


    /* ---------------------------------- End of Private Methods -------------------------------- */

}
