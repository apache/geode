/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: Queue.java,v 1.28 2005/12/21 17:08:32 belaban Exp $

package com.gemstone.org.jgroups.util;


import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.TimeoutException;

import java.util.LinkedList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Enumeration;


/**
 * Elements are added at the tail and removed from the head. Class is thread-safe in that
 * 1 producer and 1 consumer may add/remove elements concurrently. The class is not
 * explicitely designed for multiple producers or consumers. Implemented as a linked
 * list, so that removal of an element at the head does not cause a right-shift of the
 * remaining elements (as in a Vector-based implementation).
 * @author Bela Ban
 */
public class Queue  {

    /*head and the tail of the list so that we can easily add and remove objects*/
    private Element head=null, tail=null;

    /*flag to determine the state of the queue*/
    private boolean closed=false;

    /*current size of the queue*/
    private int     size=0;

    /* Lock object for synchronization. Is notified when element is added */
    private final Object  mutex=new Object();

    /** Lock object for syncing on removes. It is notified when an object is removed */
    // Object  remove_mutex=new Object();

    /*the number of end markers that have been added*/
    private int     num_markers=0;

    /**
     * if the queue closes during the runtime
     * an endMarker object is added to the end of the queue to indicate that
     * the queue will close automatically when the end marker is encountered
     * This allows for a "soft" close.
     * @see Queue#close
     */
    private static final Object endMarker=new Object();

    protected static final GemFireTracer log=GemFireTracer.getLog(Queue.class);


    /**
     * the class Element indicates an object in the queue.
     * This element allows for the linked list algorithm by always holding a
     * reference to the next element in the list.
     * if Element.next is null, then this element is the tail of the list.
     */
    static class Element  {
        /*the actual value stored in the queue*/
        Object  obj=null;
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
    public Queue() {
    }


    /**
     * Returns the first element. Returns null if no elements are available.
     */
    public Object getFirst() {
        synchronized(mutex) {
            return head != null? head.obj : null;
        }
    }

    /**
     * Returns the last element. Returns null if no elements are available.
     */
    public Object getLast() {
        synchronized(mutex) {
            return tail != null? tail.obj : null;
        }
    }


    /**
     * returns true if the Queue has been closed
     * however, this method will return false if the queue has been closed
     * using the close(true) method and the last element has yet not been received.
     * @return true if the queue has been closed
     */
    public boolean closed() {
        synchronized(mutex) {
            return closed;
        }
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
            if(log.isErrorEnabled()) log.error(ExternalStrings.Queue_ARGUMENT_MUST_NOT_BE_NULL);
            return;
        }

        /*lock the queue from other threads*/
        synchronized(mutex) {
           if(closed)
              throw new QueueClosedException();
           if(this.num_markers > 0)
              throw new QueueClosedException("queue has been closed. You can not add more elements. " +
                                             "Waiting for removal of remaining elements.");
            addInternal(obj);

            /*wake up all the threads that are waiting for the lock to be released*/
            mutex.notifyAll();
        }
    }

    public void addAll(Collection c) throws QueueClosedException {
        if(c == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.Queue_ARGUMENT_MUST_NOT_BE_NULL);
            return;
        }

        /*lock the queue from other threads*/
        synchronized(mutex) {
           if(closed)
              throw new QueueClosedException();
           if(this.num_markers > 0)
              throw new QueueClosedException("queue has been closed. You can not add more elements. " +
                                             "Waiting for removal of remaining elements.");

            Object obj;
            for(Iterator it=c.iterator(); it.hasNext();) {
                obj=it.next();
                if(obj != null)
                    addInternal(obj);
            }

            /*wake up all the threads that are waiting for the lock to be released*/
            mutex.notifyAll();
        }
    }


    public void addAll(List l) throws QueueClosedException {
        if(l == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.Queue_ARGUMENT_MUST_NOT_BE_NULL);
            return;
        }

        /*lock the queue from other threads*/
        synchronized(mutex) {
           if(closed)
              throw new QueueClosedException();
           if(this.num_markers > 0)
              throw new QueueClosedException("queue has been closed. You can not add more elements. " +
                                             "Waiting for removal of remaining elements.");

            Object obj;
            for(Enumeration en=l.elements(); en.hasMoreElements();) {
                obj=en.nextElement();
                if(obj != null)
                    addInternal(obj);
            }

            /*wake up all the threads that are waiting for the lock to be released*/
            mutex.notifyAll();
        }
    }




    /**
     * Adds a new object to the head of the queue
     * basically (obj.equals(queue.remove(queue.add(obj)))) returns true
     * If the queue has been closed with close(true) no exception will be
     * thrown if the queue has not been flushed yet.
     * @param obj - the object to be added to the queue
     * @exception QueueClosedException exception if closed() returns true
     */
    public void addAtHead(Object obj) throws QueueClosedException {
        if(obj == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.Queue_ARGUMENT_MUST_NOT_BE_NULL);
            return;
        }

        /*lock the queue from other threads*/
        synchronized(mutex) {
           if(closed)
              throw new QueueClosedException();
           if(this.num_markers > 0)
              throw new QueueClosedException("Queue.addAtHead(): queue has been closed. You can not add more elements. " +
                                             "Waiting for removal of remaining elements.");

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
            mutex.notifyAll();
        }
    }


    /**
     * Removes 1 element from head or <B>blocks</B>
     * until next element has been added or until queue has been closed
     * @return the first element to be taken of the queue
     */
    public Object remove() throws QueueClosedException
      , InterruptedException  { // GemStoneAddition
        Object retval;
        if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
        synchronized(mutex) {
            /*wait as long as the queue is empty. return when an element is present or queue is closed*/
            while(size == 0) {
                if(closed)
                    throw new QueueClosedException();
                // [GemStoneAddition] Don't wait if we were interrupted
                if (Thread.currentThread().isInterrupted()) {
//                  Thread.currentThread().interrupt();
                  throw new InterruptedException();
                }
                // GemStoneAddition - don't eat the interrupt
                //try {
                    mutex.wait();
                //}
                //catch(InterruptedException ex) {
                //}
            }

            if(closed)
                throw new QueueClosedException();

            /*remove the head from the queue, if we make it to this point, retval should not be null !*/
            retval=removeInternal();
            if(retval == null)
                if(log.isErrorEnabled()) log.error(ExternalStrings.Queue_ELEMENT_WAS_NULL_SHOULD_NEVER_BE_THE_CASE);
        }

        /*
         * we ran into an Endmarker, which means that the queue was closed before
         * through close(true)
         */
//        if(retval == endMarker) {
//            close(false); // mark queue as closed
//            throw new QueueClosedException();
//        }
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
        Object retval;

        synchronized(mutex) {
            if(closed)
                throw new QueueClosedException();

            /*if the queue size is zero, we want to wait until a new object is added*/
            if(size == 0) {
                try {
                    /*release the mutex lock and wait no more than timeout ms*/
                    mutex.wait(timeout);
                }
                catch(InterruptedException ex) {
                  Thread.currentThread().interrupt(); // GemStoneAddition
                }
            }
            /*we either timed out, or got notified by the mutex lock object*/
            if(closed)
                throw new QueueClosedException();

            /*get the next value*/
            retval=removeInternal();
            /*null result means we timed out*/
            if(retval == null) throw new TimeoutException("timeout=" + timeout + "ms");

            /*if we reached an end marker we are going to close the queue*/
//            if(retval == endMarker) {
//                close(false);
//                throw new QueueClosedException();
//            }
            /*at this point we actually did receive a value from the queue, return it*/
            return retval;
        }
    }


    /**
     * removes a specific object from the queue.
     * the object is matched up using the Object.equals method.
     * @param   obj the actual object to be removed from the queue
     */
    public void removeElement(Object obj) throws QueueClosedException {
        Element el, tmp_el;

        if(obj == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.Queue_ARGUMENT_MUST_NOT_BE_NULL);
            return;
        }

        synchronized(mutex) {
            if(closed) /*check to see if the queue is closed*/
                throw new QueueClosedException();

            el=head;

            /*the queue is empty*/
            if(el == null) return;

            /*check to see if the head element is the one to be removed*/
            if(el.obj.equals(obj)) {
                /*the head element matched we will remove it*/
                head=el.next;
                el.next=null;
                el.obj=null;
                /*check if we only had one object left
                 *at this time the queue becomes empty
                 *this will set the tail=head=null
                 */
                if(size == 1)
                    tail=head;  // null
                decrementSize();
                return;
            }

            /*look through the other elements*/
            while(el.next != null) {
                if(el.next.obj.equals(obj)) {
                    tmp_el=el.next;
                    if(tmp_el == tail) // if it is the last element, move tail one to the left (bela Sept 20 2002)
                        tail=el;
                    el.next.obj=null;
                    el.next=el.next.next;  // point to the el past the next one. can be null.
                    tmp_el.next=null;
                    tmp_el.obj=null;
                    decrementSize();
                    break;
                }
                el=el.next;
            }
        }
    }


    /**
     * returns the first object on the queue, without removing it.
     * If the queue is empty this object blocks until the first queue object has
     * been added
     * @return the first object on the queue
     */
    public Object peek() throws QueueClosedException {
        Object retval;

        synchronized(mutex) {
            while(size == 0) {
                if(closed)
                    throw new QueueClosedException();
                boolean interrupted = Thread.interrupted(); // GemStoneAddition
                try {
                    mutex.wait();
                }
                catch(InterruptedException ex) {
                  interrupted = true; // GemStoneAddition
                }
                finally {
                  if (interrupted) Thread.currentThread().interrupt();
                }
            }

            if(closed)
                throw new QueueClosedException();

            retval=(head != null)? head.obj : null;
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
        Object retval;

        synchronized(mutex) {
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

            if(retval == null) throw new TimeoutException("timeout=" + timeout + "ms");

            if(retval == endMarker) {
                close(false);
                throw new QueueClosedException();
            }
            return retval;
        }
    }

    /** Removes all elements from the queue. This method can succeed even when the queue is closed */
    public void clear() {
        synchronized(mutex) {
            head=tail=null;
            size=0;
            num_markers=0;
            mutex.notifyAll();
        }
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
        synchronized(mutex) {
            if(flush_entries && size > 0) {
                try {
                    add(endMarker); // add an end-of-entries marker to the end of the queue
                    num_markers++;
                }
                catch(QueueClosedException closed_ex) {
                }
                return;
            }
            closed=true;
            mutex.notifyAll();
        }
    }

    /** Waits until the queue has been closed. Returns immediately if already closed
     * @param timeout Number of milliseconds to wait. A value <= 0 means to wait forever
     */
    public void waitUntilClosed(long timeout) {
        synchronized(mutex) {
            if(closed)
                return;
            try {
                mutex.wait(timeout);
            }
            catch(InterruptedException e) {
              Thread.currentThread().interrupt(); // GemStoneAddition
            }
        }
    }


    /**
     * resets the queue.
     * This operation removes all the objects in the queue and marks the queue open
     */
    public void reset() {
        synchronized(mutex) {
           num_markers=0;
           if(!closed)
              close(false);
            size=0;
            head=null;
            tail=null;
            closed=false;
            mutex.notifyAll();
        }
    }

    /**
     * Returns all the elements of the queue
     * @return A copy of the queue
     */
    public LinkedList values() {
        LinkedList retval=new LinkedList();
        synchronized(mutex) {
            Element el=head;
            while(el != null) {
                retval.add(el.obj);
                el=el.next;
            }
        }
        return retval;
    }


    /**
     * returns the number of objects that are currently in the queue
     */
    public int size() {
        synchronized(mutex) {
            return size - num_markers;
        }
    }

    /**
     * prints the size of the queue
     */
    @Override // GemStoneAddition
    public String toString() {
        return "Queue (" + size() + ") messages";
    }




    /* ------------------------------------- Private Methods ----------------------------------- */


    private final void addInternal(Object obj) {
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
    }

    /**
     * Removes the first element. Returns null if no elements in queue.
     * Always called with mutex locked (we don't have to lock mutex ourselves)
     */
    private Object removeInternal() {
        Element retval;
        Object obj;

        /*if the head is null, the queue is empty*/
        if(head == null)
            return null;

        retval=head;       // head must be non-null now

        head=head.next;
        if(head == null)
            tail=null;

        decrementSize();
        if(head != null && head.obj == endMarker) {
            closed=true;
            mutex.notifyAll();
        }

        retval.next=null;
        obj=retval.obj;
        retval.obj=null;
        return obj;
    }


    /** Doesn't need to be synchronized; is always called from synchronized methods */
    final private void decrementSize() {
        size--;
        if(size < 0)
            size=0;
    }


    /* ---------------------------------- End of Private Methods -------------------------------- */

}
