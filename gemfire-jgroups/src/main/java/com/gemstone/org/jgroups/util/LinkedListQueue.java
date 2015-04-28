/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: LinkedListQueue.java,v 1.6 2004/09/23 16:29:56 belaban Exp $

package com.gemstone.org.jgroups.util;


import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.TimeoutException;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Vector;


/**
 * LinkedListQueue implementation based on java.util.Queue. Can be renamed to Queue.java and compiled if someone wants to
 * use this implementation rather than the original Queue. However, a simple insertion and removal of 1 million
 * objects into this queue shoed that it was ca. 15-20% slower than the original queue. We just include it in the
 * JGroups distribution to maybe use it at a later point when it has become faster.
 *
 * @author Bela Ban
 */
public class LinkedListQueue  {

    final LinkedList l=new LinkedList();

    /*flag to determine the state of the Queue*/
    boolean closed=false;

    /*lock object for synchronization*/
    final Object mutex=new Object();

    /*the number of end markers that have been added*/
    int num_markers=0;


    /**
     * if the queue closes during the runtime
     * an endMarker object is added to the end of the queue to indicate that
     * the queue will close automatically when the end marker is encountered
     * This allows for a "soft" close.
     *
     * @see LinkedListQueue#close
     */
    private static final Object endMarker=new Object();

    protected static final GemFireTracer log=GemFireTracer.getLog(LinkedListQueue.class);


    /**
     * creates an empty queue
     */
    public LinkedListQueue() {
    }


    /**
     * returns true if the Queue has been closed
     * however, this method will return false if the queue has been closed
     * using the close(true) method and the last element has yet not been received.
     *
     * @return true if the queue has been closed
     */
    public boolean closed() {
        return closed;
    }


    /**
     * adds an object to the tail of this queue
     * If the queue has been closed with close(true) no exception will be
     * thrown if the queue has not been flushed yet.
     *
     * @param obj - the object to be added to the queue
     * @throws QueueClosedException if closed() returns true
     */
    public void add(Object obj) throws QueueClosedException {
        if(closed)
            throw new QueueClosedException();
        if(this.num_markers > 0)
            throw new QueueClosedException("LinkedListQueue.add(): queue has been closed. You can not add more elements. " +
                                           "Waiting for removal of remaining elements.");

        /*lock the queue from other threads*/
        synchronized(mutex) {
            l.add(obj);

            /*wake up all the threads that are waiting for the lock to be released*/
            mutex.notifyAll();
        }
    }


    /**
     * Adds a new object to the head of the queue
     * basically (obj.equals(LinkedListQueue.remove(LinkedListQueue.add(obj)))) returns true
     * If the queue has been closed with close(true) no exception will be
     * thrown if the queue has not been flushed yet.
     *
     * @param obj - the object to be added to the queue
     * @throws QueueClosedException if closed() returns true
     */
    public void addAtHead(Object obj) throws QueueClosedException {
        if(closed)
            throw new QueueClosedException();
        if(this.num_markers > 0)
            throw new QueueClosedException("LinkedListQueue.addAtHead(): queue has been closed. You can not add more elements. " +
                                           "Waiting for removal of remaining elements.");

        /*lock the queue from other threads*/
        synchronized(mutex) {
            l.addFirst(obj);

            /*wake up all the threads that are waiting for the lock to be released*/
            mutex.notifyAll();
        }
    }


    /**
     * Removes 1 element from head or <B>blocks</B>
     * until next element has been added
     *
     * @return the first element to be taken of the queue
     */
    public Object remove() throws QueueClosedException {
        Object retval=null;

        /*lock the queue*/
        synchronized(mutex) {
            /*wait as long as the queue is empty*/
            while(l.size() == 0) {
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

            /*remove the head from the queue*/
            try {
                retval=l.removeFirst();
                if(l.size() == 1 && l.getFirst().equals(endMarker))
                    closed=true;
            }
            catch(NoSuchElementException ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.LinkedListQueue_RETVAL__NULL_SIZE_0, l.size());
                return null;
            }

            // we ran into an Endmarker, which means that the queue was closed before
            // through close(true)
            if(retval == endMarker) {
                close(false); // mark queue as closed
                throw new QueueClosedException();
            }
        }

        /*return the object, should be never null*/
        return retval;
    }


    /**
     * Removes 1 element from the head.
     * If the queue is empty the operation will wait for timeout ms.
     * if no object is added during the timeout time, a Timout exception is thrown
     *
     * @param timeout - the number of milli seconds this operation will wait before it times out
     * @return the first object in the queue
     */
    public Object remove(long timeout) throws QueueClosedException, TimeoutException {
        Object retval=null;

        /*lock the queue*/
        synchronized(mutex) {
            /*if the queue size is zero, we want to wait until a new object is added*/
            if(l.size() == 0) {
                if(closed)
                    throw new QueueClosedException();
                try {
                    /*release the mutex lock and wait no more than timeout ms*/
                    mutex.wait(timeout);
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
            try {
                retval=l.removeFirst();
                if(l.size() == 1 && l.getFirst().equals(endMarker))
                    closed=true;
            }
            catch(NoSuchElementException ex) {
                /*null result means we timed out*/
                throw new TimeoutException();
            }
	    
            /*if we reached an end marker we are going to close the queue*/
            if(retval == endMarker) {
                close(false);
                throw new QueueClosedException();
            }
            /*at this point we actually did receive a value from the queue, return it*/
            return retval;
        }
    }


    /**
     * removes a specific object from the queue.
     * the object is matched up using the Object.equals method.
     *
     * @param obj the actual object to be removed from the queue
     */
    public void removeElement(Object obj) throws QueueClosedException {
        boolean removed;

        if(obj == null) return;
	
        /*lock the queue*/
        synchronized(mutex) {
            removed=l.remove(obj);
            if(!removed)
                if(log.isWarnEnabled()) log.warn("element " + obj + " was not found in the queue");
        }
    }


    /**
     * returns the first object on the queue, without removing it.
     * If the queue is empty this object blocks until the first queue object has
     * been added
     *
     * @return the first object on the queue
     */
    public Object peek() throws QueueClosedException {
        Object retval=null;

        synchronized(mutex) {
            while(l.size() == 0) {
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

            try {
                retval=l.getFirst();
            }
            catch(NoSuchElementException ex) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.LinkedListQueue_RETVAL__NULL_SIZE_0, l.size());
                return null;
            }
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
     *
     * @param timeout how long in milli seconds will this operation wait for an object to be added to the queue
     *                before it times out
     * @return the first object on the queue
     */

    public Object peek(long timeout) throws QueueClosedException, TimeoutException {
        Object retval=null;

        synchronized(mutex) {
            if(l.size() == 0) {
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


            try {
                retval=l.getFirst();
            }
            catch(NoSuchElementException ex) {
                /*null result means we timed out*/
                throw new TimeoutException();
            }

            if(retval == endMarker) {
                close(false);
                throw new QueueClosedException();
            }
            return retval;
        }
    }


    /**
     * Marks the queues as closed. When an <code>add</code> or <code>remove</code> operation is
     * attempted on a closed queue, an exception is thrown.
     *
     * @param flush_entries When true, a end-of-entries marker is added to the end of the queue.
     *                      Entries may be added and removed, but when the end-of-entries marker
     *                      is encountered, the queue is marked as closed. This allows to flush
     *                      pending messages before closing the queue.
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

        synchronized(mutex) {
            closed=true;
            try {
                mutex.notifyAll();
            }
            catch(Exception e) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.LinkedListQueue_EXCEPTION_0, e);
            }
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

        synchronized(mutex) {
            l.clear();
            closed=false;
        }
    }


    /**
     * returns the number of objects that are currently in the queue
     */
    public int size() {
        return l.size() - num_markers;
    }

    /**
     * prints the size of the queue
     */
    @Override // GemStoneAddition
    public String toString() {
        return "LinkedListQueue (" + size() + ") messages [closed=" + closed + ']';
    }


    /**
     * returns a vector with all the objects currently in the queue
     */
    public Vector getContents() {
        Vector retval=new Vector();

        synchronized(mutex) {
            for(Iterator it=l.iterator(); it.hasNext();) {
                retval.addElement(it.next());
            }
        }
        return retval;
    }


}
