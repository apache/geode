/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

import com.gemstone.org.jgroups.TimeoutException;


/**
 * Class that checks on a condition and - if condition doesn't match the expected result - waits until the result
 * matches the expected result, or a timeout occurs. First version used WaitableBoolean from util.concurrent, but
 * that class would not allow for timeouts.
 * @author Bela Ban
 * @version $Id: CondVar.java,v 1.3 2004/12/31 14:10:40 belaban Exp $
 */
public class CondVar {
    Object cond;
    final String name;
    final Object lock;

    public CondVar(String name, Object cond) {
        this.name=name;
        this.cond=cond;
        lock=this;
    }

    public CondVar(String name, Object cond, Object lock) {
        this.name=name;
        this.cond=cond;
        this.lock=lock;
    }

    public Object get() {
        synchronized(lock) {
            return cond;
        }
    }

    /** Sets the result */
    public void set(Object result) {
        synchronized(lock) {
            cond=result;
            lock.notifyAll();
        }
    }

    public Object getLock() {
        return lock;
    }


    /**
     * Waits until the condition matches the expected result. Returns immediately if they match, otherwise waits
     * for timeout milliseconds or until the results match.
     * @param result The result, needs to match the condition (using equals()).
     * @param timeout Number of milliseconds to wait. A value of <= 0 means to wait forever
     * @throws TimeoutException Thrown if the result still doesn't match the condition after timeout
     * milliseconds have elapsed
     */
    public void waitUntilWithTimeout(Object result, long timeout) throws TimeoutException {
        _waitUntilWithTimeout(result, timeout);
    }

    /**
     * Waits until the condition matches the expected result. Returns immediately if they match, otherwise waits
     * for timeout milliseconds or until the results match. This method doesn't throw a TimeoutException
     * @param result The result, needs to match the condition (using equals()).
     * @param timeout Number of milliseconds to wait. A value of <= 0 means to wait forever
     */
    public void waitUntil(Object result, long timeout) {
        try {
            _waitUntilWithTimeout(result, timeout);
        }
        catch(TimeoutException e) {

        }
    }

    public void waitUntil(Object result) {
        try {waitUntilWithTimeout(result, 0);} catch(TimeoutException e) {}
    }



    private void _waitUntilWithTimeout(Object result, long timeout) throws TimeoutException {
        long    time_to_wait=timeout, start;
        boolean timeout_occurred=false;
        synchronized(lock) {
            if(result == null && cond == null) return;

            start=System.currentTimeMillis();
            while(match(result, cond) == false) {
                if(timeout <= 0) {
                    doWait();
                }
                else {
                    if(time_to_wait <= 0) {
                        timeout_occurred=true;
                        break; // terminate the while loop
                    }
                    else {
                        doWait(time_to_wait);
                        time_to_wait=timeout - (System.currentTimeMillis() - start);
                    }
                }
            }
            if(timeout_occurred)
                throw new TimeoutException();
        }
    }




    void doWait() {
        try {lock.wait();} catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
        }
    }

    void doWait(long timeout) {
        try {lock.wait(timeout);} catch(InterruptedException e) {
          Thread.currentThread().interrupt(); // GemStoneAddition
        }
    }

    private boolean match(Object o1, Object o2) {
        if(o1 != null)
            return o1.equals(o2);
        else
            return o2.equals(o1);
    }

    @Override
    public String toString() {
        return name + "=" + cond;
    }
}
