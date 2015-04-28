/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.util;

/**
 * Enables safely locking and unlocking a shared resource, without blocking the calling threads. Blocking is only done
 * on the 'passThrough' method.
 *
 * @author yaronr / Dmitry Gershkovich
 * @version 1.0
 */
public final class ReentrantLatch  {

    boolean locked;

    /**
     * Create a new unlocked latch.
     */
    public ReentrantLatch() {
        this(false);
    }

    /**
     * Create a reentrant latch
     *
     * @param locked is the latch to be created locked or not
     */
    public ReentrantLatch(boolean locked) {
        this.locked = locked;
    }

    /**
     * Lock the latch. If it is already locked, this method will have no side effects. This method will not block.
     */
    public synchronized void lock() {
        if (!locked) {
            locked = true;
        }
    }

    /**
     * Unlock the latch. If it is already unlocked, this method will have no side effects. This method will not block.
     */
    public synchronized void unlock() {
        if (locked) {
            locked = false;
            notifyAll();
        }
    }

    /**
     * Pass through only when the latch becomes unlocked. If the latch is locked, wait until someone unlocks it. Does
     * not lock the latch.
     *
     * @throws InterruptedException
     */
    public synchronized void passThrough() throws InterruptedException {
      if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
        while (locked) {
            wait();
        }
    }
}
