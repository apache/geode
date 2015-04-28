/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.blocks;

import com.gemstone.org.jgroups.ChannelException;

/**
 * <code>LockManager</code> represents generic lock manager that allows
 * obtaining and releasing locks on objects.
 * 
 * @author Roman Rokytskyy (rrokytskyy@acm.org)
 * @author Robert Schaffar-Taurok (robert@fusion.at)
 * @version $Id: LockManager.java,v 1.2 2005/06/08 15:56:54 publicnmi Exp $
 */
public interface LockManager {
    
    /**
     * Obtain lock on <code>obj</code> for specified <code>owner</code>.
     * Implementation should try to obtain lock few times within the
     * specified timeout.
     *
     * @param obj obj to lock, usually not full object but object's ID.
     * @param owner object identifying entity that will own the lock.
     * @param timeout maximum time that we grant to obtain a lock.
     *
     * @throws LockNotGrantedException if lock is not granted within
     * specified period.
     *
     * @throws ClassCastException if <code>obj</code> and/or
     * <code>owner</code> is not of type that implementation expects to get
     * (for example, when distributed lock manager obtains non-serializable
     * <code>obj</code> or <code>owner</code>).
     * 
     * @throws ChannelException if something bad happened to communication
     * channel.
     */
    void lock(Object obj, Object owner, int timeout)
    throws LockNotGrantedException, ClassCastException, ChannelException;

    /**
     * Release lock on <code>obj</code> owned by specified <code>owner</code>.
     *
     * since 2.2.9 this method is only a wrapper for 
     * unlock(Object lockId, Object owner, boolean releaseMultiLocked).
     * Use that with releaseMultiLocked set to true if you want to be able to
     * release multiple locked locks (for example after a merge)
     * 
     * @param obj obj to lock, usually not full object but object's ID.
     * @param owner object identifying entity that will own the lock.
     *
     * @throws ClassCastException if <code>obj</code> and/or
     * <code>owner</code> is not of type that implementation expects to get
     * (for example, when distributed lock manager obtains non-serializable
     * <code>obj</code> or <code>owner</code>).
     * 
     * @throws ChannelException if something bad happened to communication
     * channel.
     */
    void unlock(Object obj, Object owner)
    throws LockNotReleasedException, ClassCastException, ChannelException;

    /**
     * Release lock on <code>obj</code> owned by specified <code>owner</code>.
     *
     * @param obj obj to lock, usually not full object but object's ID.
     * @param owner object identifying entity that will own the lock.
     * @param releaseMultiLocked force unlocking of the lock if the local
     * lockManager owns the lock even if another lockManager owns the same lock
     *
     * @throws ClassCastException if <code>obj</code> and/or
     * <code>owner</code> is not of type that implementation expects to get
     * (for example, when distributed lock manager obtains non-serializable
     * <code>obj</code> or <code>owner</code>).
     * 
     * @throws ChannelException if something bad happened to communication
     * channel.
     * 
     * @throws LockMultiLockedException if the lock was unlocked, but another
     * node already held the lock
     */
    void unlock(Object obj, Object owner, boolean releaseMultiLocked)
    throws LockNotReleasedException, ClassCastException, ChannelException, LockMultiLockedException;

    
}
