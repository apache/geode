/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.distributed;

import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.locks.DLockService;

/**
 * <p>
 * A named instance of DistributedLockService defines a space for locking arbitrary names across the
 * distributed system defined by a specified distribution manager. Any number of
 * DistributedLockService instances can be created with different service names. For all processes
 * in the distributed system that have created an instance of DistributedLockService with the same
 * name, no more than one thread is permitted to own the lock on a given name in that instance at
 * any point in time. Additionally, a thread can lock the entire service, preventing any other
 * threads in the system from locking the service or any names in the service.
 * </p>
 */
public abstract class DistributedLockService {

  /**
   * Create a DistributedLockService with the given serviceName for the given DistributedSystem.
   * This DistributedLockService will continue to manage locks until <code>{@link #destroy}</code>
   * is called, or <code>ds</code> is disconnected, at which point any locks that were held by this
   * instance are released.
   *
   * @param serviceName the name of the DistributedLockService to create.
   *
   * @param ds the <code>DistributedSystem</code> for the new service instance to use for
   *        distributed lock messaging.
   *
   * @throws IllegalArgumentException if serviceName is an illegal name or this process has already
   *         created a DistributedLockService with the given <code>serviceName</code>.
   *
   * @throws IllegalStateException if this process is in the middle of disconnecting from the
   *         <code>DistributedSystem</code>
   */
  public static DistributedLockService create(String serviceName, DistributedSystem ds)
      throws IllegalArgumentException {
    DLockService.validateServiceName(serviceName);
    return DLockService.create(serviceName, (InternalDistributedSystem) ds, true, false);
  }

  /**
   * Look up and return the DistributedLockService with the given name, if it has been created in
   * this VM. If it has not been created, return null.
   *
   * @param serviceName the name of the DistributedLockService to look up
   *
   * @return the DistributedLockService with the given name, or null if it hasn't been created in
   *         this VM.
   */
  public static DistributedLockService getServiceNamed(String serviceName) {
    return DLockService.getServiceNamed(serviceName);
  }

  /**
   * Destroy a previously created DistributedLockService with the given <code>serviceName</code>.
   * Any locks currently held in this DistributedLockService by this process are released. Attempts
   * to access a destroyed lock service will result in a {@link LockServiceDestroyedException} being
   * thrown.
   *
   * @param serviceName the name of the instance to destroy, previously supplied in the
   *        <code>create(String, DistributedSystem)</code> invocation.
   *
   * @throws IllegalArgumentException if this process hasn't created a DistributedLockService with
   *         the given <code>serviceName</code> and <code>dm</code>.
   */
  public static void destroy(String serviceName) throws IllegalArgumentException {
    DLockService.destroyServiceNamed(serviceName);
  }

  /**
   * Public instance creation is prohibited - use {@link #create(String, DistributedSystem)}
   */
  protected DistributedLockService() {}

  /**
   * <p>
   * Attempts to acquire a lock named <code>name</code>. Returns <code>true</code> as soon as the
   * lock is acquired. If the lock is currently held by another thread in this or any other process
   * in the distributed system, or another thread in the system has locked the entire service, this
   * method keeps trying to acquire the lock for up to <code>waitTimeMillis</code> before giving up
   * and returning <code>false</code>. If the lock is acquired, it is held until
   * <code>unlock(Object name)</code> is invoked, or until <code>leaseTimeMillis</code> milliseconds
   * have passed since the lock was granted - whichever comes first.
   * </p>
   *
   * <p>
   * Locks are reentrant. If a thread invokes this method n times on the same instance, specifying
   * the same <code>name</code>, without an intervening release or lease expiration expiration on
   * the lock, the thread must invoke <code>unlock(name)</code> the same number of times before the
   * lock is released (unless the lease expires). When this method is invoked for a lock that is
   * already acquired, the lease time will be set to the maximum of the remaining least time from
   * the previous invocation, or <code>leaseTimeMillis</code>
   * </p>
   *
   * @param name the name of the lock to acquire in this service. This object must conform to the
   *        general contract of <code>equals(Object)</code> and <code>hashCode()</code> as described
   *        in {@link java.lang.Object#hashCode()}.
   *
   * @param waitTimeMillis the number of milliseconds to try to acquire the lock before giving up
   *        and returning false. A value of -1 causes this method to block until the lock is
   *        acquired. A value of 0 causes this method to return false without waiting for the lock
   *        if the lock is held by another member or thread.
   *
   * @param leaseTimeMillis the number of milliseconds to hold the lock after granting it, before
   *        automatically releasing it if it hasn't already been released by invoking
   *        {@link #unlock(Object)}. If <code>leaseTimeMillis</code> is -1, hold the lock until
   *        explicitly unlocked.
   *
   * @return true if the lock was acquired, false if the timeout <code>waitTimeMillis</code> passed
   *         without acquiring the lock.
   *
   * @throws LockServiceDestroyedException if this lock service has been destroyed
   */
  public abstract boolean lock(Object name, long waitTimeMillis, long leaseTimeMillis);

  /**
   * <p>
   * Attempts to acquire a lock named <code>name</code>. Returns <code>true</code> as soon as the
   * lock is acquired. If the lock is currently held by another thread in this or any other process
   * in the distributed system, or another thread in the system has locked the entire service, this
   * method keeps trying to acquire the lock for up to <code>waitTimeMillis</code> before giving up
   * and returning <code>false</code>. If the lock is acquired, it is held until
   * <code>unlock(Object name)</code> is invoked, or until <code>leaseTimeMillis</code> milliseconds
   * have passed since the lock was granted - whichever comes first.
   * </p>
   *
   * <p>
   * Locks are reentrant. If a thread invokes this method n times on the same instance, specifying
   * the same <code>name</code>, without an intervening release or lease expiration expiration on
   * the lock, the thread must invoke <code>unlock(name)</code> the same number of times before the
   * lock is released (unless the lease expires). When this method is invoked for a lock that is
   * already acquired, the lease time will be set to the maximum of the remaining least time from
   * the previous invocation, or <code>leaseTimeMillis</code>
   * </p>
   *
   * @param name the name of the lock to acquire in this service. This object must conform to the
   *        general contract of <code>equals(Object)</code> and <code>hashCode()</code> as described
   *        in {@link java.lang.Object#hashCode()}.
   *
   * @param waitTimeMillis the number of milliseconds to try to acquire the lock before giving up
   *        and returning false. A value of -1 causes this method to block until the lock is
   *        acquired.
   *
   * @param leaseTimeMillis the number of milliseconds to hold the lock after granting it, before
   *        automatically releasing it if it hasn't already been released by invoking
   *        {@link #unlock(Object)}. If <code>leaseTimeMillis</code> is -1, hold the lock until
   *        explicitly unlocked.
   *
   * @return true if the lock was acquired, false if the timeout <code>waitTimeMillis</code> passed
   *         without acquiring the lock.
   *
   * @throws InterruptedException if the thread is interrupted before or during this method.
   *
   * @throws LockServiceDestroyedException if this lock service has been destroyed
   *
   * @deprecated as of GemFire 5.1, use {@link #lock(Object, long, long)} with waitTimeMillis
   *             instead
   */
  @Deprecated
  public abstract boolean lockInterruptibly(Object name, long waitTimeMillis, long leaseTimeMillis)
      throws InterruptedException;

  /**
   * Release the lock previously granted for the given <code>name</code>.
   *
   * @param name the object to unlock in this service.
   *
   * @throws LockNotHeldException if the current thread is not the owner of this lock
   *
   * @throws LeaseExpiredException if the current thread was the owner of this lock, but it's lease
   *         has expired.
   *
   * @throws LockServiceDestroyedException if the service has been destroyed
   */
  public abstract void unlock(Object name) throws LeaseExpiredException;

  /**
   * Determine whether the current thread owns the lock on the given object.
   *
   * @return true if the current thread owns the lock for <code>name</code>.
   *
   * @throws LockServiceDestroyedException if this service has been destroyed
   */
  public abstract boolean isHeldByCurrentThread(Object name);

  /**
   * Suspend granting of locks in this service. When locking has been suspended, no other thread in
   * the distributed system will be granted a lock for any new or existing name in that service
   * until locking is resumed by the thread that suspended it. Only one thread at a time in a
   * distributed system is permitted suspend locking on a given DistributedLockService instance.
   * This method blocks until lock suspension can be granted to the current thread, and all
   * outstanding locks on names in this service held by other threads in the distributed system have
   * been released, or until <code>waitTimeMillis</code> milliseconds have passed without
   * successfully granting suspension.
   *
   * @param waitTimeMillis the number of milliseconds to try to acquire suspension before giving up
   *        and returning false. A value of -1 causes this method to block until suspension is
   *        granted.
   *
   * @return true if suspension was granted, false if the timeout <code>waitTimeMillis</code> passed
   *         before it could be granted.
   *
   * @throws IllegalStateException if the current thread already has suspended locking on this
   *         instance.
   *
   * @throws InterruptedException if the current thread is interrupted.
   *
   * @throws LockServiceDestroyedException if the service has been destroyed
   *
   * @deprecated as of GemFire 5.1, use {@link #suspendLocking(long)} with waitTimeMillis instead
   */
  @Deprecated
  public abstract boolean suspendLockingInterruptibly(long waitTimeMillis)
      throws InterruptedException;

  /**
   * Suspend granting of locks in this service. When locking has been suspended, no other thread in
   * the distributed system will be granted a lock for any new or existing name in that service
   * until locking is resumed by the thread that suspended it. Only one thread at a time in a
   * distributed system is permitted suspend locking on a given DistributedLockService instance.
   * This method blocks until lock suspension can be granted to the current thread, and all
   * outstanding locks on names in this service held by other threads in the distributed system have
   * been released, or until <code>waitTimeMillis</code> milliseconds have passed without
   * successfully granting suspension.
   *
   * @param waitTimeMillis the number of milliseconds to try to acquire suspension before giving up
   *        and returning false. A value of -1 causes this method to block until suspension is
   *        granted. A value of 0 causes this method to return false without waiting for the lock if
   *        the lock is held by another member or thread.
   *
   * @return true if suspension was granted, false if the timeout <code>waitTimeMillis</code> passed
   *         before it could be granted.
   *
   * @throws IllegalStateException if the current thread already has suspended locking on this
   *         instance
   *
   * @throws LockServiceDestroyedException if the service has been destroyed
   */
  public abstract boolean suspendLocking(long waitTimeMillis);

  /**
   * Allow locking to resume in this DistributedLockService instance.
   *
   * @throws IllegalStateException if the current thread didn't previously suspend locking
   *
   * @throws LockServiceDestroyedException if the service has been destroyed
   */
  public abstract void resumeLocking();


  /**
   * Determine whether the current thread has suspended locking in this DistributedLockService.
   *
   * @return true if locking is suspended by the current thread.
   *
   * @throws LockServiceDestroyedException if this service has been destroyed
   */
  public abstract boolean isLockingSuspendedByCurrentThread();

  /**
   * Free internal resources associated with the given <code>name</code>. This may reduce this VM's
   * memory use, but may also prohibit performance optimizations if <code>name</code> is
   * subsequently locked in this VM.
   *
   * @throws LockServiceDestroyedException if this service has been destroyed
   */
  public abstract void freeResources(Object name);

  /**
   * Specifies this member to become the grantor for this lock service. The grantor will be the lock
   * authority which is responsible for handling all lock requests for this service. Other members
   * will request locks from this member. Locking for this member is optimized as it will not
   * require messaging to acquire a given lock.
   * <p>
   * Calls to this method will block until grantor authority has been transferred to this member.
   * <p>
   * If another member calls <code>becomeLockGrantor</code> after this member, that member will
   * transfer grantor authority from this member to itself.
   * <p>
   * This operation should not be invoked repeatedly in an application. It is possible to create a
   * lock service and have two or more members endlessly calling becomeLockGrantor to transfer
   * grantorship back and forth.
   *
   * @throws LockServiceDestroyedException if this service has been destroyed
   */
  public abstract void becomeLockGrantor();

  /**
   * Specifies that this member should become the grantor for the named locking service.
   *
   * @param serviceName the name of the locking service
   *
   * @throws IllegalArgumentException if <code>serviceName<code> does not refer to any registered
   *         locking service in this process
   *
   * @see org.apache.geode.distributed.DistributedLockService#becomeLockGrantor()
   */
  public static void becomeLockGrantor(String serviceName) throws IllegalArgumentException {
    DLockService.becomeLockGrantor(serviceName);
  }

  /**
   * Returns true if this member is currently the lock authority responsible for granting locks for
   * this service. This can be explicitly requested by calling
   * {@link org.apache.geode.distributed.DistributedLockService#becomeLockGrantor()}. If no member
   * has explicitly requested grantor authority, then one member participating in the service will
   * be implicitly selected. In either case, this method returns true if the calling member is the
   * grantor.
   *
   * @return true if this member is the grantor for this service
   *
   * @throws LockServiceDestroyedException if lock service has been destroyed
   */
  public abstract boolean isLockGrantor();

  /**
   * Returns true if this member is the grantor for the named service.
   *
   * @param serviceName the name of the locking service
   *
   * @return true if this member is the grantor for this service
   *
   * @throws IllegalArgumentException if <code>serviceName<code> does not refer to any registered
   *         locking service in this process
   *
   * @see org.apache.geode.distributed.DistributedLockService#isLockGrantor()
   */
  public static boolean isLockGrantor(String serviceName) throws IllegalArgumentException {
    return DLockService.isLockGrantor(serviceName);
  }

}
