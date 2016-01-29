/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.jta;

/**
 * <p>
 * GlobalTransaction is the JTA concept of a Global Transaction.
 * </p>
 * 
 * @author Mitul Bid
 * 
 * @since 4.0 
 */
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import java.util.*;
import javax.transaction.xa.*;
import javax.transaction.*;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

public class GlobalTransaction  {

  public static boolean DISABLE_TRANSACTION_TIMEOUT_SETTING = false;
  /**
   * GTid is a byte array identifying every instance of a global transaction
   * uniquely
   */
  private final byte[] GTid;
  /**
   * An instance of the XidImpl class which implements Xid
   */
  private final Xid xid;
  /**
   * Status represents the state the Global Transaction is in
   */
  private int status = Status.STATUS_UNKNOWN;
  /**
   * A set of XAResources associated with the Global Transaction
   */
  private Map resourceMap = Collections.synchronizedMap(new HashMap());
  /**
   * List of local Transactions participating in a Global Transaction.
   */
  private List transactions = Collections.synchronizedList(new ArrayList());
  /**
   * A counter to uniquely generate the GTid
   */
  private static long mCounter = 1;
  /**
   * A timer Task for Transaction TimeOut
   */
  private boolean timedOut = false;
  /**
   * expirationTime for the Transaction
   */
  private volatile long expirationTime;

  /*
   * to enable VERBOSE = true pass System parameter jta.VERBOSE = true while
   * running the test.
   */
  private static boolean VERBOSE = Boolean.getBoolean("jta.VERBOSE");

  /**
   * Construct a new Global Transaction. Generates the GTid and also the xid
   */
  public GlobalTransaction() throws SystemException {
    try {
      GTid = generateGTid();
      xid = XidImpl.createXid(GTid);
    }
    catch (Exception e) {
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (writer.severeEnabled()) writer.severe(LocalizedStrings.GlobalTransaction_GLOBALTRANSACTION_CONSTRUCTOR_ERROR_WHILE_TRYING_TO_CREATE_XID_DUE_TO_0, e, e);
      String exception = LocalizedStrings.GlobalTransaction_GLOBALTRANSACTION_CONSTRUCTOR_ERROR_WHILE_TRYING_TO_CREATE_XID_DUE_TO_0.toLocalizedString(new Object[] {e});
      throw new SystemException(exception);
    }
  }

  /**
   * Add a transaction to the list of transactions participating in this global
   * Transaction The list of transactions is being maintained so that we can
   * remove the local transaction to global transaction entries from the map
   * being maintained by the Transaction Manager
   * 
   * @param txn Transaction instance which is participating in this Global
   *          Transaction
   */
  public void addTransaction(Transaction txn) throws SystemException {
    if (txn == null) {
      String exception = LocalizedStrings.GlobalTransaction_GLOBALTRANSACTION_ADDTRANSACTION_CANNOT_ADD_A_NULL_TRANSACTION.toLocalizedString();
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (VERBOSE) writer.fine(exception);
      throw new SystemException(exception);
    }
    transactions.add(txn);
  }

  /**
   * Delists the XAResources associated with the Global Transaction and
   * Completes the Global transaction associated with the current thread. If any
   * exception is encountered, rollback is called on the current transaction.
   * 
   * Concurrency: Some paths invoke this method after taking a lock on "this" while 
   * other paths invoke this method without taking a lock on "this". Since both types of path do
   * act on the resourceMap collection, it is being protected by a lock on resourceMap too.
   * 
   * @throws RollbackException -
   *           Thrown to indicate that the transaction has been rolled back
   *           rather than committed.
   * @throws HeuristicMixedException -
   *           Thrown to indicate that a heuristic decision was made and that
   *           some relevant updates have been committed while others have been
   *           rolled back.
   * @throws HeuristicRollbackException -
   *           Thrown to indicate that a heuristic decision was made and that
   *           all relevant updates have been rolled back.
   * @throws java.lang.SecurityException -
   *           Thrown to indicate that the thread is not allowed to commit the
   *           transaction.
   * @throws java.lang.IllegalStateException -
   *           Thrown if the current thread is not associated with a
   *           transaction.
   * @throws SystemException -
   *           Thrown if the transaction manager encounters an unexpected error
   *           condition.
   * 
   * @see javax.transaction.TransactionManager#commit()
   */
  //Asif : Changed the return type to int indicating the nature of Exception
  // encountered during commit
  public void commit() throws RollbackException, HeuristicMixedException,
      HeuristicRollbackException, SecurityException, SystemException
  {
    LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
    try {
      XAResource xar = null;
      XAResource xar1 = null;
      int loop = 0;
      Boolean isActive = Boolean.FALSE;
      synchronized (this.resourceMap) {
        Map.Entry entry;
        Iterator iterator = resourceMap.entrySet().iterator();
        while (iterator.hasNext()) {
          try {
            entry = (Map.Entry)iterator.next();
            xar = (XAResource)entry.getKey();
            isActive = (Boolean)entry.getValue();
            if (loop == 0)
              xar1 = xar;
            loop++;
            if (isActive.booleanValue()) {
              // delistResource(xar, XAResource.TMSUCCESS);
              xar.end(xid, XAResource.TMSUCCESS);
              entry.setValue(Boolean.FALSE);
            }
          }
          catch (Exception e) {
            if (VERBOSE)
              writer.info(
                LocalizedStrings.ONE_ARG,  
                "GlobalTransaction::commit:Exception in delisting XAResource",
                e);
          }
        }
      }
      if (xar1 != null)
        xar1.commit(xid, true);
      status = Status.STATUS_COMMITTED;
      if (VERBOSE)
        writer
            .fine("GlobalTransaction::commit:Transaction committed successfully");
    }
    catch (Exception e) {
      status = Status.STATUS_ROLLING_BACK;
      try {
        rollback();
      }
      catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error.  We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        // we will throw an error later, make sure that the synchronizations rollback
        status = Status.STATUS_ROLLEDBACK;
        String exception = LocalizedStrings.GlobalTransaction_GLOBALTRANSACTION_COMMIT_ERROR_IN_COMMITTING_BUT_TRANSACTION_COULD_NOT_BE_ROLLED_BACK_DUE_TO_EXCEPTION_0.toLocalizedString(t); 
        if (VERBOSE)
          writer.fine(exception, t);
        SystemException sysEx = new SystemException(exception);
        sysEx.initCause(t);
        throw sysEx;
      }
      String exception = LocalizedStrings.GlobalTransaction_GLOBALTRANSACTION_COMMIT_ERROR_IN_COMMITTING_THE_TRANSACTION_TRANSACTION_ROLLED_BACK_EXCEPTION_0_1.toLocalizedString(new Object[] {e, 
          " " + (e instanceof XAException ? ("Error Code =" + ((XAException)e).errorCode)
              : "")});
      if (VERBOSE)
        writer.fine(exception, e);
      RollbackException rbEx = new RollbackException(exception);
      rbEx.initCause(e);
      throw rbEx;
    }
    finally {
      //Map globalTransactions = tm.getGlobalTransactionMap();
      TransactionManagerImpl.getTransactionManager().cleanGlobalTransactionMap(
          transactions);
      //Asif : Clear the list of transactions
      transactions.clear();
    }
  }

  /**
   * Delists the XAResources associated with the Global Transaction and Roll
   * back the transaction associated with the current thread.
   * 
   * Concurrency: Some paths invoke this method after taking a lock on "this" while 
   * other paths invoke this method without taking a lock on "this". Since both types of path do
   * act on the resourceMap collection, it is being protected by a lock on resourceMap too.
   * 
   * @throws java.lang.SecurityException -
   *           Thrown to indicate that the thread is not allowed to roll back
   *           the transaction.
   * @throws java.lang.IllegalStateException -
   *           Thrown if the current thread is not associated with a
   *           transaction.
   * @throws SystemException -
   *           Thrown if the transaction manager encounters an unexpected error
   *           condition.
   * 
   * @see javax.transaction.TransactionManager#rollback()
   */
  public void rollback() throws IllegalStateException, SystemException
  {
    LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
    try {
      XAResource xar = null;
      XAResource xar1 = null;
      int loop = 0;
      synchronized (this.resourceMap) {
        Iterator iterator = resourceMap.entrySet().iterator();
        Boolean isActive = Boolean.FALSE;
        Map.Entry entry;
        while (iterator.hasNext()) {
          try {
            entry = (Map.Entry)iterator.next();
            xar = (XAResource)entry.getKey();
            isActive = (Boolean)entry.getValue();
            if (loop == 0) {
              xar1 = xar;
            }
            loop++;
            if (isActive.booleanValue()) {
              // delistResource(xar, XAResource.TMSUCCESS);
              xar.end(xid, XAResource.TMSUCCESS);
              entry.setValue(Boolean.FALSE);
            }
          }
          catch (Exception e) {
            if (VERBOSE)
              writer.info(
                LocalizedStrings.ONE_ARG,  
                "GlobalTransaction::rollback:Exception in delisting XAResource",
                e);
          }
        }
      }
      if (xar1 != null)
        xar1.rollback(xid);
      status = Status.STATUS_ROLLEDBACK;
      if (VERBOSE)
        writer.fine("Transaction rolled back successfully");
    }
    catch (Exception e) {
      // we will throw an error later, make sure that the synchronizations rollback
      status = Status.STATUS_ROLLEDBACK;
      String exception = LocalizedStrings.GlobalTransaction_GLOBALTRANSACTION_ROLLBACK_ROLLBACK_NOT_SUCCESSFUL_DUE_TO_EXCEPTION_0_1.toLocalizedString(new Object[] {e, " " + (e instanceof XAException ? ("Error Code =" + ((XAException)e).errorCode) : "")});
      if (VERBOSE)
        writer.fine(exception);
      SystemException sysEx = new SystemException(exception);
      sysEx.initCause(e);
      throw sysEx;
    }
    finally {
      // Map globalTransactions = tm.getGlobalTransactionMap();
      TransactionManagerImpl.getTransactionManager().cleanGlobalTransactionMap(
          transactions);
      //    Asif : Clear the list of transactions
      transactions.clear();
    }
  }

  /**
   * Mark the state of the Global Transaction so that it can be rolled back
   */
  public void setRollbackOnly() throws IllegalStateException, SystemException {
    setStatus(Status.STATUS_MARKED_ROLLBACK);
  }

  /**
   * Get the transaction state of the Global Transaction
   */
  public int getStatus() throws SystemException {
    return status;
  }

  /**
   * Enlist the specified XAResource with this transaction. Currently only one
   * Resource Manager is being supported. enlistResource checks if there is no
   * XAResource, then enlists the current XAResource. For subsequent
   * XAResources, it checks if is the same Resource Manager. If it is, then the
   * XAResources are addded, else an exception is thrown
   * 
   * Concurrency: The order of acquiring lock will be lock on "this" followed
   * by lock on resourceMap. It is possible that in some functions of this
   * class both the locks are not needed , but if the two are acquired then the realitive
   * order will always be"this" followed by resourceMap. 
   * 
   * @param xaRes XAResource to be enlisted
   * @return true, if resource was enlisted successfully, otherwise false.
   * @throws SystemException - Thrown if the transaction manager encounters an
   *           unexpected error condition.
   * @throws IllegalStateException - Thrown if the transaction in the target
   *           object is in the prepared state or the transaction is inactive.
   * @throws RollbackException - Thrown to indicate that the transaction has
   *           been marked for rollback only.
   * 
   * @see javax.transaction.Transaction#enlistResource(javax.transaction.xa.XAResource)
   */
  public boolean enlistResource(XAResource xaRes) throws RollbackException,
      IllegalStateException, SystemException {
    XAResource xar = null;
    try {
      synchronized (this) {
        if (status == Status.STATUS_MARKED_ROLLBACK) {
          String exception = "GlobalTransaction::enlistResource::Cannot enlist resource as the transaction has been marked for rollback";
          LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
          if (VERBOSE) writer.fine(exception);
          throw new RollbackException(exception);
        }
        else if (status != Status.STATUS_ACTIVE) {
          String exception = LocalizedStrings.GlobalTransaction_GLOBALTRANSACTION_ENLISTRESOURCE_CANNOT_ENLIST_A_RESOURCE_TO_A_TRANSACTION_WHICH_IS_NOT_ACTIVE.toLocalizedString(); 
          LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
          if (VERBOSE) writer.fine(exception);
          throw new IllegalStateException(exception);
        }
        if (resourceMap.isEmpty()) {
          xaRes.start(xid, XAResource.TMNOFLAGS);
          int delay = (int) ((expirationTime - System.currentTimeMillis()) / 1000);
          try {
            if(!DISABLE_TRANSACTION_TIMEOUT_SETTING) {
              xaRes.setTransactionTimeout(delay);
            }
          }
          catch (XAException xe) {
            String exception = LocalizedStrings.GlobalTransaction_GLOBALTRANSACTION_ENLISTRESOURCE_EXCEPTION_OCCURED_IN_TRYING_TO_SET_XARESOURCE_TIMEOUT_DUE_TO_0_ERROR_CODE_1.toLocalizedString(new Object[] {xe, Integer.valueOf(xe.errorCode)});
            LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
            if (VERBOSE) writer.fine(exception);
            throw new SystemException(exception);
          }
          resourceMap.put(xaRes, Boolean.TRUE);
        }
        else {
          synchronized (this.resourceMap) {
            Iterator iterator = resourceMap.keySet().iterator();
            xar = (XAResource) iterator.next();
          }
          if (!xar.isSameRM(xaRes)) {
            LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
            if (writer.severeEnabled()) writer.severe(
                LocalizedStrings.GlobalTransaction_GLOBALTRANSACTIONENLISTRESOURCEONLY_ONE_RESOUCE_MANAGER_SUPPORTED);
            throw new SystemException(LocalizedStrings.GlobalTransaction_GLOBALTRANSACTIONENLISTRESOURCEONLY_ONE_RESOUCE_MANAGER_SUPPORTED.toLocalizedString());
          }
          else {
            xaRes.start(xid, XAResource.TMJOIN);
            resourceMap.put(xaRes, Boolean.TRUE);
          }
        }
      }
    }
    catch (Exception e) {
      String addon = (e instanceof XAException ? ("Error Code =" + ((XAException) e).errorCode)
              : "");
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (VERBOSE) writer.fine(LocalizedStrings.GLOBALTRANSACTION__ENLISTRESOURCE__ERROR_WHILE_ENLISTING_XARESOURCE_0_1.toLocalizedString(new Object[] {e, addon}), e);
      SystemException sysEx = new SystemException(LocalizedStrings.GLOBALTRANSACTION__ENLISTRESOURCE__ERROR_WHILE_ENLISTING_XARESOURCE_0_1.toLocalizedString(new Object[] {e, addon}));
      sysEx.initCause(e);
      throw sysEx;
    }
    return true;
  }

  /**
   * Disassociate the XAResource specified from this transaction.
   * 
   * In the current implementation this call will never be made by the
   * application server. The delisting is happening at the time of
   * commit/rollback
   * 
   * @param xaRes XAResource to be delisted
   * @param flag One of the values of TMSUCCESS, TMSUSPEND, or TMFAIL.
   * @return true, if resource was delisted successfully, otherwise false.
   * @throws SystemException Thrown if the transaction manager encounters an
   *           unexpected error condition.
   * @throws IllegalStateException Thrown if the transaction in the target
   *           object is not active.
   * 
   * @see javax.transaction.Transaction#delistResource(javax.transaction.xa.XAResource,
   *      int)
   */
  public boolean delistResource(XAResource xaRes, int flag)
      throws IllegalStateException, SystemException {
    try {
      if (resourceMap.containsKey(xaRes)) {
        Boolean isActive = (Boolean) resourceMap.get(xaRes);
        if (isActive.booleanValue()) {
          xaRes.end(xid, flag);
          resourceMap.put(xaRes, Boolean.FALSE);
        }
      }
    }
    catch (Exception e) {
      String exception = LocalizedStrings.GlobalTransaction_ERROR_WHILE_DELISTING_XARESOURCE_0_1.toLocalizedString(new Object[] {e, " " + (e instanceof XAException ? ("Error Code =" + ((XAException) e).errorCode) : "")});
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (VERBOSE) writer.fine(exception, e);
      SystemException se = new SystemException(exception);
      se.initCause(e);
    }
    return true;
  }

  /**
   * Set the transaction state of the Global Transaction
   * 
   * @param new_status Status (int)
   */
  public void setStatus(int new_status) {
    status = new_status;
  }

  /**
   * suspends the current transaction by deactivating the XAResource (delist)    
   */
  public void suspend() throws SystemException
  {
    XAResource xar = null;
    synchronized (this.resourceMap) {
      Iterator iterator = resourceMap.entrySet().iterator();
      Map.Entry entry;
      Boolean isActive = Boolean.FALSE;
      while (iterator.hasNext()) {
        entry = (Map.Entry)iterator.next();
        xar = (XAResource)entry.getKey();
        isActive = (Boolean)entry.getValue();
        if (isActive.booleanValue())
          try {
            // delistResource(xar, XAResource.TMSUCCESS);
            xar.end(xid, XAResource.TMSUSPEND);
            entry.setValue(Boolean.FALSE);
          }
          catch (Exception e) {
            String exception = LocalizedStrings.GlobalTransaction_ERROR_WHILE_DELISTING_XARESOURCE_0_1.toLocalizedString(new Object[] {e, " " + (e instanceof XAException ? ("Error Code =" + ((XAException) e).errorCode) : "")});
            LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
            if (VERBOSE)
              writer.fine(exception);
            throw new SystemException(exception);
          }
        /*
         * catch (SystemException e) { String exception =
         * "GlobaTransaction::suspend not succesful due to " + e; LogWriterI18n
         * writer = TransactionUtils.getLogWriter(); if (VERBOSE)
         * writer.fine(exception); throw new SystemException(exception); }
         */
      }
    }
  }

  /**
   * resume the current transaction by activating all the XAResources associated
   * with the current transaction
   */
  public void resume() throws SystemException
  {
    XAResource xar = null;
    synchronized (this.resourceMap) {
      Iterator iterator = resourceMap.entrySet().iterator();
      Map.Entry entry;
      Boolean isActive = Boolean.FALSE;

      while (iterator.hasNext()) {
        entry = (Map.Entry)iterator.next();
        xar = (XAResource)entry.getKey();
        isActive = (Boolean)entry.getValue();
        if (!isActive.booleanValue())
          try {
            xar.start(xid, XAResource.TMRESUME);
            entry.setValue(Boolean.TRUE);
          }
          catch (Exception e) {
            String exception = LocalizedStrings.GlobalTransaction_GLOBATRANSACTION_RESUME_RESUME_NOT_SUCCESFUL_DUE_TO_0.toLocalizedString(e);
            LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
            if (VERBOSE)
              writer.fine(exception, e);
            throw new SystemException(exception);
          }
      }
    }
  }

  /**
   * String for current distributed system
   * 
   * @see #IdsForId
   * @guarded.By {@link #DmidMutex}
   */
  private static String DMid = null;
  
  /**
   * Distributed system for given string
   * @see #DMid
   * @guarded.By {@link #DmidMutex}
   */
  private static InternalDistributedSystem IdsForId = null;
  
  /**
   * Mutex controls update of {@link #DMid}
   * @see #DMid
   * @see #IdsForId
   */
  private static final Object DmidMutex = new Object();
  
  /**
   * Read current {@link #DMid} and return it
   * @return current DMid
   */
  private static String getId() {
    synchronized (DmidMutex) {
      InternalDistributedSystem ids = InternalDistributedSystem
      .getAnyInstance();
      if (ids == null) {
        throw new DistributedSystemDisconnectedException("No distributed system");
      }
      if (ids == IdsForId) {
        return DMid;
      }
      IdsForId = ids;
      DM dm = ids.getDistributionManager();
      DMid = dm.getId().toString();
      return DMid;
    }
  }
  
  /**
   * Returns a byte array which uses a static synchronized counter to ensure
   * uniqueness
   *  
   */
  private static byte[] generateGTid() {
    //Asif: The counter should be attached to the string inside Synch block
    StringBuffer sbuff = new StringBuffer(getId());
    synchronized (GlobalTransaction.class) {
      if (mCounter == 99999)
        mCounter = 1;
      else
        ++mCounter;
      sbuff.append(String.valueOf(mCounter));
    }
    sbuff.append('_').append(System.currentTimeMillis());
    byte[] byte_array = sbuff.toString().getBytes();
    return byte_array;
  }

  /**
   * A timer task cleaup method. This is called when Transaction timeout occurs.
   * On timeout the transaction is rolled back and the thread removed from
   * thread-Transaction Map.
   */
  void expireGTX() {
    if (timedOut) return; // this method is only called by a single thread so this is safe
    timedOut = true;
    LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
    try {
      if (writer.infoEnabled())
          writer
              .info(LocalizedStrings.GlobalTransaction_TRANSACTION_0_HAS_TIMED_OUT, this);
      TransactionManagerImpl.getTransactionManager()
          .removeTranxnMappings(transactions);
      setStatus(Status.STATUS_NO_TRANSACTION);
    }
    catch (Exception e) {
      if (writer.severeEnabled()) writer.severe(LocalizedStrings.GlobalTransaction_GLOBATRANSACTION_EXPIREGTX_ERROR_OCCURED_WHILE_REMOVING_TRANSACTIONAL_MAPPINGS_0, e, e);
    }
  }

  /**
   * Set the transaction TimeOut of the Global Transaction Asif : It returns the
   * new expiry time for the GTX.
   * 
   * @param seconds
   * @throws SystemException
   */
  long setTransactionTimeoutForXARes(int seconds) throws SystemException
  {

    XAResource xar = null;
    boolean resetXATimeOut = true;
    Map.Entry entry;
    synchronized (this.resourceMap) {
      Iterator iterator = resourceMap.entrySet().iterator();
      while (iterator.hasNext()) {
        entry= (Map.Entry)iterator.next();
        xar = (XAResource)entry.getKey();        
        if (((Boolean)entry.getValue()).booleanValue()) {
          try {
            resetXATimeOut = xar.setTransactionTimeout(seconds);
          }
          catch (XAException e) {
            String exception = LocalizedStrings.GlobalTransaction_EXCEPTION_OCCURED_WHILE_TRYING_TO_SET_THE_XARESOURCE_TIMEOUT_DUE_TO_0_ERROR_CODE_1.toLocalizedString(new Object[] {e, Integer.valueOf(e.errorCode)});
            LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
            if (VERBOSE)
              writer.fine(exception);
            throw new SystemException(exception);
          }
          break;
        }
      }
    }
    long newExp = System.currentTimeMillis() + (seconds * 1000);
    if (!resetXATimeOut)
      newExp = -1;
    return newExp;
  }

  /**
   * Testmethod to provide the size of the resource map
   *
   */
  int getResourceMapSize() {
    return this.resourceMap.size();
  }
  /**
   * Returns the List of Transactions associated with this GlobalTransaction
   * Asif : Reduced the visibility
   */
  List getTransactions() {
    return transactions;
  }

  long getExpirationTime() {
    return expirationTime;
  }

  void setTimeoutValue(long time) {
    expirationTime = time;
  }

  boolean isExpired() {
    return timedOut;
  }
  
  public int compare(GlobalTransaction other) {
    if (this == other) {
      return 0;
    }
    long compare = getExpirationTime() - other.getExpirationTime();
    if (compare < 0) {
      return -1;
    } else if (compare > 0) {
      return 1;
    }
    // need to compare something else to break the tie to fix bug 39579
    if (this.GTid.length < other.GTid.length) {
      return -1;
    } else if (this.GTid.length > other.GTid.length) {
      return 1;
    }
    // need to compare the bytes
    for (int i=0; i < this.GTid.length; i++) {
      if (this.GTid[i] < other.GTid[i]) {
        return -1;
      } else if (this.GTid[i] > other.GTid[i]) {
        return 1;
      }
    }
    // If we get here the GTids are the same!
    int myId = System.identityHashCode(this);
    int otherId = System.identityHashCode(other);
    if (myId < otherId) {
      return -1;
    } else if (myId > otherId) {
      return 1;
    } else {
      // we could just add another field to this class which has a value
      // obtained from a static atomic
      throw new IllegalStateException(
        LocalizedStrings.GlobalTransaction_COULD_NOT_COMPARE_0_TO_1
          .toLocalizedString(new Object[] {this, other}));
    }
  }
  
//   public String toString() {
//     StringBuffer sb = new StringBuffer();
//     sb.append("<");
//     sb.append(super.toString());
//     sb.append(" expireTime=" + getExpirationTime());
//     sb.append(">");
//     return sb.toString();
//   }
}
