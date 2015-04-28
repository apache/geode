/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DurableClientAttributes;
import com.gemstone.gemfire.distributed.Role;
import com.gemstone.gemfire.distributed.internal.locks.ElderState;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MemberAttributes;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.GFJGBasicAdapter;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.org.jgroups.JChannel;

/**
 * A <code>LonerDistributionManager</code> is a dm that never communicates
 * with anyone else.
 *
 * @author Darrel
 *
 *
 * @since 3.5
 */
public class LonerDistributionManager implements DM {
  private final InternalDistributedSystem system;
  private final InternalLogWriter logger;
  private ElderState elderState;
  
  
  ////////////////////////  Constructors  ////////////////////////

  /**
   * Creates a new local distribution manager
   *
   * @param system
   *        The distributed system to which this distribution manager
   *        will send messages.
   *
   */
  public LonerDistributionManager(InternalDistributedSystem system,
                                  InternalLogWriter logger) {
    this.system = system;
    this.logger = logger;
    this.id = generateMemberId();
    this.allIds = Collections.singleton(id);
    this.viewMembers = new Vector(allIds);
    DistributionStats.enableClockStats = this.system.getConfig().getEnableTimeStatistics();
    JChannel.setDefaultGFFunctions(new GFJGBasicAdapter());
  }

  //////////////////////  Instance Methods  //////////////////////

  protected void startThreads() {
    // no threads needed
  }

  protected void shutdown() {
  }

  private final InternalDistributedMember id;

  /*static {
    // Make the id a little unique
    String host;
    try {
      host = InetAddress.getLocalHost().getCanonicalHostName();
      MemberAttributes.setDefaults(65535,
              com.gemstone.gemfire.internal.OSProcess.getId(),
              DistributionManager.LONER_DM_TYPE,
              MemberAttributes.parseRoles(system.getConfig().getRoles()));
      id = new InternalDistributedMember(host, 65535); // noise value for port number

    } catch (UnknownHostException ex) {
      throw new InternalError(LocalizedStrings.LonerDistributionManager_CANNOT_RESOLVE_LOCAL_HOST_NAME_TO_AN_IP_ADDRESS.toLocalizedString());
    }

  }*/

  private final Set<InternalDistributedMember> allIds;// = Collections.singleton(id);
  private final Vector viewMembers;// = new Vector(allIds);
  private ConcurrentMap<InternalDistributedMember, InternalDistributedMember> canonicalIds = new ConcurrentHashMap();
  static private final DummyDMStats stats = new DummyDMStats();
  static private final DummyExecutor executor = new DummyExecutor();

  @Override
  public long cacheTimeMillis() {
    return this.system.getClock().cacheTimeMillis();
  }
  public InternalDistributedMember getDistributionManagerId() {
    return id;
  }

  public Set getDistributionManagerIds() {
    return allIds;
  }

  public Set getDistributionManagerIdsIncludingAdmin() {
    return allIds;
  }
  public Serializable[] getDirectChannels(InternalDistributedMember[] ids) {
    return ids;
  }
  
  public InternalDistributedMember getCanonicalId(DistributedMember dmid) {
    InternalDistributedMember iid = (InternalDistributedMember)dmid;
    InternalDistributedMember result = this.canonicalIds.putIfAbsent(iid,iid);
    if (result != null) {
      return result;
    }
    return iid;
  }

  public Set getOtherDistributionManagerIds() {
    return Collections.EMPTY_SET;
  }
  @Override
  public Set getOtherNormalDistributionManagerIds() {
    return Collections.EMPTY_SET;
  }
  public Set getAllOtherMembers() {
    return Collections.EMPTY_SET;
  }
  
  @Override // DM method
  public void retainMembersWithSameOrNewerVersion(Collection<InternalDistributedMember> members, Version version) {
    for (Iterator<InternalDistributedMember> it = members.iterator(); it.hasNext(); ) {
      InternalDistributedMember id = it.next();
      if (id.getVersionObject().compareTo(version) < 0) {
        it.remove();
      }
    }
  }
  
  @Override // DM method
  public void removeMembersWithSameOrNewerVersion(Collection<InternalDistributedMember> members, Version version) {
    for (Iterator<InternalDistributedMember> it = members.iterator(); it.hasNext(); ) {
      InternalDistributedMember id = it.next();
      if (id.getVersionObject().compareTo(version) >= 0) {
        it.remove();
      }
    }
  }
  

  public Set addMembershipListenerAndGetDistributionManagerIds(MembershipListener l) {
    //return getOtherDistributionManagerIds();
    return allIds;
  }
  public Set addAllMembershipListenerAndGetAllIds(MembershipListener l) {
    return allIds;
  }
  public int getDistributionManagerCount() {
    return 0;
  }
  public InternalDistributedMember getId() {
    return getDistributionManagerId();
  }
  public boolean isAdam() {
    return true;
  }
  public InternalDistributedMember getElderId() {
    return getId();
  }
  public boolean isElder() {
    return true;
  }
  public boolean isLoner() {
    return true;
  }

  public synchronized ElderState getElderState(boolean force, boolean useTryLock) {
    // loners are always the elder
    if (this.elderState == null) {
      this.elderState = new ElderState(this);
    }
    return this.elderState;
  }

  public long getChannelId() {
    return 0;
  }

  public Set putOutgoingUserData(final DistributionMessage message) {
    if (message.forAll() || message.getRecipients().length == 0) {
      // do nothing
      return null;
    } else {
      throw new RuntimeException(LocalizedStrings.LonerDistributionManager_LONER_TRIED_TO_SEND_MESSAGE_TO_0.toLocalizedString(message.getRecipientsDescription()));
    }
  }
  public InternalDistributedSystem getSystem() {
    return this.system;
  }
  public void addMembershipListener(MembershipListener l) {}

  public void removeMembershipListener(MembershipListener l) {}
  public void removeAllMembershipListener(MembershipListener l) {}

  public void addAdminConsole(InternalDistributedMember p_id) {}

  public DMStats getStats() {
    return stats;
  }
  public DistributionConfig getConfig() {
    DistributionConfig result = null;
    if (getSystem() != null) {
      result = getSystem().getConfig();
    }
    return result;
  }

  public void handleManagerDeparture(InternalDistributedMember p_id, 
      boolean crashed, String reason) {}

  public LogWriterI18n getLoggerI18n() {
    return this.logger;
  }
  public InternalLogWriter getInternalLogWriter() {
    return this.logger;
  }
  public ExecutorService getThreadPool() {
    return executor;
  }
  public ExecutorService getHighPriorityThreadPool() {
    return executor;
  }
  public ExecutorService getWaitingThreadPool() {
    return executor;
  }
  public ExecutorService getPrMetaDataCleanupThreadPool() {
    return executor;
  }
  public Map getChannelMap() {
    return null;
  }
  public Map getMemberMap() {
    return null;
  }
  public void close() {
  }
  public void restartCommunications() {

  }
  public Vector getViewMembers() {
    return viewMembers;
  }

  public DistributedMember getOldestMember(Collection members) throws NoSuchElementException {
    if (members.size() == 1) {
      DistributedMember member = (DistributedMember)members.iterator().next();
      if (member.equals(viewMembers.get(0))) {
        return member;
      }
    }
    throw new NoSuchElementException(LocalizedStrings.LonerDistributionManager_MEMBER_NOT_FOUND_IN_MEMBERSHIP_SET.toLocalizedString());
  }

  public Set getAdminMemberSet(){ return Collections.EMPTY_SET; }

  public static class DummyDMStats implements DMStats {
    public long getSentMessages() {return 0;}
    public void incSentMessages(long messages) {}
    public void incTOSentMsg() {}
    public long getSentCommitMessages() {return 0;}
    public void incSentCommitMessages(long messages) {}
    public long getCommitWaits() {return 0;}
    public void incCommitWaits() {}
    public long getSentMessagesTime() {return 0;}
    public void incSentMessagesTime(long nanos) {}
    public long getBroadcastMessages() {return 0;}
    public void incBroadcastMessages(long messages) {}
    public long getBroadcastMessagesTime() {return 0;}
    public void incBroadcastMessagesTime(long nanos) {}
    public long getReceivedMessages() {return 0;}
    public void incReceivedMessages(long messages) {}
    public long getReceivedBytes() {return 0;}
    public void incReceivedBytes(long bytes) {}
    public void incSentBytes(long bytes) {}
    public long getProcessedMessages() {return 0;}
    public void incProcessedMessages(long messages) {}
    public long getProcessedMessagesTime() {return 0;}
    public void incProcessedMessagesTime(long nanos) {}
    public long getMessageProcessingScheduleTime() {return 0;}
    public int getDLockWaitsInProgress() {return 0;}
    public int getDLockWaitsCompleted() {return 0;}
    public long getDLockWaitTime() {return 0;}
    public long startDLockWait() {return 0;}
    public void endDLockWait(long start, boolean gotit) {}
    public void incDLockVetosSent(int ops) {}
    public void incDLockVetosReceived(int ops) {}
    public void incDLockYesVotesSent(int ops) {}
    public void incDLockYesVotesReceived(int ops) {}
    public void incDLockNoVotesSent(int ops) {}
    public void incDLockNoVotesReceived(int ops) {}
    public void incDLockAbstainsSent(int ops) {}
    public void incDLockAbstainsReceived(int ops) {}
    public void incMessageProcessingScheduleTime(long nanos) {}
    public int getOverflowQueueSize() {return 0;}
    public void incOverflowQueueSize(int messages) {}
    public int getNumProcessingThreads() {return 0;}
    public void incNumProcessingThreads(int threads) {}
    public int getNumSerialThreads() {return 0;}
    public void incNumSerialThreads(int threads) {}
    public void incMessageChannelTime(long val) {}
    public long getReplyMessageTime() {return 0;}
    public void incReplyMessageTime(long val) {}
    public long getDistributeMessageTime() {return 0;}
    public void incDistributeMessageTime(long val) {}
    public int getNodes() {return 0;}
    public void setNodes(int val) {}
    public void incNodes(int val) {}
    public int getReplyWaitsInProgress() {return 0;}
    public int getReplyWaitsCompleted() {return 0;}
    public long getReplyWaitTime() {return 0;}
    public long startReplyWait() {return 0;}
    public void endReplyWait(long startNanos, long startMillis) {}
    public void incReplyTimeouts() { }
    public long getReplyTimeouts() { return 0; }
    public void incReceivers() {}
    public void decReceivers() {}
    public void incFailedAccept() {}
    public void incFailedConnect() {}
    public void incReconnectAttempts() {}
    public void incLostLease() {}
    public void incSenders(boolean shared, boolean preserveOrder) {}
    public void decSenders(boolean shared, boolean preserveOrder) {}
    public int getSendersSU() { return 0; }
    public long startSocketWrite(boolean sync) {return 0; }
    public void endSocketWrite(boolean sync, long start, int bytesWritten, int retries) {}
    public long startSerialization() {return 0;}
    public void endSerialization(long start, int bytes) {}
    public long startDeserialization() {return 0;}
    public void endDeserialization(long start, int bytes) {}
    public long startMsgSerialization() {return 0;}
    public void endMsgSerialization(long start) {}
    public long startMsgDeserialization() {return 0;}
    public void endMsgDeserialization(long start) {}
    public void incBatchSendTime(long start) {}
    public void incBatchCopyTime(long start) {}
    public void incBatchWaitTime(long start) {}
    public void incBatchFlushTime(long start) {}
    public long startUcastWrite() { return 0; }
    public void endUcastWrite(long start, int bytesWritten) {}
    public void incUcastWrites(int bytesWritten) {}
    public long startMcastWrite() { return 0; }
    public void endMcastWrite(long start, int bytesWritten) {}
    public void incMcastWrites(int bytesWritten) {}
    public void incUcastRetransmits() {}
    public void incMcastRetransmits() {}
    public void incMcastRetransmitRequests() {}
    public int getMcastRetransmits() { return 0; }
    public int getMcastWrites() { return 0; }
    public long startUcastFlush() { return 0; }
    public void endUcastFlush(long start) {}
    public void incFlowControlRequests() {}
    public void incFlowControlResponses() {}
    public long startFlowControlWait() { return 0; }
    public void endFlowControlWait(long start) {}
    public long startFlowControlThrottleWait() { return 0; }
    public void endFlowControlThrottleWait(long start) {}
    public void incUcastReadBytes(long amount) {}
    public void incMcastReadBytes(long amount) {}
    public void incJgUNICASTdataReceived(long amount) {}

    public void setJgQueuedMessagesSize(long value) {}
    public void setJgSTABLEreceivedMessagesSize(long value) {}
    public void setJgSTABLEsentMessagesSize(long value) {}
    public void incJgSTABLEsuspendTime(long value) {}
    public void incJgSTABLEmessages(long value) {}
    public void incJgSTABLEmessagesSent(long value) {}
    public void incJgSTABILITYmessages(long value) {}

    public void incjgDownTime(long value){}
    public void incjgUpTime(long value){}
    public void incjChannelUpTime(long value){}
    
    public void incThreadOwnedReceivers(long value, int dominoCount) {}

    public void incJgFCsendBlocks(long value)
    {}
    public void incJgFCautoRequests(long value)
    {}
    public void incJgFCreplenish(long value)
    {}
    public void incJgFCresumes(long value)
    {}
    public void incJgFCsentCredits(long value)
    {}
    public void incJgFCsentThrottleRequests(long value)
    {}
    public void incJgFragmentationsPerformed()
    {}
    public void incJgFragmentsCreated(long numFrags)
    {}
    public void setJgUNICASTreceivedMessagesSize(long amount) {
    }
    public void setJgUNICASTsentMessagesSize(long amount) {
    }
    public void setJgUNICASTsentHighPriorityMessagesSize(long amount) {
    }
    public int getAsyncSocketWritesInProgress() {return 0;}
    public int getAsyncSocketWrites() {return 0;}
    public int getAsyncSocketWriteRetries() {return 0;}
    public long getAsyncSocketWriteBytes() {return 0;}
    public long getAsyncSocketWriteTime() {return 0;}
    public int getAsyncQueues() {return 0;}
    public void incAsyncQueues(int inc) {}
    public int getAsyncQueueFlushesInProgress() {return 0;}
    public int getAsyncQueueFlushesCompleted() {return 0;}
    public long getAsyncQueueFlushTime() {return 0;}
    public long startAsyncQueueFlush() {return 0;}
    public void endAsyncQueueFlush(long start) {}
    public int getAsyncQueueTimeouts() {return 0;}
    public void incAsyncQueueTimeouts(int inc) {}
    public int getAsyncQueueSizeExceeded() {return 0;}
    public void incAsyncQueueSizeExceeded(int inc) {}
    public int getAsyncDistributionTimeoutExceeded() {return 0;}
    public void incAsyncDistributionTimeoutExceeded() {}
    public long getAsyncQueueSize() {return 0;}
    public void incAsyncQueueSize(long inc) {}
    public long getAsyncQueuedMsgs() {return 0;}
    public void incAsyncQueuedMsgs() {}
    public long getAsyncDequeuedMsgs() {return 0;}
    public void incAsyncDequeuedMsgs() {}
    public long getAsyncConflatedMsgs() {return 0;}
    public void incAsyncConflatedMsgs() {}
    public int getAsyncThreads() {return 0;}
    public void incAsyncThreads(int inc) {}
    public int getAsyncThreadInProgress() {return 0;}
    public int getAsyncThreadCompleted() {return 0;}
    public long getAsyncThreadTime() {return 0;}
    public long startAsyncThread() {return 0;}
    public void endAsyncThread(long start) {}
    public long getAsyncQueueAddTime() {return 0;}
    public void incAsyncQueueAddTime(long inc) {}
    public long getAsyncQueueRemoveTime() {return 0;}
    public void incAsyncQueueRemoveTime(long inc) {}
    public void incJgNAKACKwaits(long value) {}
    public void incThreadOwnedReceivers(long value) {}
    public void incReceiverBufferSize(int inc, boolean direct) {}
    public void incSenderBufferSize(int inc, boolean direct) {}
    public long startSocketLock() {return 0;}
    public void endSocketLock(long start) {}
    public long startBufferAcquire() {return 0;}
    public void endBufferAcquire(long start) {}
    public void incMessagesBeingReceived(boolean newMsg, int bytes) {}
    public void decMessagesBeingReceived(int bytes) {}
    public void incReplyHandOffTime(long start) {}
    public int getElders() {return 0;}
    public void incElders(int val) {}
    public int getInitialImageMessagesInFlight() {return 0;}
    public void incInitialImageMessagesInFlight(int val) {}
    public int getInitialImageRequestsInProgress() {return 0;}
    public void incInitialImageRequestsInProgress(int val) {}
    public void incPdxSerialization(int bytesWritten) {}
    public void incPdxDeserialization(int i) {}
    public long startPdxInstanceDeserialization() {return 0;}
    public void endPdxInstanceDeserialization(long start) {}
    public void incPdxInstanceCreations() {}
  }
  protected static class DummyExecutor implements ExecutorService {
    @Override
    public void execute(Runnable command) {
      command.run();
    }

    @Override
    public void shutdown() {
    }

    @Override
    public List<Runnable> shutdownNow() {
      return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
      return false;
    }

    @Override
    public boolean isTerminated() {
      return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
        throws InterruptedException {
      return true;
    }

    @Override
    public <T> Future<T> submit(final Callable<T> task) {
      Exception ex = null;
      T result = null;
      try {
        result = task.call();
      } catch (Exception e) {
        ex = e;
      }
      return new CompletedFuture<T>(result, ex);
    }

    @Override
    public <T> Future<T> submit(final Runnable task, final T result) {
      return submit(new Callable<T>() {
        @Override
        public T call() throws Exception {
          task.run();
          return result;
        }
      });
    }

    @Override
    public Future<?> submit(Runnable task) {
      return submit(task, null);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
        throws InterruptedException {
      List<Future<T>> results = new ArrayList<Future<T>>();
      for (Callable<T> task : tasks) {
        results.add(submit(task));
      }
      return results;
    }

    @Override
    public <T> List<Future<T>> invokeAll(
        Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException {
      return invokeAll(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
        throws InterruptedException, ExecutionException {

      ExecutionException ex = null;
      for (Callable<T> task : tasks) {
        try {
          return submit(task).get();
        } catch (ExecutionException e) {
          ex = e;
        }
      }
      throw (ExecutionException) ex.fillInStackTrace();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
        long timeout, TimeUnit unit) throws InterruptedException,
        ExecutionException, TimeoutException {
      return invokeAny(tasks);
    }
  }
  
  private static class CompletedFuture<T> implements Future<T> {
    private final T result;
    private final Exception ex;
    
    public CompletedFuture(T result, Exception ex) {
      this.result = result;
      this.ex = ex;
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return false;
    }

    @Override
    public boolean isCancelled() {
      return false;
    }

    @Override
    public boolean isDone() {
      return true;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
      if (ex != null) {
        throw new ExecutionException(ex);
      }
      return result;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException,
        ExecutionException, TimeoutException {
      return get();
    }
  }
  
  public void throwIfDistributionStopped() {
    stopper.checkCancelInProgress(null);
  }

  /** Returns count of members filling the specified role */
  public int getRoleCount(Role role) {
    return id.getRoles().contains(role) ? 1 : 0;
  }

  /** Returns true if at least one member is filling the specified role */
  public boolean isRolePresent(Role role) {
    return id.getRoles().contains(role);
  }

  /** Returns a set of all roles currently in the distributed system. */
  public Set getAllRoles() {
    return id.getRoles();
  }

  private int lonerPort = 0;

  //private static final int CHARS_32KB = 16384;
  private InternalDistributedMember generateMemberId() {
    InternalDistributedMember result = null;
    String host;
    try {
      // create string of the current millisecond clock time
      StringBuffer sb = new StringBuffer();
      // use low four bytes for backward compatibility
      long time = System.currentTimeMillis() & 0xffffffffL;
      for (int i = 0; i < 4; i++) {
        String hex = Integer.toHexString((int)(time & 0xff));
        if (hex.length() < 2) {
          sb.append('0');
        }
        sb.append(hex);
        time = time / 0x100;
      }
      String uniqueString = sb.toString();

      String name = this.system.getName();

      host = SocketCreator.getLocalHost().getCanonicalHostName();
      DistributionConfig config = system.getConfig();
      DurableClientAttributes dac = null;
      if (config.getDurableClientId() != null) {
        dac = new DurableClientAttributes(config.getDurableClientId(), config
            .getDurableClientTimeout());
      }
      MemberAttributes.setDefaults(lonerPort,
              com.gemstone.gemfire.internal.OSProcess.getId(),
              DistributionManager.LONER_DM_TYPE, -1,
              name,
              MemberAttributes.parseGroups(config.getRoles(), config.getGroups()), dac);
      result = new InternalDistributedMember(host, lonerPort, name, uniqueString);

    } catch (UnknownHostException ex) {
      throw new InternalGemFireError(LocalizedStrings.LonerDistributionManager_CANNOT_RESOLVE_LOCAL_HOST_NAME_TO_AN_IP_ADDRESS.toLocalizedString());
    }
    return result;
  }

  /**
   * update the loner port with an integer that may be more unique than the 
   * default port (zero).  This updates the ID in place and establishes new
   * default settings for the manufacture of new IDs.
   * 
   * @param newPort the new port to use
   */
  public void updateLonerPort(int newPort) {
    this.logger.config(LocalizedStrings.LonerDistributionmanager_CHANGING_PORT_FROM_TO,
        new Object[]{this.lonerPort, newPort});
    this.lonerPort = newPort;
    MemberAttributes.setDefaults(lonerPort,
        MemberAttributes.DEFAULT.getVmPid(),
        DistributionManager.LONER_DM_TYPE,
        -1,
        MemberAttributes.DEFAULT.getName(),
        MemberAttributes.DEFAULT.getGroups(), MemberAttributes.DEFAULT.getDurableClientAttributes());
    this.getId().setPort(this.lonerPort);
  }
  public boolean isCurrentMember(InternalDistributedMember p_id) {
    return getId().equals(p_id);
  }

  public Set putOutgoing(DistributionMessage msg)
  {
    return null;
  }

  public boolean shutdownInProgress() {
    return false;
  }

  public void removeUnfinishedStartup(InternalDistributedMember m, boolean departed) {
  }

  public void setUnfinishedStartups(Collection s) {
  }

  protected static class Stopper extends CancelCriterion {

    @Override
    public String cancelInProgress() {
      checkFailure();
      return null;
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      return null;
    }
  }
  private final Stopper stopper = new Stopper();

  public CancelCriterion getCancelCriterion() {
    return stopper;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.distributed.internal.DM#getMembershipManager()
   */
  public MembershipManager getMembershipManager() {
    // no membership manager
    return null;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.distributed.internal.DM#getRootCause()
   */
  public Throwable getRootCause() {
    return null;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.distributed.internal.DM#setRootCause(java.lang.Throwable)
   */
  public void setRootCause(Throwable t) {
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.distributed.internal.DM#getMembersOnThisHost()
   * @since gemfire59poc
   */
  public Set<InternalDistributedMember> getMembersInThisZone() {
    return this.allIds;
  }

  public void acquireGIIPermitUninterruptibly() {
  }

  public void releaseGIIPermit() {
  }

  public int getDistributedSystemId() {
    return getSystem().getConfig().getDistributedSystemId();
  }

  public boolean enforceUniqueZone() {
    return system.getConfig().getEnforceUniqueHost() || system.getConfig().getRedundancyZone() != null;
  }

  public boolean areInSameZone(InternalDistributedMember member1,
      InternalDistributedMember member2) {
    return false;
  }

  public boolean areOnEquivalentHost(InternalDistributedMember member1,
                                     InternalDistributedMember member2) {
    return member1 == member2;
  }
  
  public Set<InternalDistributedMember> getMembersInSameZone(
      InternalDistributedMember acceptedMember) {
    return Collections.singleton(acceptedMember);
  }
  
  public Set<InetAddress> getEquivalents(InetAddress in) {
    Set<InetAddress> value = new HashSet<InetAddress>();
    value.add(this.getId().getIpAddress());
    return value;
  }

  public Set<DistributedMember> getGroupMembers(String group) {
    if (getDistributionManagerId().getGroups().contains(group)) {
      return Collections.singleton((DistributedMember)getDistributionManagerId());
    } else {
      return Collections.emptySet();
    }
  }

  public void addHostedLocators(InternalDistributedMember member, Collection<String> locators, boolean isSharedConfigurationEnabled) {
    // no-op
  }
  
  public Collection<String> getHostedLocators(InternalDistributedMember member) {
    return Collections.<String>emptyList();
  }
  
  public Map<InternalDistributedMember, Collection<String>> getAllHostedLocators() {
    return Collections.<InternalDistributedMember, Collection<String>>emptyMap();
  }

  @Override
  public Set getNormalDistributionManagerIds() {
    return getDistributionManagerIds();
  }

  @Override
  public Map<InternalDistributedMember, Collection<String>> getAllHostedLocatorsWithSharedConfiguration() {
    return Collections.<InternalDistributedMember, Collection<String>>emptyMap();
  }

  @Override
  public void forceUDPMessagingForCurrentThread() {
    // no-op for loners
  }

  @Override
  public void releaseUDPMessagingForCurrentThread() {
    // no-op for loners
  }

  @Override
  public boolean isSharedConfigurationServiceEnabledForDS() {
    //return false for loner
    return false;
  }
}
