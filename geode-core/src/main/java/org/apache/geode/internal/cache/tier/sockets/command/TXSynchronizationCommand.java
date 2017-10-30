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

package org.apache.geode.internal.cache.tier.sockets.command;

import org.apache.geode.cache.client.internal.TXSynchronizationOp.CompletionType;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.TXCommitMessage;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.TXSynchronizationRunnable;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.MessageTooLargeException;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;

import java.io.IOException;
import java.util.concurrent.Executor;

import javax.transaction.Status;

public class TXSynchronizationCommand extends BaseCommand {

  private final static TXSynchronizationCommand singleton = new TXSynchronizationCommand();

  public static Command getCommand() {
    return singleton;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.tier.sockets.BaseCommand#shouldMasqueradeForTx(org.apache.geode
   * .internal.cache.tier.sockets.Message,
   * org.apache.geode.internal.cache.tier.sockets.ServerConnection)
   */
  @Override
  protected boolean shouldMasqueradeForTx(Message clientMessage,
      ServerConnection serverConnection) {
    // masquerading is done in the waiting thread pool
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.tier.sockets.BaseCommand#cmdExecute(org.apache.geode.internal.
   * cache.tier.sockets.Message, org.apache.geode.internal.cache.tier.sockets.ServerConnection,
   * long)
   */
  @Override
  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start)
      throws IOException, ClassNotFoundException, InterruptedException {

    serverConnection.setAsTrue(REQUIRES_RESPONSE);

    CompletionType type = CompletionType.values()[clientMessage.getPart(0).getInt()];
    /* int txIdInt = */ clientMessage.getPart(1).getInt(); // [bruce] not sure if we need to
                                                           // transmit this
    final Part statusPart;
    if (type == CompletionType.AFTER_COMPLETION) {
      statusPart = clientMessage.getPart(2);
    } else {
      statusPart = null;
    }

    final TXManagerImpl txMgr =
        (TXManagerImpl) serverConnection.getCache().getCacheTransactionManager();
    final InternalDistributedMember member =
        (InternalDistributedMember) serverConnection.getProxyID().getDistributedMember();

    // get the tx state without associating it with this thread. That's done later
    final TXStateProxy txProxy = txMgr.masqueradeAs(clientMessage, member, true);

    // we have to run beforeCompletion and afterCompletion in the same thread
    // because beforeCompletion obtains locks for the thread and afterCompletion
    // releases them
    if (txProxy != null) {
      final boolean isDebugEnabled = logger.isDebugEnabled();
      try {
        if (type == CompletionType.BEFORE_COMPLETION) {
          Runnable beforeCompletion = new Runnable() {
            @SuppressWarnings("synthetic-access")
            public void run() {
              TXStateProxy txState = null;
              Throwable failureException = null;
              try {
                txState = txMgr.masqueradeAs(clientMessage, member, false);
                if (isDebugEnabled) {
                  logger.debug("Executing beforeCompletion() notification for transaction {}",
                      clientMessage.getTransactionId());
                }
                txState.setIsJTA(true);
                txState.beforeCompletion();
                try {
                  writeReply(clientMessage, serverConnection);
                } catch (IOException e) {
                  if (isDebugEnabled) {
                    logger.debug("Problem writing reply to client", e);
                  }
                }
                serverConnection.setAsTrue(RESPONDED);
              } catch (ReplyException e) {
                failureException = e.getCause();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } catch (Exception e) {
                failureException = e;
              } finally {
                txMgr.unmasquerade(txState);
              }
              if (failureException != null) {
                try {
                  writeException(clientMessage, failureException, false, serverConnection);
                } catch (IOException ioe) {
                  if (isDebugEnabled) {
                    logger.debug("Problem writing reply to client", ioe);
                  }
                }
                serverConnection.setAsTrue(RESPONDED);
              }
            }
          };
          TXSynchronizationRunnable sync =
              new TXSynchronizationRunnable(beforeCompletion, serverConnection.getAcceptor());
          txProxy.setSynchronizationRunnable(sync);
          Executor exec = InternalDistributedSystem.getConnectedInstance().getDistributionManager()
              .getWaitingThreadPool();
          exec.execute(sync);
          sync.waitForFirstExecution();
        } else {
          Runnable afterCompletion = new Runnable() {
            @SuppressWarnings("synthetic-access")
            public void run() {
              TXStateProxy txState = null;
              try {
                txState = txMgr.masqueradeAs(clientMessage, member, false);
                int status = statusPart.getInt();
                if (isDebugEnabled) {
                  logger.debug("Executing afterCompletion({}) notification for transaction {}",
                      status, clientMessage.getTransactionId());
                }
                txState.setIsJTA(true);
                txState.afterCompletion(status);
                // GemFire commits during afterCompletion - send the commit info back to the client
                // where it can be applied to the local cache
                TXCommitMessage cmsg = txState.getCommitMessage();
                try {
                  CommitCommand.writeCommitResponse(cmsg, clientMessage, serverConnection);
                  txMgr.removeHostedTXState(txState.getTxId());
                } catch (IOException e) {
                  // not much can be done here
                  if (isDebugEnabled || (e instanceof MessageTooLargeException)) {
                    logger.warn("Problem writing reply to client", e);
                  }
                }
                serverConnection.setAsTrue(RESPONDED);
              } catch (RuntimeException e) {
                try {
                  writeException(clientMessage, e, false, serverConnection);
                } catch (IOException ioe) {
                  if (isDebugEnabled) {
                    logger.debug("Problem writing reply to client", ioe);
                  }
                }
                serverConnection.setAsTrue(RESPONDED);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } finally {
                txMgr.unmasquerade(txState);
              }
            }
          };
          // if there was a beforeCompletion call then there will be a thread
          // sitting in the waiting pool to execute afterCompletion. Otherwise
          // we have failed-over and may need to do beforeCompletion & hope that it works
          TXSynchronizationRunnable sync = txProxy.getSynchronizationRunnable();
          if (sync != null) {
            sync.runSecondRunnable(afterCompletion);
          } else {
            if (statusPart.getInt() == Status.STATUS_COMMITTED) {
              TXStateProxy txState = txMgr.masqueradeAs(clientMessage, member, false);
              try {
                if (isDebugEnabled) {
                  logger.debug(
                      "Executing beforeCompletion() notification for transaction {} after failover",
                      clientMessage.getTransactionId());
                }
                txState.setIsJTA(true);
                txState.beforeCompletion();
              } finally {
                txMgr.unmasquerade(txState);
              }
            }
            afterCompletion.run();
          }
        }
      } catch (Exception e) {
        writeException(clientMessage, MessageType.EXCEPTION, e, false, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
      }
      if (isDebugEnabled) {
        logger.debug("Sent tx synchronization response");
      }
    }
  }

}
