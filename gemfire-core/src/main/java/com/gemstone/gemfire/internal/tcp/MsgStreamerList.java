/*=========================================================================
 * Copyright (c) 2009-2014 Pivotal Software, Inc. All Rights Reserved. This product
 * is protected by U.S. and international copyright and intellectual
 * property laws. Pivotal products are covered by one or more patents listed
 * at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.tcp;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Encapsulates a set of {@link MsgStreamer}s and {@link VersionedMsgStreamer}s
 * requiring possibly different serializations for different versions of
 * product.
 * 
 * @author swale
 * @since 7.1
 */
public final class MsgStreamerList implements BaseMsgStreamer {
  private static final Logger logger = LogService.getLogger();

  /**
   * List of {@link MsgStreamer}s encapsulated by this MsgStreamerList.
   */
  private final List<MsgStreamer> streamers;

  MsgStreamerList(List<MsgStreamer> streamers) {
    this.streamers = streamers;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reserveConnections(long startTime, long ackTimeout,
      long ackSDTimeout) {
    for (MsgStreamer streamer : this.streamers) {
      streamer.reserveConnections(startTime, ackTimeout, ackSDTimeout);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int writeMessage() throws IOException {
    int result = 0;
    RuntimeException ex = null;
    IOException ioex = null;
    for (MsgStreamer streamer : this.streamers) {
      if (ex != null) {
        streamer.release();
      }
      try {
        result += streamer.writeMessage();
        // if there is an exception we need to finish the
        // loop and release the other streamer's buffers
      } catch (RuntimeException e) {
        ex = e;
      } catch (IOException e) {
        ioex = e;
      }
    }
    if (ex != null) {
      throw ex;
    }
    if (ioex != null) {
      throw ioex;
    }
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public List<?> getSentConnections() {
    List<Object> sentCons = Collections.emptyList();
    for (MsgStreamer streamer : this.streamers) {
      if (sentCons.size() == 0) {
        sentCons = (List<Object>)streamer.getSentConnections();
      }
      else {
        sentCons.addAll(streamer.getSentConnections());
      }
    }
    return sentCons;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ConnectExceptions getConnectExceptions() {
    ConnectExceptions ce = null;
    for (MsgStreamer streamer : this.streamers) {
      if (ce == null) {
        ce = streamer.getConnectExceptions();
      }
      else {
        // loop through all failures and add to base ConnectionException
        ConnectExceptions e = streamer.getConnectExceptions();
        if (e != null) {
          List<?> members = e.getMembers();
          List<?> exs = e.getCauses();
          for (int i = 0; i < exs.size(); i++) {
            ce.addFailure((InternalDistributedMember)members.get(i),
                (Throwable)exs.get(i));
          }
        }
      }
    }
    return ce;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws IOException {
    // only throw the first exception and try to close all
    IOException ex = null;
    for (MsgStreamer m : this.streamers) {
      try {
        m.close();
      } catch (IOException e) {
        if (ex == null) {
          ex = e;
        }
        else {
          // log the exception and move on to close others
          logger.fatal("Unknown error closing streamer: {}", e.getMessage(), e);
        }
      }
    }
    if (ex != null) {
      throw ex;
    }
  }
}
