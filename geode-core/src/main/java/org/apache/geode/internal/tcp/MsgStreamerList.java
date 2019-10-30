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

package org.apache.geode.internal.tcp;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Encapsulates a set of {@link MsgStreamer}s and {@link VersionedMsgStreamer}s requiring possibly
 * different serializations for different versions of product.
 *
 * @since GemFire 7.1
 */
public class MsgStreamerList implements BaseMsgStreamer {
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
  public void reserveConnections(long startTime, long ackTimeout, long ackSDTimeout) {
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
        // TODO: shouldn't we call continue here?
        // It seems wrong to call writeMessage on a streamer we have just released.
        // But why do we call release on a streamer when we had an exception on one
        // of the previous streamer?
        // release clears the direct bb and returns it to the pool but leaves
        // it has the "buffer". THen we call writeMessage and it will use "buffer"
        // that has also been returned to the pool.
        // I think we only have a MsgStreamerList when a DS has a mix of versions
        // which usually is just during a rolling upgrade so that might be why we
        // haven't noticed this causing a bug.
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
        sentCons = (List<Object>) streamer.getSentConnections();
      } else {
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
      } else {
        // loop through all failures and add to base ConnectionException
        ConnectExceptions e = streamer.getConnectExceptions();
        if (e != null) {
          List<?> members = e.getMembers();
          List<?> exs = e.getCauses();
          for (int i = 0; i < exs.size(); i++) {
            ce.addFailure((InternalDistributedMember) members.get(i), (Throwable) exs.get(i));
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
        } else {
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
