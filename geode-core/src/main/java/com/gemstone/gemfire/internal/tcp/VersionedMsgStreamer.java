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

package com.gemstone.gemfire.internal.tcp;

import java.util.List;

import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataStream;

/**
 * An extension of {@link MsgStreamer} that implements
 * {@link VersionedDataStream}.
 * 
 * @since GemFire 7.1
 */
final class VersionedMsgStreamer extends MsgStreamer implements
    VersionedDataStream {

  private final Version version;

  VersionedMsgStreamer(List<?> cons, DistributionMessage msg,
      boolean directReply, DMStats stats, int sendBufferSize, Version version) {
    super(cons, msg, directReply, stats, sendBufferSize);
    this.version = version;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final Version getVersion() {
    return this.version;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return super.toString() + " (" + this.version + ')';
  }
}
