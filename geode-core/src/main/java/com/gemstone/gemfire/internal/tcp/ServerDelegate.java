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

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.*;
import com.gemstone.gemfire.i18n.LogWriterI18n;


/** <p>ServerDelegate is a conduit plugin that receives
    {@link com.gemstone.gemfire.distributed.internal.DistributionMessage}
    objects received from other conduits.</p>

    @see com.gemstone.gemfire.distributed.internal.direct.DirectChannel

    @since GemFire 2.0
   
  */
public interface ServerDelegate {

  public void receive( DistributionMessage message, int bytesRead,
                       DistributedMember connId );

  public LogWriterI18n getLogger();

  /**
   * Called when a possibly new member is detected by receiving a direct channel
   * message from him.
   */
  public void newMemberConnected(InternalDistributedMember member);
}
