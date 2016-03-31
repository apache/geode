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
package com.gemstone.gemfire.internal.sequencelog;

import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 *
 */
public class MessageLogger {
  
  public static final SequenceLogger LOGGER = SequenceLoggerImpl.getInstance();
  
  public static boolean isEnabled() {
    return LOGGER.isEnabled(GraphType.MESSAGE);
  }
  
  public static void logMessage(DistributionMessage message, InternalDistributedMember source, InternalDistributedMember dest) {
    if(isEnabled()) {
      LOGGER.logTransition(GraphType.MESSAGE, message.getClass().getSimpleName(), "", "received", source, dest);
    }
  }

}
