/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package org.apache.geode.internal.shared;

import java.io.Closeable;
import java.nio.channels.Channel;

/**
 * Common base methods for {@link Channel} read/write operations (usually buffered).
 */
public interface StreamChannel extends Channel, Closeable {

  /**
   * Return the thread if waiting for read/write on this channel.
   */
  Thread getParkedThread();

  /**
   * Set the thread waiting for read/write on this channel (null to clear).
   */
  void setParkedThread(Thread thread);

  /**
   * Maximum time to wait before giving up read/write on channel
   */
  long getParkNanosMax();
}
