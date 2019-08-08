/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.logging.internal;

import org.apache.geode.logging.spi.SessionContext;

public interface InternalSessionContext extends SessionContext {
  State getState();

  enum State {
    CREATED,
    STARTED,
    STOPPED;

    public State changeTo(final State newState) {
      switch (newState) {
        case CREATED:
          if (this != STOPPED) {
            throw new IllegalStateException("Session must not exist before creating");
          }
          return CREATED;
        case STARTED:
          if (this != CREATED) {
            throw new IllegalStateException("Session must be created before starting");
          }
          return STARTED;
        case STOPPED:
          if (this != STARTED) {
            throw new IllegalStateException("Session must be started before stopping");
          }
      }
      return STOPPED;
    }
  }
}
