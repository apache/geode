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
#include <gfcpp/CqState.hpp>

using namespace apache::geode::client;

bool CqState::isRunning() const { return (m_state == RUNNING); }

bool CqState::isStopped() const { return (m_state == STOPPED); }

bool CqState::isClosed() const { return (m_state == CLOSED); }
bool CqState::isClosing() const { return (m_state == CLOSING); }
void CqState::setState(CqState::StateType state) { m_state = state; }

CqState::StateType CqState::getState() { return m_state; }
const char* CqState::toString() const {
  switch (m_state) {
    case STOPPED:
      return "STOPPED";
    case RUNNING:
      return "RUNNING";
    case CLOSED:
      return "CLOSED";
    default:
      return "UNKNOWN";
  }
}
