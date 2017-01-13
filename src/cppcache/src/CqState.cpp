/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/CqState.hpp>

using namespace gemfire;

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
