/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
#include "config.h"
#include "ExpiryTaskManager.hpp"
#include <gfcpp/Log.hpp>
#include <gfcpp/Assert.hpp>
#include <gfcpp/DistributedSystem.hpp>
#include "DistributedSystemImpl.hpp"

#if defined(_WIN32)
#include <ace/WFMO_Reactor.h>
#endif
#if defined(WITH_ACE_Select_Reactor)
#include <ace/Select_Reactor.h>
#else
#include <ace/Dev_Poll_Reactor.h>
#endif

using namespace gemfire;

const char* ExpiryTaskManager::NC_ETM_Thread = "NC ETM Thread";

ExpiryTaskManager::ExpiryTaskManager() : m_reactorEventLoopRunning(false) {
#if defined(_WIN32)
  m_reactor = new ACE_Reactor(
      new ACE_WFMO_Reactor(NULL, new GF_Timer_Heap_ImmediateReset()), 1);
#elif defined(WITH_ACE_Select_Reactor)
  m_reactor = new ACE_Reactor(
      new ACE_Select_Reactor(NULL, new GF_Timer_Heap_ImmediateReset()), 1);
#else
  m_reactor = new ACE_Reactor(
      new ACE_Dev_Poll_Reactor(NULL, new GF_Timer_Heap_ImmediateReset()), 1);
#endif
}

long ExpiryTaskManager::scheduleExpiryTask(ACE_Event_Handler* handler,
                                           uint32_t expTime, uint32_t interval,
                                           bool cancelExistingTask) {
  LOGFINER("ExpiryTaskManager: expTime %d, interval %d, cancelExistingTask %d",
           expTime, interval, cancelExistingTask);
  if (cancelExistingTask) {
    m_reactor->cancel_timer(handler, 1);
  }

  ACE_Time_Value expTimeValue(expTime);
  ACE_Time_Value intervalValue(interval);
  LOGFINER("Scheduled expiration ... in %d seconds.", expTime);
  return m_reactor->schedule_timer(handler, 0, expTimeValue, intervalValue);
}

long ExpiryTaskManager::scheduleExpiryTask(ACE_Event_Handler* handler,
                                           ACE_Time_Value expTimeValue,
                                           ACE_Time_Value intervalVal,
                                           bool cancelExistingTask) {
  if (cancelExistingTask) {
    m_reactor->cancel_timer(handler, 1);
  }

  return m_reactor->schedule_timer(handler, 0, expTimeValue, intervalVal);
}

int ExpiryTaskManager::resetTask(long id, uint32_t sec) {
  ACE_Time_Value interval(sec);
  return m_reactor->reset_timer_interval(id, interval);
}

int ExpiryTaskManager::cancelTask(long id) {
  return m_reactor->cancel_timer(id, 0, 0);
}

int ExpiryTaskManager::svc() {
  DistributedSystemImpl::setThreadName(NC_ETM_Thread);
  LOGFINE("ExpiryTaskManager thread is running.");
  m_reactorEventLoopRunning = true;
  m_reactor->owner(ACE_OS::thr_self());
  m_reactor->run_reactor_event_loop();
  LOGFINE("ExpiryTaskManager thread has stopped.");
  return 0;
}

void ExpiryTaskManager::stopExpiryTaskManager() {
  if (m_reactorEventLoopRunning) {
    m_reactor->end_reactor_event_loop();
    this->wait();
    GF_D_ASSERT(m_reactor->reactor_event_loop_done() > 0);
    m_reactorEventLoopRunning = false;
  }
}

void ExpiryTaskManager::begin() {
  this->activate();
  ACE_Time_Value t;
  t.msec(50);
  while (!m_reactorEventLoopRunning) {
    ACE_OS::sleep(t);
  }
}

ExpiryTaskManager::~ExpiryTaskManager() {
  stopExpiryTaskManager();
  delete m_reactor;
  m_reactor = NULL;
}
