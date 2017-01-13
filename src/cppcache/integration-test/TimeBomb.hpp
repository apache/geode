/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _TEST_TIMEBOMB_HPP_
#define _TEST_TIMEBOMB_HPP_

#include <ace/Task.h>
#include <ace/OS.h>
#include <ace/Time_Value.h>
#include <assert.h>

#define MAX_CLIENT 10

class ClientCleanup {
 private:
  void (*m_cleanupCallback[MAX_CLIENT])();
  int m_numberOfClient;

 public:
  ClientCleanup() : m_numberOfClient(0) {}

  void callClientCleanup() {
    printf("callClientCleanup ... %d \n", m_numberOfClient);
    for (int i = 0; i < m_numberOfClient; i++) {
      try {
        m_cleanupCallback[i]();
      } catch (...) {
      }
    }
  }

  bool registerCallback(void (*cleanupFunc)()) {
    if (m_numberOfClient < MAX_CLIENT) {
      m_cleanupCallback[m_numberOfClient++] = cleanupFunc;
      return true;
    }
    return false;
  }
};

// Automatic stack variable that exits the process after
// a time specified in the environment.

class TimeBomb : public ACE_Task_Base {
 private:
  // UNUSED int m_numberOfClient;
  void (*m_cleanupCallback)();
  void callClientCleanup() {
    if (m_cleanupCallback != NULL) m_cleanupCallback();
  }

 public:
  ACE_Time_Value m_sleep;

  TimeBomb(void (*cleanupFunc)() = NULL)
      : m_sleep(0) /* UNUSED , m_numberOfClient( -1 )*/
  {
    char* sleepEnv = ACE_OS::getenv("TIMEBOMB");
    if (sleepEnv != NULL) {
      m_sleep.sec(atol(sleepEnv));
    }
    m_cleanupCallback = cleanupFunc;
    arm();  // starting
  }

  int arm() {
    int thrAttrs = THR_NEW_LWP | THR_DETACHED;
#ifndef WIN32
    thrAttrs |= THR_INHERIT_SCHED;
#endif
    return activate(thrAttrs, 1);
  }

  int svc() {
    if (m_sleep == ACE_Time_Value(0)) {
      printf("###### TIMEBOMB Disabled. ######\n");
      fflush(stdout);
      return 0;
    }
    ACE_Time_Value start = ACE_OS::gettimeofday();
    ACE_Time_Value now = ACE_OS::gettimeofday();
    do {
      ACE_OS::sleep(1);
      now = ACE_OS::gettimeofday();
    } while (now - start < m_sleep);
    printf("####### ERROR: TIMEBOMB WENT OFF, TEST TIMED OUT ########\n");
    fflush(stdout);

    callClientCleanup();

    exit(-1);
    return 0;
  }

  ~TimeBomb() {}
};

#endif
