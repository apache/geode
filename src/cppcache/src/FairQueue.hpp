/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_FAIRQUEUE_HPP_
#define _GEMFIRE_FAIRQUEUE_HPP_

#include <deque>
#include <ace/ACE.h>
#include <ace/Thread_Mutex.h>
#include <ace/Token.h>
#include <ace/Condition_T.h>
#include <ace/Time_Value.h>
#include <ace/Guard_T.h>

namespace gemfire {

template <class T, class MUTEX = ACE_Thread_Mutex>
class FairQueue {
 public:
  FairQueue() : m_cond(m_queueLock), m_closed(false) {}

  virtual ~FairQueue() { close(); }

  int waiters() { return m_queueGetLock.waiters(); }

  /** get without wait */
  T* getNoWait() {
    bool isClosed;
    T* mp = getNoGetLock(isClosed);
    return mp;
  }

  /** wait sec time until notified */
  T* getUntil(int64_t& sec) {
    bool isClosed;
    T* mp = getNoGetLock(isClosed);

    if (mp == NULL && !isClosed) {
      mp = getUntilWithToken(sec, isClosed, (void*)NULL);
    }
    return mp;
  }

  void put(T* mp, bool openQueue) {
    GF_DEV_ASSERT(mp != 0);

    bool delMp = false;
    {
      ACE_Guard<MUTEX> _guard(m_queueLock);
      if (openQueue || !m_closed) {
        m_queue.push_front(mp);
        m_closed = false;
        m_cond.signal();
      } else {
        delMp = true;
      }
    }
    if (delMp) {
      mp->close();
      delete mp;
    }
  }

  uint32_t size() {
    ACE_Guard<MUTEX> _guard(m_queueLock);

    return static_cast<uint32_t>(m_queue.size());
  }

  bool empty() { return (size() == 0); }

  void close() {
    ACE_Guard<MUTEX> _guard(m_queueLock);

    m_closed = true;
    LOGDEBUG("Internal fair queue size while closing is %d", m_queue.size());
    while (m_queue.size() > 0) {
      T* mp = m_queue.back();
      m_queue.pop_back();
      mp->close();
      delete mp;
      deleteAction();
    }
    LOGDEBUG("FairQueue::close( ): queue closed ");
    m_cond.signal();
  }

  void reset() {
    ACE_Guard<MUTEX> _guard(m_queueLock);

    m_closed = false;
  }

  MUTEX& getQueueLock() { return m_queueLock; }

 private:
  T* getNoGetLock(bool& isClosed) {
    ACE_Guard<MUTEX> _guard(m_queueLock);

    return popFromQueue(isClosed);
  }

  ACE_Condition<MUTEX> m_cond;
  ACE_Token m_queueGetLock;
  bool m_closed;

  bool exclude(T* mp, void*) { return false; }

 protected:
  std::deque<T*> m_queue;
  MUTEX m_queueLock;

  inline T* popFromQueue(bool& isClosed) {
    T* mp = NULL;

    isClosed = m_closed;
    if (!isClosed && m_queue.size() > 0) {
      mp = m_queue.back();
      m_queue.pop_back();
    }
    return mp;
  }

  template <typename U>
  T* getUntilWithToken(int64_t& sec, bool& isClosed, U* excludeList = NULL) {
    T* mp = NULL;

    ACE_Guard<ACE_Token> _guard(m_queueGetLock);

    ACE_Time_Value currTime(ACE_OS::gettimeofday());
    ACE_Time_Value stopAt(currTime);
    stopAt += sec;

    ACE_Guard<MUTEX> _guard1(m_queueLock);

    if (!m_closed) {
      do {
        m_cond.wait(&stopAt);
        mp = popFromQueue(isClosed);
        if (mp && excludeList) {
          if (exclude(mp, excludeList)) {
            mp->close();
            GF_SAFE_DELETE(mp);
            deleteAction();
          }
        }
      } while (mp == NULL && (currTime = ACE_OS::gettimeofday()) < stopAt &&
               !isClosed);
      sec = (stopAt - currTime).sec();
    }

    return mp;
  }

  virtual void deleteAction() {}
};

}  // end namespace

#endif  // ifndef _GEMFIRE_FAIRQUEUE_HPP_
