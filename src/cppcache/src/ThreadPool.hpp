/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * ThreadPool.hpp
 *
 *  Created on: 16-Mar-2010
 *      Author: ankurs
 */

#ifndef THREADPOOL_HPP_
#define THREADPOOL_HPP_

#include <ace/Task.h>
#include <ace/Method_Request.h>
//#include <ace/Future.h>
#include <ace/Activation_Queue.h>
#include <ace/Condition_T.h>
#include <ace/Singleton.h>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Guard_T.h>

namespace gemfire {

template <class T>
class PooledWork : public ACE_Method_Request {
 private:
  // ACE_Future<T> result_;
  T m_retVal;
  ACE_Recursive_Thread_Mutex m_mutex;
  ACE_Condition<ACE_Recursive_Thread_Mutex> m_cond;
  bool m_done;

 public:
  PooledWork() : m_mutex(), m_cond(m_mutex), m_done(false) {}

  virtual ~PooledWork() {}

  virtual int call(void) {
    T res = execute();

    ACE_Guard<ACE_Recursive_Thread_Mutex> sync(m_mutex);

    m_retVal = res;
    m_done = true;
    m_cond.broadcast();
    // result_.set(res);
    return 0;
  }

  /*
  void attach(ACE_Future_Observer<T> *cb) {
    result_.attach(cb);
  }
  */

  T getResult(void) {
    ACE_Guard<ACE_Recursive_Thread_Mutex> sync(m_mutex);

    while (!m_done) {
      m_cond.wait();
    }
    // T res;
    // result_.get(res);
    return m_retVal;
  }

 protected:
  virtual T execute(void) = 0;
};

template <typename S, typename R = int>
class PooledWorkFP : public PooledWork<R> {
 public:
  typedef R (S::*OPERATION)(void);
  PooledWorkFP(S* op_handler, OPERATION op)
      : op_handler_(op_handler), m_op(op) {}
  virtual ~PooledWorkFP() {}

 protected:
  virtual R execute(void) { return (this->op_handler_->*m_op)(); }

 private:
  S* op_handler_;
  OPERATION m_op;
};

class ThreadPoolWorker;

class IThreadPool {
 public:
  virtual int returnToWork(ThreadPoolWorker* worker) = 0;
  virtual ~IThreadPool() {}
};

class ThreadPoolWorker : public ACE_Task<ACE_MT_SYNCH> {
 public:
  ThreadPoolWorker(IThreadPool* manager);
  virtual ~ThreadPoolWorker();
  int perform(ACE_Method_Request* req);
  int shutDown(void);

  virtual int svc(void);
  ACE_thread_t threadId(void);

 private:
  IThreadPool* manager_;
  ACE_thread_t threadId_;
  ACE_Activation_Queue queue_;
  int shutdown_;
};

class ThreadPool : public ACE_Task_Base, IThreadPool {
  friend class ACE_Singleton<ThreadPool, ACE_Recursive_Thread_Mutex>;

 public:
  int perform(ACE_Method_Request* req);
  int svc(void);
  int shutDown(void);
  virtual int returnToWork(ThreadPoolWorker* worker);

 private:
  ThreadPool();
  virtual ~ThreadPool();

  ThreadPoolWorker* chooseWorker(void);
  int createWorkerPool(void);
  int done(void);
  ACE_thread_t threadId(ThreadPoolWorker* worker);

 private:
  int poolSize_;
  int shutdown_;
  ACE_Thread_Mutex workersLock_;
  ACE_Condition<ACE_Thread_Mutex> workersCond_;
  ACE_Unbounded_Queue<ThreadPoolWorker*> workers_;
  ACE_Activation_Queue queue_;
  static const char* NC_Pool_Thread;
};

typedef ACE_Singleton<ThreadPool, ACE_Recursive_Thread_Mutex> TPSingleton;
}

#endif /* THREADPOOL_HPP_ */
