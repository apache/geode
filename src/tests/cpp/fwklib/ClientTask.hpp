/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#ifndef __ClientTask_hpp__
#define __ClientTask_hpp__

#include <gfcpp/GemfireCppCache.hpp>
#include <AtomicInc.hpp>
#include "fwklib/PerfFwk.hpp"
#include "fwklib/FwkObjects.hpp"

#include <string>

namespace gemfire {
namespace testframework {

class ClientTask {
 private:
  AtomicInc m_TotalIters;
  AtomicInc m_passCount;
  AtomicInc m_failCount;

 protected:
  bool m_Exit;
  bool m_Run;
  int32_t m_Iterations;
  int32_t m_Loop;

 public:
  ClientTask(int32_t iterations = 0)
      : m_Exit(false), m_Run(true), m_Iterations(iterations), m_Loop(-1) {}

  virtual ~ClientTask() {}

  void initTask() {
    m_Exit = false;
    m_Run = true;
    m_Iterations = 0;
    m_Loop = -1;
    m_TotalIters.resetValue(0);
  }

  void passed() { m_passCount++; }
  void failed() { m_failCount++; }
  int32_t getPassCount() { return m_passCount.value(); }
  int32_t getFailCount() { return m_failCount.value(); }

  // Defined by subclasses to implement the task functionality.
  // The id parameter is intended to be the thread id,
  // for informational purposes such as logging within doTask().
  // The return value should be the actual number of iterations performed,
  // allowing performance measurments of ops/sec.
  virtual uint32_t doTask(ACE_thread_t id) = 0;

  // Defined by subclasses to implement functionality that should be
  // performed prior to running the task. Used to remove actions that
  // should not be a part of a performance measurement,
  // but need to be performed before the task.
  // The id parameter is intended to be the thread id,
  // for informational purposes such as logging within doTask().
  //
  // This should return true, if it is OK to execute doTask() and doCleanup().
  virtual bool doSetup(ACE_thread_t id) = 0;

  // Defined by subclasses to implement functionality that should be
  // performed prior to running the task. Used to remove actions that
  // should not be a part of a performance measurement,
  // but need to be performed after the task.
  // The id parameter is intended to be the thread id,
  // for informational purposes such as logging within doTask().
  virtual void doCleanup(ACE_thread_t id) = 0;

  // Add iterations to the total.
  void addIters(int32_t iters) { m_TotalIters += iters; }

  // Get total iterations.
  int32_t getIters() { return m_TotalIters.value(); }

  // Set the number of iterations the test should do.
  void setIterations(int32_t iterations) {
    m_Iterations = iterations;
    m_TotalIters = AtomicInc();
  }

  // Used to terminate tasks being run by threads
  // m_Run should be used to control execution loop in doTask(),
  // possibly in conjuction with m_Iterations
  void endRun() { m_Run = false; }

  // used to ask threads to exit
  bool mustExit() { return m_Exit; }
};

class ExitTask : public ClientTask {
 public:
  ExitTask() { m_Exit = true; }
  bool doSetup(ACE_thread_t id) {
    id = 0;
    return true;
  }
  uint32_t doTask(ACE_thread_t id) {
    id = 0;
    return 0;
  }
  void doCleanup(ACE_thread_t id) { id = 0; }
};

class ThreadedTask : public ClientTask {
  FwkAction m_func;
  std::string m_args;
  // UNUSED const char * m_file;
  // UNUSED const char * m_class;
  // UNUSED const char * m_method;

 public:
  ThreadedTask(FwkAction func, std::string args) : m_func(func), m_args(args) {}

  ThreadedTask(FwkAction func, std::string args, const char* file,
               const char* className, const char* method)
      : m_func(func), m_args(args) {}

  uint32_t doTask(ACE_thread_t id) {
    id = 0;
    int32_t result = m_func(m_args.c_str());
    if (result != FWK_SUCCESS) {
      failed();
    } else {
      passed();
    }
    return 0;
  }

  bool doSetup(ACE_thread_t id) {
    id = 0;
    return true;
  }
  void doCleanup(ACE_thread_t id) { id = 0; }
};

}  // testframework
}  // gemfire

#endif  // __ClientTask_hpp__
