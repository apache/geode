/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _FWPERF_H_
#define _FWPERF_H_

/**

fw_perf.hpp provides framework macros for measuring performance,
archiving the results, and comparing against previous archives.

Steps for a performance suite:

perf::PerfSuite suite( "SuiteName" );

const char* name = "my operation type/test description";
perf::TimeStamp starttime;
... perform n operations.
perf::TimeStamp stoptime;
suite.addRecord( name, opcount, starttime, stoptime );

suite.save( );    // will save current results to <suite>_results.<hostname>
suite.compare( ); // will compare against the file named
<suite>_baseline.<hostname>

If no baseline file for the host is available, then an error will occur,
recommending
that the results be analyzed for acceptability, and checked in as the hosts
baseline.

If a baseline is found, a comparison report is generated, if the deviation is
beyond
established limits, a TestException will be thrown.

*/

#include <string>
#include <map>

#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>

#include <ace/Task.h>
#include <ace/Condition_T.h>
#include <ace/Thread_Mutex.h>
//#include "Name_Handler.h"

namespace perf {

class Semaphore {
 private:
  ACE_Thread_Mutex m_mutex;
  ACE_Condition<ACE_Thread_Mutex> m_cond;
  volatile int m_count;

 public:
  Semaphore(int count);
  ~Semaphore();
  void acquire(int t = 1);
  void release(int t = 1);

 private:
  Semaphore();
  Semaphore(const Semaphore& other);
  Semaphore& operator=(const Semaphore& other);
};

class TimeStamp {
 private:
  int64_t m_msec;

 public:
  TimeStamp();
  TimeStamp(const TimeStamp& other);
  TimeStamp(int64_t msec);
  TimeStamp& operator=(const TimeStamp& other);

  ~TimeStamp();

  int64_t msec() const;
  void msec(int64_t t);
};

class Record {
 private:
  std::string m_testName;
  int64_t m_operations;
  TimeStamp m_startTime;
  TimeStamp m_stopTime;

 public:
  Record(std::string testName, const long ops, const TimeStamp& start,
         const TimeStamp& stop);

  Record();

  Record(const Record& other);

  Record& operator=(const Record& other);

  void write(gemfire::DataOutput& output);

  void read(gemfire::DataInput& input);

  int elapsed();
  int perSec();
  std::string asString();

  ~Record();
};

typedef std::map<std::string, Record> RecordMap;

class PerfSuite {
 private:
  std::string m_suiteName;
  RecordMap m_records;

 public:
  PerfSuite(const char* suiteName);

  void addRecord(std::string testName, const long ops, const TimeStamp& start,
                 const TimeStamp& stop);

  /** create a file in cwd, named "<suite>_results.<host>" */
  void save();

  /** load data saved in $ENV{'baselines'} named "<suite>_baseline.<host>"
   *  A non-favorable comparison will throw an TestException.
   */
  void compare();
};

class Thread;

class ThreadLauncher {
 private:
  int m_thrCount;
  Semaphore m_initSemaphore;
  Semaphore m_startSemaphore;
  Semaphore m_stopSemaphore;
  Semaphore m_cleanSemaphore;
  Semaphore m_termSemaphore;
  TimeStamp* m_startTime;
  TimeStamp* m_stopTime;
  Thread& m_threadDef;

 public:
  ThreadLauncher(int thrCount, Thread& thr);

  void go();

  ~ThreadLauncher();

  Semaphore& initSemaphore() { return m_initSemaphore; }

  Semaphore& startSemaphore() { return m_startSemaphore; }

  Semaphore& stopSemaphore() { return m_stopSemaphore; }

  Semaphore& cleanSemaphore() { return m_cleanSemaphore; }

  Semaphore& termSemaphore() { return m_termSemaphore; }

  TimeStamp startTime() { return *m_startTime; }

  TimeStamp stopTime() { return *m_stopTime; }

 private:
  ThreadLauncher& operator=(const ThreadLauncher& other);
  ThreadLauncher(const ThreadLauncher& other);
};

class Thread : public ACE_Task_Base {
 private:
  ThreadLauncher* m_launcher;
  bool m_used;

 public:
  Thread();
  // Unhide function to prevent SunPro Warnings
  using ACE_Shared_Object::init;
  void init(ThreadLauncher* l) {
    ASSERT(!m_used, "Cannot reliably reuse Thread.");
    m_launcher = l;
  }

  ~Thread();

  /** called before measurement begins. override to do per thread setup. */
  virtual void setup() {}

  /** run during measurement */
  virtual void perftask() = 0;

  /** called after measurement to clean up what might have been setup in setup..
   */
  virtual void cleanup() {}

  virtual int svc();
};

// class NamingServiceThread
//: public ACE_Task_Base
//{
//  private:
//  uint32_t m_port;
//
//  void namingService()
//  {
//    char * argsv[2];
//    char pbuf[32];
//    sprintf( pbuf, "-p %d", 12321 );
//
//    argsv[0] = strdup( pbuf );
//    argsv[1] = 0;
//    ACE_Service_Object_Ptr svcObj = ACE_SVC_INVOKE( ACE_Name_Acceptor );
//
//    if ( svcObj->init( 1, argsv ) == -1 ) {
//      fprintf( stdout, "Failed to construct the Naming Service." );
//      fflush( stdout );
//    }
//      ACE_Reactor::run_event_loop();
//  }
//
//  public:
//  NamingServiceThread( uint32_t port ) : m_port( port ) {}
// virtual int svc() { };//namingService(); }
//};
}

#endif
