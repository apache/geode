/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#ifndef __PerfFwk_hpp__
#define __PerfFwk_hpp__

#include <gfcpp/gf_base.hpp>

#include <string>
#include <map>

#include "AtomicInc.hpp"
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
#include "FwkException.hpp"

// kludge to compile on bar
#define _CPP_CMATH 1
#include <sstream>
#include <iostream>
#include <iomanip>

#include <ace/ACE.h>
#include <ace/OS.h>
#include <ace/Task.h>
#include <ace/Condition_T.h>
#include <ace/Thread_Mutex.h>

namespace gemfire {
namespace testframework {
namespace perf {

void logSize(const char* tag = NULL);

inline void sleepSeconds(int32_t secs) {
  ACE_Time_Value sleepTime(secs, 0);
  ACE_OS::sleep(sleepTime);
}

inline void sleepMillis(int32_t millis) {
  ACE_Time_Value sleepTime((millis / 1000), ((millis % 1000) * 1000));
  ACE_OS::sleep(sleepTime);
}

inline void sleepMicros(int32_t micros) {
  ACE_Time_Value sleepTime((micros / 1000000), (micros % 1000000));
  ACE_OS::sleep(sleepTime);
}

class Semaphore {
 private:
  ACE_Thread_Mutex m_mutex;
  ACE_Condition<ACE_Thread_Mutex> m_cond;
  volatile int32_t m_count;

 public:
  Semaphore() : m_mutex(), m_cond(m_mutex), m_count(0) {}
  Semaphore(int32_t count) : m_mutex(), m_cond(m_mutex), m_count(count) {}
  //  Semaphore( const Semaphore& other ) : m_mutex(), m_cond( m_mutex ),
  //    m_count( other.getCount() ) {}

  //  Semaphore& operator=( const Semaphore& other ) {
  //    m_count = other.getCount();
  //    return *this;
  //  }

  ~Semaphore() {}

  bool acquire(ACE_Time_Value* until, int32_t t = 1);
  void acquire(int32_t t = 1);
  void release(int32_t t = 1);
  int32_t getCount() const { return m_count; }
};

class Record {
 private:
  char* m_testName;
  char* m_action;
  char* m_runDate;
  uint32_t m_numKeys;
  uint32_t m_numClients;
  uint32_t m_valueSize;
  uint32_t m_numThreads;
  uint32_t m_operations;
  uint32_t m_micros;

 public:
  //    inline Record( const char * testName, uint32_t ops, uint32_t micros )
  //    : m_testName( strdup(testName) ),
  //      m_operations( ops ),
  //      m_micros( micros )
  //    {
  //    }
  //
  //    inline Record( const char * testName, uint32_t ops, uint32_t micros )
  //    : m_testName( strdup(testName) ),
  //      m_operations( ops ),
  //      m_micros( micros )
  //    {
  //    }

  inline Record()
      : m_testName(NULL),
        m_action(NULL),
        m_runDate(NULL),
        m_numKeys(0),
        m_numClients(0),
        m_valueSize(0),
        m_numThreads(0),
        m_operations(0),
        m_micros(0) {}

  //    inline Record( const Record& other )
  //    : m_testName( other.m_testName ),
  //      m_operations( other.m_operations ),
  //      m_micros( other.m_micros )
  //    {
  //    }

  inline ~Record() {
    if (m_testName != NULL) free(m_testName);
    if (m_action != NULL) free(m_action);
    if (m_runDate != NULL) free(m_runDate);
    m_testName = NULL;
    m_action = NULL;
    m_runDate = NULL;
  }

  void setName(const char* name) {
    if (m_testName != NULL) free(m_testName);
    if (name != NULL)
      m_testName = strdup(name);
    else
      m_testName = NULL;
  }

  void setAction(const char* action) {
    if (m_action != NULL) free(m_action);
    if (action != NULL)
      m_action = strdup(action);
    else
      m_action = NULL;
  }

  void setDate(const char* date) {
    if (m_runDate != NULL) free(m_runDate);
    if (date != NULL)
      m_runDate = strdup(date);
    else
      m_runDate = NULL;
  }

  void setDate() {
    if (m_runDate == NULL) m_runDate = (char*)malloc(32);

    time_t esecs = time(0);
    struct tm* now = localtime(&esecs);
    sprintf(m_runDate, "%d/%d/%d %d:%d:%d", now->tm_mon + 1, now->tm_mday,
            now->tm_year + 1900, now->tm_hour, now->tm_min, now->tm_sec);
  }

  void setNumKeys(uint32_t num) { m_numKeys = num; }
  void setNumClients(uint32_t num) { m_numClients = num; }
  void setValueSize(uint32_t num) { m_valueSize = num; }
  void setNumThreads(uint32_t num) { m_numThreads = num; }
  void setOperations(uint32_t num) { m_operations = num; }
  void setMicros(uint32_t num) { m_micros = num; }

  //    inline Record& operator=( const Record& other )
  //    {
  //      m_testName = other.m_testName;
  //      m_operations = other.m_operations;
  //      m_micros = other.m_micros;
  //      return *this;
  //    }
  //
  inline void write(gemfire::DataOutput& output) {
    output.writeASCII(m_testName);
    output.writeInt((int32_t)m_operations);
    output.writeInt((int32_t)m_micros);
  }

  inline void read(gemfire::DataInput& input) {
    input.readASCII(&m_testName);
    input.readInt((int32_t*)&m_operations);
    input.readInt((int32_t*)&m_micros);
  }

  inline std::string perSec() {
    double ps = ((1000000.0 * (double)m_operations) / (double)m_micros);
    std::ostringstream oss;
    if (ps < 10.0) {
      oss.precision(2);
      oss << ps;
    } else {
      oss << ((int32_t)(ps + .5));
    }
    return oss.str();
    //      return (int32_t) ((((double) 1000000 * m_operations) / (double)
    //      m_micros ) + 0.5);
  }

  inline std::string asTag() {
    std::stringstream result;
    result << m_testName << "-" << m_action << "-Clients-" << m_numClients
           << "-Keys-" << m_numKeys << "-VSize-" << m_valueSize << "-Threads-"
           << m_numThreads;
    return result.str();
  }
  inline std::string asString() {
    std::stringstream result;
    result << asTag() << " -- " << perSec() << " ops/sec, " << m_operations
           << " ops, " << m_micros << " micros";
    return result.str();
  }
  inline std::string asCSV() {
    setDate();
    std::stringstream result;
    result << m_testName << "," << m_action << "," << m_numClients << ","
           << m_numKeys << "," << m_valueSize << "," << m_numThreads << ","
           << perSec() << "," << m_operations << "," << m_micros << ","
           << m_runDate;
    return result.str();
  }
};

typedef std::map<const char*, Record> RecordMap;

class PerfSuite {
 private:
  Record m_current;
  std::stringstream m_summary;
  static ACE_utsname* utsname;

 public:
  PerfSuite();

  static inline const char* getSysName() {
    if (utsname == NULL) {
      utsname = new ACE_utsname();
      ACE_OS::uname(utsname);
    }
    return utsname->sysname;
  }
  static inline const char* getNodeName() {
    if (utsname == NULL) {
      utsname = new ACE_utsname();
      ACE_OS::uname(utsname);
    }
    return utsname->nodename;
  }
  static inline const char* getRelease() {
    if (utsname == NULL) {
      utsname = new ACE_utsname();
      ACE_OS::uname(utsname);
    }
    return utsname->release;
  }
  static inline const char* getVersion() {
    if (utsname == NULL) {
      utsname = new ACE_utsname();
      ACE_OS::uname(utsname);
    }
    return utsname->version;
  }
  static inline const char* getMachine() {
    if (utsname == NULL) {
      utsname = new ACE_utsname();
      ACE_OS::uname(utsname);
    }
    return utsname->machine;
  }

  void setName(const char* name) { m_current.setName(name); }
  void setAction(const char* action) { m_current.setAction(action); }
  void setDate(const char* date) { m_current.setDate(date); }
  void setNumKeys(uint32_t num) { m_current.setNumKeys(num); }
  void setNumClients(uint32_t num) { m_current.setNumClients(num); }
  void setValueSize(uint32_t num) { m_current.setValueSize(num); }
  void setNumThreads(uint32_t num) { m_current.setNumThreads(num); }
  void setOperations(uint32_t num) { m_current.setOperations(num); }
  void setMicros(uint32_t num) { m_current.setMicros(num); }

  void addRecord(uint32_t ops, uint32_t micros);

  std::string asString() { return m_current.asTag(); }
  std::string summary() { return m_summary.str(); }

  //    /** create a file in cwd, named "<suite>_results.<host>" */
  //     void save( );
  //
  //    /** load data saved in $ENV{'baselines'} named "<suite>_baseline.<host>"
  //     *  A non-favorable comparison will throw an FwkException.
  //     */
  //     void compare( );
};

class Counter {
 public:
  Counter() : m_count(0) {}
  void set(int32_t val) { m_count = val; }
  void add(int32_t val) { m_count += val; }
  int32_t value() { return m_count; }
  void zero() { m_count = 0; }

 private:
  int32_t m_count;
};

}  // namespace perf
}  // namespace testframework
}  // namespace gemfire

#endif  // __PerfFwk_hpp__
