/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/
#include "PerfFwk.hpp"
#include "fwklib/FwkLog.hpp"
#include "ace/OS.h"
#include <signal.h>

using namespace gemfire;
using namespace gemfire::testframework;
using namespace gemfire::testframework::perf;

ACE_utsname* PerfSuite::utsname = NULL;

// void perf::logSignals( const char * tag ) {
//  if ( tag == NULL ) {
//    tag = "perf::logSignals";
//  }
//
//  struct sigaction sa;
//
//  int32_t blockedSignals = 0;
//  sigset_t set;
//  sigset_t old;
//  sigemptyset( &set );
//  sigemptyset( &old );
//  int32_t ret = sigprocmask( SIG_BLOCK, &set, &old );
//
//  for ( int32_t i = 1; i < 32; i++ ) {
//    if ( ret == 0 ) {
//      int32_t blocked = sigismember( &old, i );
//      if ( blocked == 1 ) {
//        blockedSignals++;
//        FWKINFO( tag << " Signal " << i << " is blocked." );
//      }
//    }
//    int32_t retVal = sigaction( i, 0, &sa );
//    if ( retVal == 0 ) {
//      if ( sa.sa_handler == SIG_DFL ) {
//        FWKINFO( tag << " 'Default' for signal " << i );
//      }
//      else
//      if ( sa.sa_handler == SIG_IGN ) {
//        FWKINFO( tag << " 'Ignore' for signal " << i );
//      }
//      else {
//        FWKINFO( tag << " 'Application specified' for signal " << i );
//      }
//    }
//  }
//  FWKINFO( tag << " Number of signals blocked: " << blockedSignals );
//}

// Not thread safe, not critical that it be
void perf::logSize(const char* tag) {
#ifndef WIN32
  static double vsizeMax = 0;
  static double rssMax = 0;
  uint32_t pid = ACE_OS::getpid();

  char procFileName[128];
  FILE* fil;

  sprintf(procFileName, "/proc/%u/status", pid);
  fil = fopen(procFileName, "rb"); /* read only */
  if (fil == (FILE*)0) {
    FWKINFO("Unable to read status file.");
    return;
  }

  uint32_t val = 0;
  double vs = -1;
  double rs = -1;
  char rbuff[1024];
  while (fgets(rbuff, 1023, fil) != 0) {
    if ((ACE_OS::strncasecmp(rbuff, "VmSize:", 7) == 0) &&
        (sscanf(rbuff, "%*s %u", &val) == 1)) {
      vs = static_cast<double>(val);
    } else if ((ACE_OS::strncasecmp(rbuff, "VmRSS:", 6) == 0) &&
               (sscanf(rbuff, "%*s %u", &val) == 1)) {
      rs = static_cast<double>(val);
    }
  }
  fclose(fil);
  vs /= 1024.0;
  rs /= 1024.0;
  char pbuf[1024];
  sprintf(pbuf, "Size of Process %u: Size: %.4f (%.4f) Mb  RSS: %.4f (%.4f) Mb",
          pid, vs, vsizeMax, rs, rssMax);
  if (tag == NULL) {
    FWKINFO(pbuf);
    FWKINFO(":CSV Size::::," << pid << "," << vs << "," << rs);
  } else {
    FWKINFO(pbuf << "  : " << tag);
    FWKINFO(tag << " : :CSV Size::::," << pid << "," << vs << "," << rs);
  }
  if (vs > vsizeMax) {
    vsizeMax = vs;
  }
  if (rs > rssMax) {
    rssMax = rs;
  }
#endif
}

// ========================================================================
PerfSuite::PerfSuite() {
  m_summary << "Performance Summary: \nTest, ops/sec , ops, microsec \n";
}

void PerfSuite::addRecord(uint32_t ops, uint32_t micros) {
  setOperations(ops);
  setMicros(micros);
  m_summary << m_current.asTag() << ", " << m_current.perSec() << ", " << ops
            << ", " << micros << "\n";
  FWKINFO("[PerfSuite] " << m_current.asString());
  FWKINFO("[PerfData]," << m_current.asCSV());
}

///** create a file in cwd, named "<suite>_results.<host>" */
// void PerfSuite::save( )
//{
//  gemfire::DataOutput output;
//  output.writeASCII( m_suiteName.c_str(), m_suiteName.length() );
//
//  char hname[100];
//  ACE_OS::hostname( hname, 100 );
//  std::string fname = m_suiteName + "_results." + hname;
//
//  output.writeASCII( hname );
//
//  for( RecordMap::iterator iter = m_records.begin(); iter != m_records.end();
//  iter++ ) {
//    Record record = (*iter).second;
//    record.write( output );
//  }
//  fprintf( stdout, "[PerfSuite] finished serializing results.\n" );
//  fflush( stdout );
//
//  fprintf( stdout, "[PerfSuite] writing results to %s\n", fname.c_str() );
//  FILE* of = ACE_OS::fopen( fname.c_str(), "a+b" );
//  if ( of == 0 ) {
//    throw FwkException( "Failed to open result file handle for PerfSuite." );
//  }
//  uint32_t len = 0;
//  char* buf = (char*) output.getBuffer( &len );
//  ACE_OS::fwrite( buf, len, 1, of );
//  ACE_OS::fflush( of );
//  ACE_OS::fclose( of );
//  fprintf( stdout, "[PerfSuite] finished saving results file %s\n",
//  fname.c_str() );
//  fflush( stdout );
//}
//
///** load data saved in $ENV{'baselines'} named "<suite>_baseline.<host>" */
// void PerfSuite::compare( )
//{
//  char hname[100];
//  ACE_OS::hostname( hname, 100 );
//  std::string fname = m_suiteName + "_baseline." + hname;
//}

bool Semaphore::acquire(ACE_Time_Value* until, int32_t t) {
  ACE_Guard<ACE_Thread_Mutex> _guard(m_mutex);

  while (m_count < t) {
    if (m_cond.wait(until) == -1) return false;
  }
  m_count -= t;
  return true;
}

void Semaphore::acquire(int32_t t) {
  ACE_Guard<ACE_Thread_Mutex> _guard(m_mutex);

  while (m_count < t) {
    m_cond.wait();
  }
  m_count -= t;
}

void Semaphore::release(int32_t t) {
  ACE_Guard<ACE_Thread_Mutex> _guard(m_mutex);

  m_count += t;
  if (m_count > 0) {
    m_cond.broadcast();
  }
}
