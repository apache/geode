/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "config.h"
#include <gfcpp/gfcpp_globals.hpp>
#include "StackTrace.hpp"
#include <string>

// -------- Windows ----------

#if defined(_WINDOWS)
#include <Windows.h>
#include <DbgHelp.h>
#include "NonCopyable.hpp"
#include <ace/OS.h>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Guard_T.h>

namespace gemfire {
ACE_Recursive_Thread_Mutex lockFrame;
StackTrace::StackTrace() {
  addFrame(m_frames);
  m_size = m_frames.size();
}

StackTrace::~StackTrace() {}

void StackTrace::print() const {
  for (std::list<std::string>::const_iterator itr = m_frames.begin();
       itr != m_frames.end(); ++itr) {
    printf("%s\n", (*itr).c_str());
  }
}

void StackTrace::getString(std::string& tracestring) const {
  for (std::list<std::string>::const_iterator itr = m_frames.begin();
       itr != m_frames.end(); ++itr) {
    tracestring += *itr;
    tracestring += "\n";
  }
}

void StackTrace::addFrame(std::list<std::string>& frames) {
  ACE_Guard<ACE_Recursive_Thread_Mutex> guard(lockFrame);
  unsigned int i;
  void* stack[100];
  unsigned short framesCounts;
  SYMBOL_INFO* symbol;
  HANDLE process;

  process = GetCurrentProcess();
  SymInitialize(process, NULL, TRUE);

  typedef USHORT(WINAPI * CaptureStackBackTraceType)(
      __in ULONG, __in ULONG, __out PVOID*, __out_opt PULONG);
  CaptureStackBackTraceType func = (CaptureStackBackTraceType)(
      GetProcAddress(LoadLibrary("kernel32.dll"), "RtlCaptureStackBackTrace"));

  if (!func) {
    return;
  }

  framesCounts = func(0, 62, stack, NULL);
  symbol = (SYMBOL_INFO*)calloc(sizeof(SYMBOL_INFO) + 256 * sizeof(char), 1);
  symbol->MaxNameLen = 255;
  symbol->SizeOfStruct = sizeof(SYMBOL_INFO);

  for (i = 0; i < framesCounts; i++) {
    SymFromAddr(process, (DWORD64)(stack[i]), 0, symbol);
    m_frames.push_back(symbol->Name);
  }
  free(symbol);
}
}

#elif defined(_LINUX)
#include <execinfo.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>

namespace gemfire {

StackTrace::StackTrace() {
  void* nativetrace_array[GF_TRACE_LEN];
  m_size = backtrace(nativetrace_array, GF_TRACE_LEN);
  char** symbols = backtrace_symbols(nativetrace_array, m_size);
  for (int i = 0; i < m_size; i++) {
    addFrame(symbols[i], i);
  }
  free(symbols);
}
}
#elif defined(_SOLARIS)
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <ucontext.h>

namespace gemfire {
StackTrace::StackTrace() {
  void* nativetrace_array[GF_TRACE_LEN];
  // NOT YET IMPLEMENTED
  /*
  ucontext_t *context;
  if ( getcontext(context) ) {
  }
  */
}
}
#elif defined(_MACOSX)
#include <execinfo.h>

namespace gemfire {
StackTrace::StackTrace() {
  void* nativetrace_array[GF_TRACE_LEN];
  m_size = backtrace(nativetrace_array, GF_TRACE_LEN);
  char** symbols = backtrace_symbols(nativetrace_array, m_size);
  for (int i = 0; i < m_size; i++) {
    addFrame(symbols[i], i);
  }
  free(symbols);
}
}  // namespace gemfire
#else
#error porting error
#endif

#ifndef _WIN32
namespace gemfire {
StackTrace::~StackTrace() {}
void StackTrace::addFrame(const char* line, int i) { m_frames[i].init(line); }

void StackTrace::print() const {
#ifdef _SOLARIS
  printstack(fileno(stdout));
#else
  std::string tracestring;
  bool last = false;
  int start = 2;
  StackTrace* self = const_cast<StackTrace*>(this);
  for (int i = start; (!last) && (i < m_size); i++) {
    tracestring += "    ";
    tracestring += self->m_frames[i].asString();
    tracestring += "\n";
    if (0 == strcmp("main", m_frames[i].m_symbol)) {
      last = true;
    }
  }
  printf("%s", tracestring.c_str());
#endif
}

#ifndef _SOLARIS
void StackTrace::getString(std::string& tracestring) const {
  bool last = false;
  int start = 2;
  StackTrace* self = const_cast<StackTrace*>(this);
  for (int i = start; (!last) && (i < m_size); i++) {
    tracestring += "    ";
    tracestring += self->m_frames[i].asString();
    tracestring += "\n";
    if (0 == strcmp("main", m_frames[i].m_symbol)) {
      last = true;
    }
  }
}
#endif
}  // namespace gemfire
#endif
