/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GEMFIRE_STACKFRAME_HPP_
#define _GEMFIRE_STACKFRAME_HPP_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <ace/OS.h>

#ifdef _WIN32
#include <Windows.h>
#include <DbgHelp.h>
#else
// Since we dont compile with g++ on Sun
#ifdef _SOLARIS
#define DEMANGLED_SYMBOL_BUFSIZE 512
#include <demangle.h>
#else
#include <exception>
#include <cxxabi.h>
#endif
#endif

namespace gemfire {

class StackFrame {
 public:
  char m_offset[12];
  char m_symbol[256];
  char m_module[700];
  char m_string[1024];

  StackFrame() {}
  char* asString();
  ~StackFrame() {}

#ifdef _WIN32
#ifndef _WIN64
  // ------------ WINDOWS -----------

  void init(const char* btline) {
    m_offset[0] = '\0';
    m_symbol[0] = '\0';
    m_module[0] = '\0';
    if (!UnDecorateSymbolName(btline, m_string, 1023, UNDNAME_COMPLETE)) {
      strncpy(m_string, btline, 1023);
    }
    m_string[1023] = '\0';
  }
#endif
#else
  // ------------ UNIX -----------

  void init(const char* btline) {
    m_string[0] = 0;
    int status = 0;
    char* symbol = extractSymbol(btline);
    char* demangled = NULL;
#ifdef _SOLARIS
    demangled = new char[DEMANGLED_SYMBOL_BUFSIZE];
    status = cplus_demangle(symbol, demangled, DEMANGLED_SYMBOL_BUFSIZE);
#else
    demangled = abi::__cxa_demangle(symbol, 0, 0, &status);
#endif
    if (status != 0) {
      ACE_OS::strncpy(m_symbol, symbol, sizeof(m_symbol));
    } else {
      ACE_OS::strncpy(m_symbol, demangled, sizeof(m_symbol));
    }
#ifdef _SOLARIS
    GF_SAFE_DELETE_ARRAY(demangled);
#else
    free(demangled);
#endif
    delete[] symbol;
  }

#endif
  // ------------ ALL -----------

  char* extractSymbol(const char* btline) {
    /*
    This code receives a stacktrace line in the forms:

    /export/hoth2/framework/lib/debug/libeventtest.so(_ZN7gemfire13testframework9EventTest17doEventOperationsEv+0x4a0)
    [0x93a1a0]
    /export/hoth2/framework/lib/debug/libeventtest.so(doRegionOperations+0x13d)
    [0x92c9e3]
    Client [0x804ae56]
    Client [0x804c9ca]
    /lib/tls/libc.so.6(__libc_start_main+0xda) [0xe1c79a]
    Client(_Znwj+0x41) [0x804a6ed]

    Sets members: m_module and m_offset
    returns the symbol

    */

    size_t btlen = strlen(btline);
    char* symbol =
        new char[btlen + 1];  // COVERTY --> 30298 Out-of-bounds access
    symbol[0] = '\0';
    m_module[0] = '\0';
    m_offset[0] = '\0';

    if (strchr(btline, '+')) {
      sscanf(btline, "%[^(](%[^+]+%[^)])", m_module, symbol, m_offset);
    } else {
      sscanf(btline, "%[^ ] [%[x01-9a-f]]", m_module, m_offset);
    }
    char* shortmodule = NULL;
    if (NULL != (shortmodule = strrchr(m_module, '/'))) {
      char* tmp =
          new char[btlen + 1];  // COVERTY --> 30298 Out-of-bounds access
      ACE_OS::strncpy(tmp, shortmodule + 1, btlen);
      ACE_OS::strncpy(m_module, tmp, btlen);
      delete[] tmp;
    }
    return symbol;
  }
};
}

#endif /* _GEMFIRE_STACKFRAME_HPP_ */
