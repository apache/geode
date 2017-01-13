/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GEMFIRE_STACKTRACE_HPP_
#define _GEMFIRE_STACKTRACE_HPP_

#define GF_TRACE_LEN 25

#include "StackFrame.hpp"
#include <gfcpp/SharedPtr.hpp>
#include <string>
#include <list>

namespace gemfire {

class StackTrace;
typedef SharedPtr<StackTrace> StackTracePtr;
#ifdef _WINDOWS
class StackTrace : public SharedBase {
 public:
  StackTrace();
  virtual ~StackTrace();
  void print() const;
  void getString(std::string& tracestring) const;

  size_t m_size;
  void addFrame(std::list<std::string>& frames);

 private:
  std::list<std::string> m_frames;
};
#else
class StackTrace : public SharedBase {
  StackFrame m_frames[GF_TRACE_LEN];
  int m_size;

 public:
  StackTrace();
  virtual ~StackTrace();
  void print() const;
#ifndef _SOLARIS
  void getString(std::string& tracestring) const;
#endif
  void addFrame(const char* line, int i);
};
#endif
}

#endif
