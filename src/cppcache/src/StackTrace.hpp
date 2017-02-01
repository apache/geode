#pragma once

#ifndef GEODE_STACKTRACE_H_
#define GEODE_STACKTRACE_H_

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define GF_TRACE_LEN 25

#include "StackFrame.hpp"
#include <gfcpp/SharedPtr.hpp>
#include <string>
#include <list>

namespace apache {
namespace geode {
namespace client {

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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_STACKTRACE_H_
