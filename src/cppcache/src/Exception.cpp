/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

extern "C" {
#include <string.h>
#include <stdlib.h>
}
#include <ace/OS.h>

#include <gfcpp/Exception.hpp>
#include <gfcpp/CacheableString.hpp>
#include <StackTrace.hpp>
#include <ace/TSS_T.h>

#include <string>

namespace gemfire {

// globals can only be trusted to initialize to ZERO.
bool Exception::s_exceptionStackTraceEnabled = false;

void Exception::setStackTraces(bool stackTraceEnabled) {
  s_exceptionStackTraceEnabled = stackTraceEnabled;
}

Exception::Exception(const char* msg1, const char* msg2, bool forceTrace,
                     const ExceptionPtr& cause)
    : m_stack(), m_cause(cause) {
  size_t len1 = 0;
  if (msg1) {
    len1 = strlen(msg1);
  }
  size_t len2 = 0;
  if (msg2) {
    len2 = strlen(msg2);
  }
  size_t len = len1 + len2;
  char* msg;
  GF_NEW(msg, char[len + 1]);
  if (msg1) {
    ACE_OS::memcpy(msg, msg1, len1);
  }
  if (msg2) {
    ACE_OS::memcpy(msg + len1, msg2, len2);
  }
  msg[len] = '\0';

  if (s_exceptionStackTraceEnabled || forceTrace) {
    GF_NEW(m_stack, StackTrace());
  }
  m_message = CacheableString::createNoCopy(msg, static_cast<int32_t>(len));
}

Exception::~Exception() {}

const char _exception_name_Exception[] = "gemfire::Exception";

const char* Exception::getName() const { return _exception_name_Exception; }

const char* Exception::getMessage() const { return m_message->asChar(); }

void Exception::showMessage() const {
  printf("%s: msg = %s\n", this->getName(), m_message->asChar());
}

void Exception::printStackTrace() const {
  showMessage();
  if (m_stack == NULLPTR) {
    fprintf(stdout, "  No stack available.\n");
  } else {
    m_stack->print();
  }
  if (m_cause != NULLPTR) {
    fprintf(stdout, "Cause by exception: ");
    m_cause->printStackTrace();
  }
}

#ifndef _SOLARIS

size_t Exception::getStackTrace(char* buffer, size_t maxLength) const {
  size_t len = 0;
  if (maxLength > 0) {
    std::string traceString;
    if (m_stack == NULLPTR) {
      traceString = "  No stack available.\n";
    } else {
      m_stack->getString(traceString);
    }
    if (m_cause != NULLPTR) {
      traceString += "Cause by exception: ";
      m_cause->m_stack->getString(traceString);
    }
    len = ACE_OS::snprintf(buffer, maxLength, "%s", traceString.c_str());
  }
  return len;
}

#endif

Exception::Exception(const Exception& other)
    : m_message(other.m_message),
      m_stack(other.m_stack),
      m_cause(other.m_cause) {}

Exception::Exception(const CacheableStringPtr& message,
                     const StackTracePtr& stack, const ExceptionPtr& cause)
    : m_message(message), m_stack(stack), m_cause(cause) {}

Exception* Exception::clone() const {
  return new Exception(m_message, m_stack, m_cause);
}

// class to store/clear last server exception in TSS area

class TSSExceptionString {
 private:
  std::string m_exMsg;

 public:
  TSSExceptionString() : m_exMsg() {}
  virtual ~TSSExceptionString() {}

  inline std::string& str() { return m_exMsg; }

  static ACE_TSS<TSSExceptionString> s_tssExceptionMsg;
};

ACE_TSS<TSSExceptionString> TSSExceptionString::s_tssExceptionMsg;

void setTSSExceptionMessage(const char* exMsg) {
  TSSExceptionString::s_tssExceptionMsg->str().clear();
  if (exMsg != NULL) {
    TSSExceptionString::s_tssExceptionMsg->str().append(exMsg);
  }
}

const char* getTSSExceptionMessage() {
  return TSSExceptionString::s_tssExceptionMsg->str().c_str();
}

}  // namespace gemfire
