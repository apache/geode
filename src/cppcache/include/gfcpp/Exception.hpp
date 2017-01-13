#ifndef __GEMFIRE_EXCEPTION_H__
#define __GEMFIRE_EXCEPTION_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/**
 * @file
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"

namespace gemfire {

#define GF_EX_MSG_LIMIT 2048

class Exception;
typedef SharedPtr<Exception> ExceptionPtr;

class DistributedSystem;

/**
 * @class Exception Exception.hpp
 * A description of an exception that occurred during a cache operation.
 */
class CPPCACHE_EXPORT Exception : public SharedBase {
  /**
   * @brief public methods
   */
 public:
  /** Creates an exception.
   * @param  msg1 message pointer, this is copied into the exception.
   * @param  msg2 optional extra message pointer, appended to msg1.
   * @param  forceTrace enables a stacktrace for this exception regardless of
   * stacktrace-enabled system property.
   * @param  cause optional cause of the exception which can be later
   *               retrieved using <code>getCause</code>
   **/
  Exception(const char* msg1, const char* msg2 = NULL, bool forceTrace = false,
            const ExceptionPtr& cause = NULLPTR);

  /** Creates an exception as a copy of the given other exception.
   * @param  other the original exception.
   *
   **/
  Exception(const Exception& other);

  /** Create a clone of this exception. */
  virtual Exception* clone() const;

  /**
   * @brief destructor
   */
  virtual ~Exception();

  /** Returns the message pointer
   *
   * @return  message pointer
   */
  virtual const char* getMessage() const;
  /** Show the message pointer
   *
   */
  virtual void showMessage() const;

  /** On some platforms, print a stacktrace from the location the exception
    * was created.
    */
  virtual void printStackTrace() const;

#ifndef _SOLARIS
  /** On some platforms, get a stacktrace string from the location the
    * exception was created.
    */
  virtual size_t getStackTrace(char* buffer, size_t maxLength) const;
#endif

  /** Return the name of this exception type. */
  virtual const char* getName() const;

  /**
   * Throw polymorphically; this allows storing an exception object
   * pointer and throwing it later.
   */
  virtual void raise() { throw * this; }

  inline ExceptionPtr getCause() const { return m_cause; }

 protected:
  /** internal constructor used to clone this exception */
  Exception(const CacheableStringPtr& message, const StackTracePtr& stack,
            const ExceptionPtr& cause);

  static bool s_exceptionStackTraceEnabled;

  CacheableStringPtr m_message;  // error message
  StackTracePtr m_stack;
  ExceptionPtr m_cause;

 private:
  static void setStackTraces(bool stackTraceEnabled);

  friend class DistributedSystem;
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_EXCEPTION_H__
