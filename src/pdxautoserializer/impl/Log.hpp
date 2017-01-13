/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GFAS_LOG_HPP_
#define _GFAS_LOG_HPP_

#include <string>
#include <iosfwd>

namespace gemfire {
namespace pdx_auto_serializer {
/**
 * This class provides simple static functions to print messages on the
 * console in a uniform manner, or to throw exceptions on fatal conditions.
 */
class Log {
 public:
  /**
   * Initialize the log with the given output and error streams. The
   * default is to use <code>std::cout</code> and <code>std::cerr</code>
   * for them respectively.
   *
   * @param outStream The output stream to use for logging of
   *                  <code>Log::info</code> and <code>Log::debug</code>
   *                  level messages below.
   * @param errStream The output stream to use for logging of
   *                  <code>Log::warn</code> level messages.
   */
  static void init(std::ostream* outStream, std::ostream* errStream);

  /**
   * Write info level message on the console.
   *
   * @param moduleName The module which needs to print the message.
   * @param message The message to be printed.
   */
  static void info(const std::string& moduleName, const std::string& message);

  /**
   * Write a warning message on the console.
   *
   * @param moduleName The module which needs to print the message.
   * @param message The message to be printed.
   */
  static void warn(const std::string& moduleName, const std::string& message);

  /**
   * Throw a fatal exception -- for now this uses
   * <code>std::invalid_argument</code> exception.
   *
   * @param moduleName The module which needs to print the message.
   * @param message The message to be printed.
   */
  static void fatal(const std::string& moduleName, const std::string& message);

  /**
   * Write debug level message on the console. This is only enabled in the
   * debug build when _DEBUG macro is set.
   *
   * @param moduleName The module which needs to print the message.
   * @param message The message to be printed.
   */
  static void debug(const std::string& moduleName, const std::string& message);

 private:
  // The default and copy constructors are never defined.
  Log();
  Log(const Log&);
};
}
}

#endif  // _GFAS_LOG_HPP_
