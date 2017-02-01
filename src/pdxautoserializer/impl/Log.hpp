#pragma once

#ifndef APACHE_GEODE_GUARD_ede048400be44f9dde5948f28b51fc23
#define APACHE_GEODE_GUARD_ede048400be44f9dde5948f28b51fc23

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


#include <string>
#include <iosfwd>

namespace apache {
namespace geode {
namespace client {
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
}  // namespace pdx_auto_serializer
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // APACHE_GEODE_GUARD_ede048400be44f9dde5948f28b51fc23
