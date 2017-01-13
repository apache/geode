/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "Log.hpp"
#include <iostream>
#include <stdexcept>

namespace gemfire {
namespace pdx_auto_serializer {
static std::ostream* g_outStream = &std::cout;
static std::ostream* g_errStream = &std::cerr;

void Log::init(std::ostream* outStream, std::ostream* errStream) {
  g_outStream = outStream;
  g_errStream = errStream;
}

void Log::info(const std::string& moduleName, const std::string& message) {
  *g_outStream << moduleName << ":: " << message << std::endl;
}

void Log::warn(const std::string& moduleName, const std::string& message) {
  *g_errStream << moduleName << ":: Warning: " << message << std::endl;
}

void Log::fatal(const std::string& moduleName, const std::string& message) {
  throw std::invalid_argument(moduleName + ":: FATAL: " + message);
}

void Log::debug(const std::string& moduleName, const std::string& message) {
#ifdef _DEBUG
  *g_outStream << moduleName << ":: DEBUG: " << message << std::endl;
#endif
}
}  // namespace pdx_auto_serializer
}  // namespace gemfire
