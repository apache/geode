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

#include "Log.hpp"
#include <iostream>
#include <stdexcept>

namespace apache {
namespace geode {
namespace client {
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
}  // namespace client
}  // namespace geode
}  // namespace apache
