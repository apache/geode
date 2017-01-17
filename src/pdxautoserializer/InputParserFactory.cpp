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

#include "base_types.hpp"
#include "InputParserFactory.hpp"
#include <stdio.h>
#ifndef WIN32
#include <strings.h>
#endif
#include "impl/CPPParser/CPPInputParser.hpp"
#ifdef GEMFIRE_CLR
#include "impl/DotNetParser.hpp"
#endif

namespace gemfire {
namespace pdx_auto_serializer {
InputParserFactory::InputParserFactory() {
  // Register the available parsers here.
  m_parserMap["C++"] = CPPInputParser::create;
#ifdef GEMFIRE_CLR
  m_parserMap[".NET"] = DotNetParser::create;
#endif  // GEMFIRE_CLR
}

InputParser* InputParserFactory::getInstance(
    const std::string& parserName) const {
  std::map<std::string, InputParserFn>::const_iterator mapIterator =
      m_parserMap.find(parserName);
  if (mapIterator != m_parserMap.end()) {
    return mapIterator->second();
  }
  return NULL;
}

StringVector InputParserFactory::getParsers() const {
  StringVector parserList;

  for (std::map<std::string, InputParserFn>::const_iterator parserIterator =
           m_parserMap.begin();
       parserIterator != m_parserMap.end(); ++parserIterator) {
    parserList.push_back(parserIterator->first);
  }
  return parserList;
}

InputParserFactory::~InputParserFactory() { m_parserMap.clear(); }
}  // namespace pdx_auto_serializer
}  // namespace gemfire
