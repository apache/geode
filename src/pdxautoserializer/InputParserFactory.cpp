/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
