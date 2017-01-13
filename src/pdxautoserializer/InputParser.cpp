/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "base_types.hpp"
#include "InputParser.hpp"

namespace gemfire {
namespace pdx_auto_serializer {
// ClassInfo method definitions

void ClassInfo::init(InputParser* parser) { m_parser = parser; }

// InputParser method definitions

bool InputParser::select(const std::string& className) {
  ASClassFlagMap::iterator findClass = m_classes.find(className);
  if (findClass != m_classes.end()) {
    findClass->second.second = true;
    return true;
  }
  return false;
}

void InputParser::getSelectedClasses(ASClassVector& classes) const {
  for (ASClassFlagMap::const_iterator mapIterator = m_classes.begin();
       mapIterator != m_classes.end(); ++mapIterator) {
    if (mapIterator->second.second) {
      classes.push_back(mapIterator->second.first);
    }
  }
}

bool InputParser::contains(const std::string& className) const {
  ASClassFlagMap::const_iterator findClass = m_classes.find(className);
  return (findClass != m_classes.end());
}
}  // namespace pdx_auto_serializer
}  // namespace gemfire
