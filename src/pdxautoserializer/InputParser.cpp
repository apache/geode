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
#include "InputParser.hpp"

namespace apache {
namespace geode {
namespace client {
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
}  // namespace client
}  // namespace geode
}  // namespace apache
