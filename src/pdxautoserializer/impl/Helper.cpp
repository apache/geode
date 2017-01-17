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

#include "../base_types.hpp"
#include "Helper.hpp"

namespace gemfire {
namespace pdx_auto_serializer {
void Helper::splitString(const std::string& str, const std::string& delim,
                         StringVector& splitStr) {
  std::string::size_type offset = 0;
  std::string::size_type delimIndex;

  while ((delimIndex = str.find(delim, offset)) != std::string::npos) {
    splitStr.push_back(str.substr(offset, delimIndex - offset));
    offset += delimIndex - offset + delim.length();
  }
  splitStr.push_back(str.substr(offset));
}

std::string Helper::stringReplace(const std::string& source,
                                  const std::string& findStr,
                                  const std::string& replaceStr) {
  std::string resultStr;
  std::string::const_iterator srcIterator = source.begin();
  while (srcIterator != source.end()) {
    bool matchFound = true;
    std::string::const_iterator matchIterator = srcIterator;
    std::string::const_iterator findIterator = findStr.begin();
    while (findIterator != findStr.end()) {
      if (matchIterator == source.end() ||
          *matchIterator++ != *findIterator++) {
        matchFound = false;
        break;
      }
    }
    if (matchFound) {
      resultStr += replaceStr;
      srcIterator = matchIterator;
    } else {
      resultStr += *srcIterator;
      ++srcIterator;
    }
  }
  return resultStr;
}

bool Helper::getSingleProperty(PropertyMap& properties, const std::string& name,
                               std::string& value) {
  PropertyMap::iterator propertyFind = properties.find(name);
  if (propertyFind != properties.end()) {
    if (propertyFind->second.size() == 1) {
      value = propertyFind->second[0];
    }
    properties.erase(propertyFind);
    return true;
  }
  return false;
}

bool Helper::getMultiProperty(PropertyMap& properties, const std::string& name,
                              StringVector& value) {
  PropertyMap::iterator propertyFind = properties.find(name);
  if (propertyFind != properties.end()) {
    value = propertyFind->second;
    properties.erase(propertyFind);
    return true;
  }
  return false;
}

void Helper::deleteASClasses(ASClassVector& classes) {
  for (ASClassVector::const_iterator classIterator = classes.begin();
       classIterator != classes.end(); ++classIterator) {
    delete *classIterator;
  }
  classes.clear();
}
}  // namespace pdx_auto_serializer
}  // namespace gemfire
