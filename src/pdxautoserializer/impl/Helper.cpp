/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
