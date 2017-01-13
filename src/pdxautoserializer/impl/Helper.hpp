/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GFAS_HELPER_HPP_
#define _GFAS_HELPER_HPP_

#include "../InputParser.hpp"
#include <algorithm>
#include <cctype>
#include <sstream>

// TODO cmake what is the purpose of only doing this on GNU? #ifdef __GNUC__
#if !defined(_WIN32)
extern "C" {
#include <cxxabi.h>
#include <stdlib.h>
#include <string.h>
}
#endif

namespace gemfire {
namespace pdx_auto_serializer {
/**
 * Class containing some static utility methods.
 */
class Helper {
 public:
  /**
   * Set the various fields for a <code>TypeInfo</code> object.
   *
   * @param type Pointer to the type whose fields have to be set.
   * @param kind The kind (using the constants defined in
   *             <code>TypeKind</code>) for the type.
   * @param modified The modifier (using the constants defined in
   *                 <code>TypeModifier</code> for the type.
   * @param nameOrSize The name or size of this type.
   * @param child Child type of this, if any.
   */
  inline static void setTypeInfo(TypeInfo* type, const int kind,
                                 const int modifier,
                                 const std::string& nameOrSize,
                                 const std::string& nameOfArrayElemSize,
                                 TypeInfo* children, int numChildren) {
    type->m_kind = kind;
    type->m_modifier = modifier;
    type->m_nameOrSize = nameOrSize;
    type->m_nameOfArrayElemSize = nameOfArrayElemSize;
    type->m_children = children;
    type->m_numChildren = numChildren;
  }

  /**
   *  Convert a given string to lower-case.
   *
   * @param str The string to be converted.
   * @return The string converted to lower-case.
   */
  inline static std::string toLower(const std::string& str) {
    std::string strLower = str;
    std::transform(strLower.begin(), strLower.end(), strLower.begin(),
                   (int (*)(int))std::tolower);
    return strLower;
  }

  /**
   * Convenience function to split a given string on the given delimiter.
   *
   * @param str The string to be split.
   * @param delim The delimiter to be used for splitting the string.
   * @param splitStr The vector containing the split portions of string.
   */
  static void splitString(const std::string& str, const std::string& delim,
                          StringVector& splitStr);

  /**
   * Replace all the occurances of a sequence with a given string.
   * Right now uses a simple sliding window algorithm.
   *
   * @param source The string to search in.
   * @param findStr The sequence to search for in <code>source</code>.
   * @param replaceStr The replacement string.
   * @return The result string after replacing all occurances of
   *         <code>findStr</code> with <code>replaceStr</code> in
   *         <code>source</code>.
   */
  static std::string stringReplace(const std::string& source,
                                   const std::string& findStr,
                                   const std::string& replaceStr);

  /**
   * Template function to convert between different types when possible.
   * Tries to emulate the function of same name provided by the boost
   * library.
   *
   * @param val The source to be converted.
   * @param dest The destination that shall contain the conversion.
   */
  template <typename TDest, class TSrc>
  inline static void lexical_cast(const TSrc& src, TDest& dest) {
    std::stringstream ss;
    if (!(ss << src && ss >> dest && ss >> std::ws && ss.eof())) {
      throw std::invalid_argument("Conversion failed.");
    }
  }

  /**
   * Get a property with single value and erase from the property map.
   *
   * @param properties The property map. If the property is found in
   *                   the map then it is erased.
   * @param name The name of the property to obtain.
   * @param value The value of the property. It should have a single
   *              value else this is not filled in.
   * @return True if the property was found in the map.
   */
  static bool getSingleProperty(PropertyMap& properties,
                                const std::string& name, std::string& value);

  /**
   * Get a property with multiple values and erase from the property map.
   *
   * @param properties The property map. If the property is found in
   *                   the map then it is erased.
   * @param name The name of the property to obtain.
   * @param value The value vector for the property.
   * @return True if the property was found in the map.
   */
  static bool getMultiProperty(PropertyMap& properties, const std::string& name,
                               StringVector& value);

  /**
   * Cleanup the vector of allocated <code>ClassInfo</code> objects.
   *
   * @param classes The vector of <code>ClassInfo</code>es.
   */
  static void deleteASClasses(ASClassVector& classes);

  /**
   * Get the typename after demangling (if required) the name returned
   * by <code>typeid</code> for a given object.
   *
   * @param obj The object whose type name is required.
   * @return The type name of the given object.
   */
  template <typename T>
  static std::string typeName(const T& obj) {
    const char* typeidName = typeid(obj).name();
    std::string typeName;
#ifdef __GNUC__
    int status;
    char* demangledName = abi::__cxa_demangle(typeidName, NULL, NULL, &status);
    if (status == 0 && demangledName != NULL) {
      typeName = demangledName;
      free(demangledName);
    }
#endif
    if (typeName.length() == 0) {
      typeName = typeidName;
    }
    const char* classPrefix = "class ";
    size_t classPrefixLen = ::strlen(classPrefix);
    if (typeName.substr(0, classPrefixLen) == classPrefix) {
      typeName = typeName.substr(classPrefixLen);
    }
    return typeName;
  }
};
}
}

#endif  // _GFAS_HELPER_HPP_
