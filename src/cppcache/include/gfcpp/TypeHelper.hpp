#pragma once

#ifndef GEODE_GFCPP_TYPEHELPER_H_
#define GEODE_GFCPP_TYPEHELPER_H_

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

/**
 * @file Contains helper classes for extracting compile-time type traits
 */

#include "gfcpp_globals.hpp"

namespace apache {
namespace geode {
namespace client {
// Forward declaration of SharedPtr<T>
template <typename Target>
class SharedPtr;

// Forward declaration of SharedArrayPtr<T, ID>
template <typename Target, int8_t TYPEID>
class SharedArrayPtr;

// Forward declaration of CacheableArrayType<T, ID>
template <typename Target, int8_t TYPEID>
class CacheableArrayType;

/**
 * @brief Helper type traits and other structs/classes to determine type
 *        information at compile time using typename.
 *        Useful for templates in particular.
 */
namespace TypeHelper {
typedef uint8_t yes_type;
typedef uint32_t no_type;

template <typename TBase, typename TDerived>
struct BDHelper {
  template <typename T>
  static yes_type check_sig(TDerived const volatile*, T);
  static no_type check_sig(TBase const volatile*, int);
};

/**
 * @brief This struct helps us determine whether or not a class is a
 *        subclass of another at compile time, so that it can be used
 *        in templates. For an explanation of how this works see:
 *        {@link
 * http://groups.google.com/group/comp.lang.c++.moderated/msg/dd6c4e4d5160bd83}
 *        {@link
 * http://groups.google.com/group/comp.lang.c++.moderated/msg/645b2486ae80e5fb}
 */
template <typename TBase, typename TDerived>
struct SuperSubclass {
 private:
  struct Host {
    operator TBase const volatile*() const;
    operator TDerived const volatile*();
  };

 public:
  static const bool result = sizeof(BDHelper<TBase, TDerived>::check_sig(
                                 Host(), 0)) == sizeof(yes_type);
};

/**
 * @brief Specialization of <code>SuperSubclass</code> to return true
 *        for the special case when the two types being checked are same.
 */
template <typename TBase>
struct SuperSubclass<TBase, TBase> {
  static const bool result = true;
};

/**
 * @brief This struct helps convert a boolean value into static objects
 *        of different types. Useful for matching of template functions.
 */
template <bool getType = true>
struct YesNoType {
  static const yes_type value = 0;
};

/**
 * @brief Specialization of YesNoType for boolean value false.
 */
template <>
struct YesNoType<false> {
  static const no_type value = 0;
};

/** @brief This struct unwraps the type <code>T</code> inside SharedPtr. */
template <class T>
struct UnwrapSharedPtr {};

/** @brief This struct unwraps the type <code>T</code> inside SharedPtr. */
template <class T>
struct UnwrapSharedPtr<SharedPtr<T> > {
  typedef T type;
};

/** @brief This struct unwraps the type <code>T</code> inside SharedArrayPtr. */
template <class T, int8_t ID>
struct UnwrapSharedPtr<SharedArrayPtr<T, ID> > {
  typedef CacheableArrayType<T, ID> type;
};
}  // namespace TypeHelper
}  // namespace client
}  // namespace geode
}  // namespace apache

/** @brief Macro to unwrap the type <code>T</code> inside SharedPtr. */
#define GF_UNWRAP_SP(T) \
  typename apache::geode::client::TypeHelper::UnwrapSharedPtr<T>::type

/**
 * @brief Macro to determine if the type <code>T</code> is derived from
 *        <code>Serializable</code>.
 */
#define GF_TYPE_IS_SERIALIZABLE(T)                  \
  apache::geode::client::TypeHelper::SuperSubclass< \
      apache::geode::client::Serializable, T>::result

/**
 * @brief Macro that returns <code>yes_type</code> if the type <code>T</code> is
 *        derived from <code>Serializable</code> and <code>no_type</code>
 *        otherwise. Useful for overloaded template functions.
 */
#define GF_TYPE_IS_SERIALIZABLE_TYPE(T)                                 \
  apache::geode::client::TypeHelper::YesNoType<GF_TYPE_IS_SERIALIZABLE( \
      T)>::value

#define GF_SRC_IS_TARGET_TYPE(TARGET, SRC)                     \
  apache::geode::client::TypeHelper::YesNoType<                \
      apache::geode::client::TypeHelper::SuperSubclass<TARGET, \
                                                       SRC>::result>::value

#endif // GEODE_GFCPP_TYPEHELPER_H_
