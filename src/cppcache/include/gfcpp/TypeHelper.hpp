#ifndef _GEMFIRE_TYPEHELPER_HPP_
#define _GEMFIRE_TYPEHELPER_HPP_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
 * @file Contains helper classes for extracting compile-time type traits
 */

#include "gfcpp_globals.hpp"

namespace gemfire {
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
}
}

/** @brief Macro to unwrap the type <code>T</code> inside SharedPtr. */
#define GF_UNWRAP_SP(T) typename gemfire::TypeHelper::UnwrapSharedPtr<T>::type

/**
 * @brief Macro to determine if the type <code>T</code> is derived from
 *        <code>Serializable</code>.
 */
#define GF_TYPE_IS_SERIALIZABLE(T) \
  gemfire::TypeHelper::SuperSubclass<gemfire::Serializable, T>::result

/**
 * @brief Macro that returns <code>yes_type</code> if the type <code>T</code> is
 *        derived from <code>Serializable</code> and <code>no_type</code>
 *        otherwise. Useful for overloaded template functions.
 */
#define GF_TYPE_IS_SERIALIZABLE_TYPE(T) \
  gemfire::TypeHelper::YesNoType<GF_TYPE_IS_SERIALIZABLE(T)>::value

#define GF_SRC_IS_TARGET_TYPE(TARGET, SRC) \
  gemfire::TypeHelper::YesNoType<          \
      gemfire::TypeHelper::SuperSubclass<TARGET, SRC>::result>::value

#endif  // _GEMFIRE_TYPEHELPER_HPP_
