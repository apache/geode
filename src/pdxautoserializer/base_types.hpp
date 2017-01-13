/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GFAS_BASETYPES_HPP_
#define _GFAS_BASETYPES_HPP_

#include <string>
#include <vector>
#include <set>
#include <map>
#include <cassert>
#include <stdexcept>

namespace gemfire {
namespace pdx_auto_serializer {
/** Shorthand for vector of strings. */
typedef std::vector<std::string> StringVector;

/** Shorthand for set of strings. */
typedef std::set<std::string> StringSet;

/**
 * Shorthand for the <code>std::map</code> containing property
 * key-value pairs.
 */
typedef std::map<std::string, StringVector> PropertyMap;

/**
 * Shorthand for the <code>std::map</code> containing string
 * key-value pairs.
 */
typedef std::map<std::string, std::string> StringMap;

/**
 * Shorthand for the <code>std::map</code> containing string
 * keys mapped to usage string and boolean indicator of option
 * requiring a value.
 */
typedef std::map<std::string, std::pair<bool, std::string> > OptionMap;

/** Shorthand for iterator of vector of strings. */
typedef std::vector<std::string>::const_iterator StringVectorIterator;

/** Shorthand for iterator of set of strings. */
typedef std::set<std::string>::const_iterator StringSetIterator;

/**
 * Encapsulates different kinds of references (headers/dlls etc).
 */
class Reference {
 public:
  enum Kind {
    /** Indicates a C++ header file. */
    HEADER,

    /** Indicates a library (so/dll). */
    LIB,

    /** Indicates a jar file (Java). */
    JAR
  };
};

/**
 * Structure to hold information of a reference (e.g. header/dll).
 */
struct ReferenceInfo {
  /** Path of the reference. */
  std::string m_path;

  /** the <code>Reference::Kind</code> of the reference. */
  Reference::Kind m_kind;
};

/**
 * Encapsulates different kinds of types like <code>VALUE</code>,
 * <code>ARRAY</code> etc.
 *
 * Also includes the information whether the type is a built-in one or
 * a user-defined type, and whether the type is a .NET managed type.
 */
class TypeKind {
 public:
  /** An invalid type. */
  static const int INVALID = 0x0;

  /** A value type. */
  static const int VALUE = 0x01;

  /** A pointer type. */
  static const int POINTER = 0x02;

  /** A reference type or pass by reference. */
  static const int REFERENCE = 0x04;

  /** An array type with fixed/dynamic size. */
  static const int ARRAY = 0x08;

  /**
   * The array is of fixed size -- both this and <code>ARRAY</code>
   * bits should be set for fixed size arrays.
   */
  static const int FIXEDARRAY = 0x10;

  /** A C++ template or .NET/Java generic type. */
  static const int TEMPLATE = 0x20;

  /** The type is actually a template parameter symbol. */
  static const int TEMPLATEPARAM = 0x40;

  /**
   * Indicates that the type is a .NET managed type -- required for
   * generators like C++/CLI that understand both normal objects as
   * well as .NET types.
   */
  static const int MANAGED = 0x100;

  /**
   * Indicates that the type is a builtin one. This will be normally
   * OR'd with one of the actual kinds above. If this bit is not set
   * then the type is assumed to be a user-defined type.
   */
  static const int BUILTIN = 0x200;
};

/**
 * Mask to extract the Type part only from the constants in
 * <code>TypeKind</code> class.
 */
const int TYPEKIND_TYPEMASK = 0xFF;

/**
 * Constants for the possible kinds of type modifiers.
 */
class TypeModifier {
 public:
  /** No modifier. */
  static const int NONE = 0x01;

  /** The type is defined to be a constant. */
  static const int CONSTANT = 0x02;

  /** The type is defined to be volatile. */
  static const int VOLATILE = 0x04;

  /** The member is private to the class. */
  static const int PRIVATE = 0x08;

  /** The member is declared to be protected. */
  static const int PROTECTED = 0x10;

  /** The member is declared to be public. */
  static const int PUBLIC = 0x20;

  /** The member is declared to be internal (.NET). */
  static const int INTERNAL = 0x40;

  /** The member is a property (.NET). */
  static const int PROPERTY = 0x80;

  /** The member is defined to be transient (Java). */
  static const int TRANSIENT = 0x100;
};

/**
 * Structure to hold information for a type.
 */
struct TypeInfo {
  /** The {@link gemfire::pdx_auto_serializer::TypeKind} of the type. */
  int m_kind;

  /** The {@link gemfire::pdx_auto_serializer::TypeModifier} for the type. */
  int m_modifier;

  /**
   * Contains either the name of the variable, or for the case of
   * <code>FIXEDARRAY</code> or <code>ARRAY</code>, the size of the array,
   * name of variable containing the size respectively.
   */
  std::string m_nameOrSize;

  std::string m_nameOfArrayElemSize;

  /** The namespace for the type expressed as a vector. */
  StringVector m_namespaces;

  /**
   * Information of any child sub-types for the case of
   * <code>POINTER</code>, <code>REFERENCE</code> or
   * <code>TEMPLATE</code> types.
   */
  TypeInfo* m_children;

  /**
   *  The number of child sub-types. Can be greater than one for the case
   *  of <code>TEMPLATE</code> types.
   */
  int m_numChildren;
};

/**
 * Structure to hold information for a variable.
 */
struct VariableInfo {
  /** The type of the variable. */
  TypeInfo m_type;

  bool m_markIdentityField;
  bool m_markPdxUnreadField;
  /** The name of the variable. */
  std::string m_name;
};

/** Shorthand for a vector of <code>ReferenceInfo</code>. */
typedef std::vector<ReferenceInfo> ReferenceVector;

/**
 * Shorthand for <code>const_iterator</code> of a vector of
 * <code>ReferenceInfo</code>.
 */
typedef std::vector<ReferenceInfo>::const_iterator ReferenceVectorIterator;

/** Shorthand for a vector of <code>VariableInfo</code>. */
typedef std::vector<VariableInfo> VariableVector;

/**
 * Shorthand for <code>const_iterator</code> of a vector of
 * <code>VariableInfo</code>.
 */
typedef std::vector<VariableInfo>::const_iterator VariableVectorIterator;
}
}

#endif  // _GFAS_BASETYPES_HPP_
