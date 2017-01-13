/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GFAS_CPPINPUTPARSER_HPP_
#define _GFAS_CPPINPUTPARSER_HPP_

#include "../../InputParser.hpp"
#include "CPPLexer.hpp"
#include "CPPParser.hpp"

namespace gemfire {
namespace pdx_auto_serializer {
class CPPHeaderParser;

/**
 * This class implements <code>ClassInfo</code> interface for C++.
 */
class CPPClassInfo : public ClassInfo {
 public:
  // ClassInfo implementations

  virtual std::string getName() const;

  virtual void getReferences(ReferenceVector& references) const;

  virtual void getTypeInfo(TypeInfo& classType) const;

  virtual void getMembers(VariableVector& members) const;

  // Setter methods for various fields.

  void setHeaderName(const std::string headerName);

  void setClassName(const std::string className);

  void addMember(const VariableInfo& member);

  void addNamespace(std::vector<std::string>& inNamespaceVector);

  /** Default constructor. */
  CPPClassInfo();

  /** Virtual destructor. */
  virtual ~CPPClassInfo();

  void setMethodPrefix(const std::string className);
  virtual std::string getMethodPrefix() const;

 private:
  /** The name of the header file containing this class. */
  std::string m_headerName;

  /** The name of the class. */
  std::string m_className;

  /** Vector of the members of the class. */
  VariableVector m_members;

  std::vector<std::string> m_NamespacesList;

  std::string m_method_prefix;

  friend class CPPHeaderParser;
  friend class CPPInputParser;
};

/** This class parses the classes from header files. */
class CPPHeaderParser : public CPPParser {
 public:
  /**
   * Construct the object using the given <code>CPPLexer</code>.
   *
   * @param lexer The <code>CPPLexer</code> used as the tokenizer.
   * @param classes The global classes map to be populated.
   * @param selectAll True to mark all the classes as selected.
   */
  CPPHeaderParser(CPPLexer& lexer, ASClassFlagMap& classes,
                  const bool selectAll);

  // CPPParser overrides

  virtual void beginClassDefinition(TypeSpecifier ts, const char* tag);

  virtual void exitNamespaceScope();

  virtual void endClassDefinition();

  virtual void declaratorID(const char* id, QualifiedItem item);

  virtual void declarationSpecifier(bool td, bool fd, StorageClass sc,
                                    TypeQualifier tq, TypeSpecifier ts,
                                    FunctionSpecifier fs);

  virtual void gfArraySize(const char* id);

  virtual void gfArrayElemSize(const char* id);

 private:
  /** Accumulates the classes as the classes are parsed. */
  ASClassFlagMap& m_classes;

  /** True to mark all the classes as selected. */
  const bool m_selectAll;

  /** Stores the current class being parsed. */
  std::vector<CPPClassInfo*> m_currentClassesVector;

  /** The scope of the current class. */
  int m_currentClassScope;

  /** True when the current field has to be excluded. */
  bool m_currentInclude;

  /** True when the current fiels is marked as identify field */
  bool m_markIdentityField;

  bool m_markPdxUnreadField;

  /**
   * If the current field is an array size specification of field 'x',
   * then this stores the name of 'x'.
   */
  std::string m_currentArraySizeRef;

  std::string m_currentArrayElemSizeRef;

  /**
   * The map of all fields that are array sizes for other fields.
   */
  StringMap m_arraySizeRefMap;
  StringMap m_arrayElemSizeRefMap;

  std::vector<std::string> m_namespaceVector;
};

/**
 * This class implements <code>InputParser</code> interface for C++.
 */
class CPPInputParser : public InputParser {
 public:
  // InputParser implementations

  virtual void getOptions(OptionMap& options) const;

  virtual void init(PropertyMap& properties);

  virtual void selectClasses(const StringVector& resources,
                             const StringVector& classNames);

  /**
   * Defines the static function returning an <code>InputParser</code>
   * object, which is registered with <code>InputParserFactory</code>.
   *
   * @return The <code>InputParser</code> object.
   */
  static InputParser* create();

  /** @brief Virtual destructor. */
  virtual ~CPPInputParser();

 private:
  /**
   * Default constructor -- this is not exposed to public which should use
   * the {@link CPPInputParser::create} function.
   */
  CPPInputParser();
};
}
}

#endif  // _GFAS_CPPINPUTPARSER_HPP_
