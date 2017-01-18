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

#ifndef _GFAS_CPPINPUTPARSER_HPP_
#define _GFAS_CPPINPUTPARSER_HPP_

#include "../../InputParser.hpp"
#include "CPPLexer.hpp"
#include "CPPParser.hpp"

namespace apache {
namespace geode {
namespace client {
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
}  // namespace pdx_auto_serializer
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif  // _GFAS_CPPINPUTPARSER_HPP_
