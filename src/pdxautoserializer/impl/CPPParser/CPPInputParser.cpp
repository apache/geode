/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "../../base_types.hpp"
#include <stdio.h>
#ifndef WIN32
#include <strings.h>
#endif
#include "CPPInputParser.hpp"
#include "../../impl/Log.hpp"
#include <fstream>
#include <antlr/ANTLRException.hpp>

// Used to control selected tracing at statement level.
extern int statementTrace;

int deferredLineCount = 0;

void process_line_directive(const char* includedFile,
                            const char* includedLineNo) {}

ANTLR_USING_NAMESPACE(antlr);

namespace gemfire {
namespace pdx_auto_serializer {
/** The name of this module to be used for logging. */
static std::string ModuleName = "CPPInputParser";

// CPPClassInfo definitions

std::string CPPClassInfo::getName() const { return m_className; }

std::string CPPClassInfo::getMethodPrefix() const { return m_method_prefix; }

void CPPClassInfo::getReferences(ReferenceVector& references) const {
  if (m_headerName.length() > 0) {
    ReferenceInfo classHeader = {m_headerName, Reference::HEADER};

    references.push_back(classHeader);
  }

  /*ReferenceInfo builtinHeader = { "ASBuiltins.hpp",
                                  Reference::HEADER };*/

  // references.push_back(builtinHeader);

  /*ReferenceInfo builtinHeaderWriter = { "gfcpp/PdxWriter.hpp",
  Reference::HEADER };
  references.push_back(builtinHeaderWriter);

  ReferenceInfo builtinHeaderReader = { "gfcpp/PdxWriter.hpp",
  Reference::HEADER };
  references.push_back(builtinHeaderReader);*/
}

void CPPClassInfo::getTypeInfo(TypeInfo& classInfo) const {
  classInfo.m_kind = TypeKind::VALUE;
  classInfo.m_nameOrSize = m_className;
  classInfo.m_nameOfArrayElemSize = m_className;
  classInfo.m_children = NULL;
  classInfo.m_numChildren = 0;
  classInfo.m_namespaces = m_NamespacesList;
}

void CPPClassInfo::getMembers(VariableVector& members) const {
  for (VariableVectorIterator memberIterator = m_members.begin();
       memberIterator != m_members.end(); ++memberIterator) {
    members.push_back(*memberIterator);
  }
}

void CPPClassInfo::setHeaderName(const std::string headerName) {
  m_headerName = headerName;
}

void CPPClassInfo::setClassName(const std::string className) {
  m_className = className;
}

void CPPClassInfo::setMethodPrefix(const std::string className) {
  m_method_prefix = m_method_prefix + className + "::";
}

void CPPClassInfo::addMember(const VariableInfo& member) {
  m_members.push_back(member);
}

void CPPClassInfo::addNamespace(std::vector<std::string>& inNamespaceVector) {
  for (std::vector<std::string>::iterator itr = inNamespaceVector.begin();
       itr != inNamespaceVector.end(); ++itr) {
    m_NamespacesList.push_back(*itr);
  }
}

CPPClassInfo::CPPClassInfo() { m_method_prefix = ""; }

CPPClassInfo::~CPPClassInfo() {}

// CPPHeaderParser definitions

void CPPHeaderParser::beginClassDefinition(TypeSpecifier ts, const char* tag) {
  CPPParser::beginClassDefinition(ts, tag);
  m_currentClassesVector.push_back(new CPPClassInfo());
  m_currentClassesVector.back()->setHeaderName(getFilename());
  m_currentClassesVector.back()->setClassName(tag);

  if (m_currentClassesVector.size() > 1) {
    for (std::vector<CPPClassInfo*>::iterator iter =
             m_currentClassesVector.begin();
         iter != m_currentClassesVector.end() - 1; ++iter) {
      m_currentClassesVector.back()->setMethodPrefix((*iter)->getName());
    }
  }
  m_currentClassScope = symbols->getCurrentScopeIndex();
}

void CPPHeaderParser::exitNamespaceScope() { m_namespaceVector.pop_back(); }

void CPPHeaderParser::endClassDefinition() {
  CPPParser::endClassDefinition();
  if (m_currentClassesVector.size() == 0) return;
  if (m_arraySizeRefMap.size() > 0) {
    for (VariableVector::iterator memberIterator =
             m_currentClassesVector.back()->m_members.begin();
         memberIterator != m_currentClassesVector.back()->m_members.end();
         memberIterator++) {
      StringMap::const_iterator findMember =
          m_arraySizeRefMap.find(memberIterator->m_name);
      if (findMember != m_arraySizeRefMap.end()) {
        memberIterator->m_type.m_kind |= TypeKind::ARRAY;
        memberIterator->m_type.m_nameOrSize = findMember->second;
        Log::info(ModuleName, "Using \"" + findMember->second +
                                  "\" as size of array \"" +
                                  memberIterator->m_name + '"');
      }

      StringMap::const_iterator findMemberElem =
          m_arrayElemSizeRefMap.find(memberIterator->m_name);
      if (findMemberElem != m_arrayElemSizeRefMap.end()) {
        memberIterator->m_type.m_kind |= TypeKind::ARRAY;
        memberIterator->m_type.m_nameOfArrayElemSize = findMemberElem->second;
        Log::info(ModuleName, "Using \"" + findMemberElem->second +
                                  "\" as size of array \"" +
                                  memberIterator->m_name + '"');
      }
    }
  }
  /*
   * add the namespace informaton here
   */
  m_currentClassesVector.back()->addNamespace(m_namespaceVector);

  m_classes[m_currentClassesVector.back()->getName()] = std::make_pair(
      static_cast<ClassInfo*>(m_currentClassesVector.back()), m_selectAll);
  m_currentClassesVector.pop_back();
  m_currentClassScope = -1;
}

void CPPHeaderParser::declaratorID(const char* id, QualifiedItem item) {
  CPPParser::declaratorID(id, item);
  if (item == qiNamespace) {
    m_namespaceVector.push_back(id);
  }
  if (m_currentClassesVector.size() != 0 && m_currentClassScope >= 0 &&
      m_currentClassScope == symbols->getCurrentScopeIndex()) {
    if (item == qiVar) {  // check if this is a member field declaration
      if (m_currentInclude && m_currentArraySizeRef.length() == 0 &&
          m_currentArrayElemSizeRef.length() == 0) {
        VariableInfo var;

        var.m_type.m_kind = TypeKind::VALUE;
        var.m_type.m_modifier = TypeModifier::NONE;
        var.m_type.m_children = NULL;
        var.m_type.m_numChildren = 0;
        var.m_name = id;
        if (m_markIdentityField == true) {
          var.m_markIdentityField = true;
        } else {
          var.m_markIdentityField = false;
        }

        if (m_markPdxUnreadField == true) {
          var.m_markPdxUnreadField = true;
        } else {
          var.m_markPdxUnreadField = false;
        }

        m_currentClassesVector.back()->addMember(var);
      }
      /*else if ( item == qiType ) {
          m_currentClass->addNamespace(id);
      }*/
      else if (m_currentArraySizeRef.length() > 0) {
        if (NULL == strchr(m_currentArraySizeRef.c_str(), ',')) {
          m_arraySizeRefMap[m_currentArraySizeRef] = id;
        } else {
          m_currentArraySizeRef.erase(m_currentArraySizeRef.begin());
          m_currentArraySizeRef.erase(m_currentArraySizeRef.end() - 1);
          char* p =
              strtok(const_cast<char*>(m_currentArraySizeRef.c_str()), ",");
          while (p) {
            m_arraySizeRefMap[p] = id;
            p = strtok(NULL, ",");
          }
        }
      } else if (m_currentArrayElemSizeRef.length() > 0) {
        // std::cout << "Map Entries --> " << "m_arrayElemSizeRefMap[" <<
        // m_currentArrayElemSizeRef <<"] = " << id << std::endl;
        m_arrayElemSizeRefMap[m_currentArrayElemSizeRef] = id;
      }
    }
  }
  // m_currentInclude = true;
  m_currentArraySizeRef = "";
  m_currentArrayElemSizeRef = "";
}

void CPPHeaderParser::declarationSpecifier(bool td, bool fd, StorageClass sc,
                                           TypeQualifier tq, TypeSpecifier ts,
                                           FunctionSpecifier fs) {
  CPPParser::declarationSpecifier(td, fd, sc, tq, ts, fs);
  if ((tq & tqCONST) || ((tq & tqGFEXCLUDE) && !(tq & tqGFINCLUDE))) {
    m_currentInclude = false;
  } else {
    m_currentInclude = true;
  }
  if (tq & tqGFID) {
    m_markIdentityField = true;
  } else {
    m_markIdentityField = false;
  }
  if (tq & tqGFUNREAD) {
    m_markPdxUnreadField = true;
  } else {
    m_markPdxUnreadField = false;
  }
}

void CPPHeaderParser::gfArraySize(const char* id) {
  m_currentArraySizeRef = id;
}

void CPPHeaderParser::gfArrayElemSize(const char* id) {
  m_currentArrayElemSizeRef = id;
}

CPPHeaderParser::CPPHeaderParser(CPPLexer& lexer, ASClassFlagMap& classes,
                                 const bool selectAll)
    : CPPParser(lexer),
      m_classes(classes),
      m_selectAll(selectAll),
      m_currentClassesVector(std::vector<CPPClassInfo*>()),
      m_currentClassScope(-1),
      m_currentInclude(true),
      m_markIdentityField(false),
      m_markPdxUnreadField(false) {}

// CPPInputParser definitions

void CPPInputParser::getOptions(OptionMap& options) const {}

void CPPInputParser::init(PropertyMap& properties) {}

void CPPInputParser::selectClasses(const StringVector& resources,
                                   const StringVector& classNames) {
#ifndef _DEBUG
  statementTrace = 0;
#endif
  statementTrace = 0;
  bool selectAll = (classNames.size() == 0);
  for (StringVectorIterator resourceIterator = resources.begin();
       resourceIterator != resources.end(); ++resourceIterator) {
    try {
      std::ifstream istream(resourceIterator->c_str());
      CPPLexer lexer(istream);
      CPPHeaderParser parser(lexer, m_classes, selectAll);
      parser.init();
      parser.setFilename(*resourceIterator);
      parser.translation_unit();
    } catch (const ANTLRException& ex) {
      throw std::invalid_argument(ex.getMessage());
    }
  }

  for (StringVectorIterator classIterator = classNames.begin();
       classIterator != classNames.end(); ++classIterator) {
    if (!select(*classIterator)) {
      std::string warnMsg =
          "Could not load class '" + *classIterator + "'; skipping it.";
      Log::warn(ModuleName, warnMsg);
    }
  }
}

CPPInputParser::CPPInputParser() {}

InputParser* CPPInputParser::create() { return new CPPInputParser(); }

CPPInputParser::~CPPInputParser() {}
}  // namespace pdx_auto_serializer
}  // namespace gemfire
