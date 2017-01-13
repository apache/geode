/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "base_types.hpp"
#include "CodeGeneratorFactory.hpp"
#include "impl/CPPCodeGenerator.hpp"
//#include "impl/CPPCLICodeGenerator.hpp"
//#include "impl/CSharpCodeGenerator.hpp"

namespace gemfire {
namespace pdx_auto_serializer {
CodeGeneratorFactory::CodeGeneratorFactory() {
  // Register the available code generators here.
  m_generatorMap["C++"] = CPPCodeGenerator::create;
  // m_generatorMap["C++/CLI"] = CPPCLICodeGenerator::create;
  // m_generatorMap["C#"] = CSharpCodeGenerator::create;
}

CodeGenerator* CodeGeneratorFactory::getInstance(
    const std::string& generatorName) const {
  std::map<std::string, ASCodeGeneratorFn>::const_iterator mapIterator =
      m_generatorMap.find(generatorName);
  if (mapIterator != m_generatorMap.end()) {
    return mapIterator->second();
  }
  return NULL;
}

StringVector CodeGeneratorFactory::getGenerators() const {
  StringVector generatorList;

  for (std::map<std::string, ASCodeGeneratorFn>::const_iterator
           generatorIterator = m_generatorMap.begin();
       generatorIterator != m_generatorMap.end(); ++generatorIterator) {
    generatorList.push_back(generatorIterator->first);
  }
  return generatorList;
}

CodeGeneratorFactory::~CodeGeneratorFactory() { m_generatorMap.clear(); }
}  // namespace pdx_auto_serializer
}  // namespace gemfire
