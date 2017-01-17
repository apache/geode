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
