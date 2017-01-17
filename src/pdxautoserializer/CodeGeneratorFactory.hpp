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

#ifndef _GFAS_CODEGENERATORFACTORY_HPP_
#define _GFAS_CODEGENERATORFACTORY_HPP_

#include "CodeGenerator.hpp"

namespace gemfire {
namespace pdx_auto_serializer {
/**
 * Defines a function with no arguments returning a pointer to
 * <code>CodeGenerator</code> object.
 */
typedef CodeGenerator* (*ASCodeGeneratorFn)(void);

/**
 * Factory class to obtain instances of <code>CodeGenerator</code>
 * implementations.
 */
class CodeGeneratorFactory {
 public:
  /**
   * Default constructor that registers all the available
   * <code>CodeGenerator</code> implementations.
   */
  CodeGeneratorFactory();

  /**
   * Get an instance of an <code>CodeGenerator</code> using the name
   * of the code generator.
   *
   * @param generatorName The name of the code generator backend.
   * @return An instance of <code>CodeGenerator</code>.
   */
  CodeGenerator* getInstance(const std::string& generatorName) const;

  /** Get a list of all registered code generator backends. */
  StringVector getGenerators() const;

  /** Virtual destructor. */
  virtual ~CodeGeneratorFactory();

 private:
  /**
   * The map containing the mappings from the names of code generators
   * to their factory functions.
   */
  std::map<std::string, ASCodeGeneratorFn> m_generatorMap;
};
}
}

#endif  // _GFAS_CODEGENERATORFACTORY_HPP_
