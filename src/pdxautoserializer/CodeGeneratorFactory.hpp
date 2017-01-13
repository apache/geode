/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
