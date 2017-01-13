/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GFAS_CODEGENERATOR_HPP_
#define _GFAS_CODEGENERATOR_HPP_

#include "base_types.hpp"

namespace gemfire {
namespace pdx_auto_serializer {
/**
 * Abstract class that defines the interface of the auto-serializer
 * code generator backends.
 */
class CodeGenerator {
 public:
  /**
   * Enumerates the various methods that can be generated.
   */
  class Method {
   public:
    enum Type {
      /** Indicates the toData() method. */
      TODATA,

      /** Indicates the fromData() method. */
      FROMDATA,

      ///** Indicates the objectSize() method. */
      // OBJECTSIZE,

      ///** Indicates the equals/==() method. */
      // EQUALS,

      ///** Indicates the hashCode() method. */
      // HASHCODE,

      /**
       * Indicates generation of the complete class including all
       * of its fields and getters/setters for those.
       */
      CLASS

    };
  };

  // Pure virtual methods defining the interface.

  /**
   * Get a list of options and usage for the code generator.
   *
   * @param options Output parameter for options along-with their usage.
   */
  virtual void getOptions(OptionMap& options) const = 0;

  /**
   * Initialize the code generator with the given properties.
   *
   * @param properties The set of property/value pairs provided by the
   *                   user. Implementations should modify this map so
   *                   as to remove the properties used by the
   *                   implementation. This should also match the usage
   *                   as provided by <code>getOptions</code> method.
   */
  virtual void init(PropertyMap& properties) = 0;

  /**
   * Initialize the code generator for the given class.
   *
   * @param classInfo <code>TypeInfo</code> of the class for which
   *                  <code>toData</code> and <code>fromData</code>
   *                  methods are to be generated.
   */
  virtual void initClass(const TypeInfo& classInfo) = 0;

  /**
   * Add any references to the generated code (e.g headers).
   *
   * @param references A vector of reference names.
   */
  virtual void addReferences(const ReferenceVector& references) = 0;

  virtual void addFileHeader(int, char**) = 0;

  /**
   * Generate code to mark the start of the class in the given namespaces.
   *
   * @param members Vector containing information of the members of the
   *                class that need to be auto-serialized.
   */
  virtual void startClass(const VariableVector& members) = 0;

  /**
   * Start of code generation for a method.
   *
   * @param type Type of the method to be generated.
   * @param varName Name of the variable (if any) used by the method
   *                e.g. <code>DataOutput</code> variable for toData().
   */
  virtual void startMethod(const Method::Type type, const std::string& varName,
                           const std::string& methodPrefix) = 0;

  // Ticket #905 Changes starts here
  virtual void addTryBlockStart(const Method::Type type) = 0;
  virtual void finishTryBlock(const Method::Type type) = 0;
  // Ticket #905 Changes ends here
  /**
   * Generate the method fragment for a given member of the class.
   *
   * @param type Type of the method to be generated.
   * @param varName Name of the variable (if any) used by the method
   *                e.g. <code>DataOutput</code> variable for toData().
   * @param var Information of the member variable of the class.
   */
  virtual void genMethod(const Method::Type type, const std::string& varName,
                         const VariableInfo& var) = 0;

  /**
   * End of code generation for a method.
   *
   * @param type Type of the method to be generated.
   * @param varName Name of the variable (if any) used by the method
   *                e.g. <code>DataOutput</code> variable for toData().
   */
  virtual void endMethod(const Method::Type type,
                         const std::string& outputVarName) = 0;

  /**
   * Generate the code for typeId function of gemfire
   * Serializable/DataSerializable interface.
   */
  virtual void genTypeId(const std::string& methodPrefix) = 0;

  virtual void genClassNameMethod(std::map<std::string, std::string>&,
                                  const std::string& methodPrefix) = 0;

  virtual void genCreateDeserializable(const std::string& methodPrefix) = 0;

  /**
   * Generate code to mark the end of the class in the given namespaces.
   */
  virtual void endClass() = 0;

  /**
   * Any cleanup that may be required in case of abnormal termination.
   */
  virtual void cleanup() = 0;

  /** virtual destructor. */
  virtual ~CodeGenerator() {}

  // End pure virtual methods
};
}
}

#endif  // _GFAS_CODEGENERATOR_HPP_
