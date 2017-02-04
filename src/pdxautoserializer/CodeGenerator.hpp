#pragma once

#ifndef APACHE_GEODE_GUARD_fb876ec7655e479e35df4a1cc2653f47
#define APACHE_GEODE_GUARD_fb876ec7655e479e35df4a1cc2653f47

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

namespace apache {
namespace geode {
namespace client {
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
   * Generate the code for typeId function of geode
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
}  // namespace pdx_auto_serializer
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // APACHE_GEODE_GUARD_fb876ec7655e479e35df4a1cc2653f47
