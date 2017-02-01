#pragma once

#ifndef APACHE_GEODE_GUARD_906466ac6028b9eac9226aa77077245f
#define APACHE_GEODE_GUARD_906466ac6028b9eac9226aa77077245f

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
#include "CodeGenerator.hpp"

namespace apache {
namespace geode {
namespace client {
namespace pdx_auto_serializer {
// Forward declaration.
class InputParser;

/**
 * This class describes the interface to be implemented by the
 * front-end parsers to represent each class in the resource.
 */
class ClassInfo {
 public:
  /**
   * Destruct the class.
   */
  virtual ~ClassInfo() {}

  /**
   * Initialize with a handle to <code>InputParser</code>.
   *
   * @param parser Pointer to the <code>InputParser</code> object.
   */
  virtual void init(InputParser* parser);

  /**
   * Get the name of the class.
   *
   * @return The name of this class.
   */
  virtual std::string getName() const = 0;

  /**
   * Get the set of references required for the class.
   *
   * @param references The vector of references.
   */
  virtual void getReferences(ReferenceVector& references) const = 0;

  /**
   * Get the class information (with the namespace information) for this
   * class.
   *
   * @param classType The <code>TypeInfo</code> for the class.
   */
  virtual void getTypeInfo(TypeInfo& classType) const = 0;

  /**
   * Get the list of members of this class to be serialized/deserialized.
   *
   * @param members The vector of variables that contains the list
   *                of variables that need to be serialized/deserialized.
   */
  virtual void getMembers(VariableVector& members) const = 0;

  virtual std::string getMethodPrefix() const = 0;

 protected:
  /**
   * The <code>InputParser</code> object for which classes are being
   * extracted.
   */
  InputParser* m_parser;
};

/** Shorthand for a vector of <code>ClassInfo</code> objects. */
typedef std::vector<const ClassInfo*> ASClassVector;

/**
 * Shorthand for a map of class names to corresponding
 * <code>ClassInfo</code> objects.
 */
typedef std::map<std::string, const ClassInfo*> ASClassMap;

/**
 * Shorthand for a map of class names to corresponding
 * <code>ClassInfo</code> objects with a flag for selection.
 */
typedef std::map<std::string, std::pair<ClassInfo*, bool> > ASClassFlagMap;

/**
 * This class describes the interface to be implemented by front-end
 * parsers.
 */
class InputParser {
 public:
  /**
   * Destruct the parser.
   */
  virtual ~InputParser() {}

  /**
   * Get a list of options and usage for the parser.
   *
   * @param options Output parameter for options along-with their usage.
   */
  virtual void getOptions(OptionMap& options) const = 0;

  /**
   * Initialize the parser with the given properties.
   *
   * @param properties The set of properties for the resource as given
   *                   on the command-line. The function should modify
   *                   this map so as to remove the properties used by
   *                   the implementation. This should also match the
   *                   usage as provided by
   *                   <code>InputParser::getOptions</code> method.
   */
  virtual void init(PropertyMap& properties) = 0;

  /**
   * Select some or all classes in the given resources.
   *
   * @param resources The resources to parse.
   * @param classNames The names of the classes which constitute the
   *                   initial selection as specified by the user.
   *                   If this is empty then all the classes are marked
   *                   as selected.
   */
  virtual void selectClasses(const StringVector& resources,
                             const StringVector& classNames) = 0;

  /**
   * Add the given class to the current selection of classes.
   *
   * @param className The name of the new class to be added.
   * @return True if the given class name exists in the global list.
   */
  virtual bool select(const std::string& className);

  /**
   * Search the given class name in the global list of classes.
   *
   * @param className The name of the class to search.
   * @return True if the given class name exists in the global list.
   */
  virtual bool contains(const std::string& className) const;

  /**
   * Get the selected list of classes.
   *
   * @param classes Output parameter containing the vector of
   *                selected classes.
   */
  virtual void getSelectedClasses(ASClassVector& classes) const;

 protected:
  /**
   * Stores the set of all the classes with the classes selected so far
   * as marked selected.
   */
  ASClassFlagMap m_classes;
};
}  // namespace pdx_auto_serializer
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // APACHE_GEODE_GUARD_906466ac6028b9eac9226aa77077245f
