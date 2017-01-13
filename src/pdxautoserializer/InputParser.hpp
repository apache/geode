/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GFAS_INPUTPARSER_HPP_
#define _GFAS_INPUTPARSER_HPP_

#include "base_types.hpp"
#include "CodeGenerator.hpp"

namespace gemfire {
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
}
}

#endif  // _GFAS_INPUTPARSER_HPP_
