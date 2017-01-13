/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GFAS_CPPCODEGENERATOR_HPP_
#define _GFAS_CPPCODEGENERATOR_HPP_

#include "../CodeGenerator.hpp"
#include "../OutputFormatter.hpp"

namespace gemfire {
namespace pdx_auto_serializer {
/**
 * The C++ code generator backend.
 */
class CPPCodeGenerator : public CodeGenerator {
 public:
  // CodeGenerator method implementations

  virtual void getOptions(OptionMap& options) const;

  virtual void init(PropertyMap& properties);

  virtual void initClass(const TypeInfo& classInfo);

  virtual void addReferences(const ReferenceVector& references);
  virtual void addFileHeader(int, char**);

  virtual void startClass(const VariableVector& members);

  virtual void startMethod(const Method::Type type, const std::string& varName,
                           const std::string& methodPrefix);

  // Ticket #905 Changes starts here
  virtual void addTryBlockStart(const Method::Type type);
  virtual void finishTryBlock(const Method::Type type);
  // Ticket #905 Changes ends here
  virtual void genMethod(const Method::Type type, const std::string& varName,
                         const VariableInfo& var);

  virtual void endMethod(const Method::Type type, const std::string& varName);

  virtual void genTypeId(const std::string& methodPrefix);
  virtual void genClassNameMethod(std::map<std::string, std::string>&,
                                  const std::string& methodPrefix);
  virtual void genCreateDeserializable(const std::string& methodPrefix);
  virtual void endClass();

  virtual void cleanup();

  // End CodeGenerator implementations

  /**
   * Static factory function to create an object of
   * <code>CPPCodeGenerator</code> class. This is registered with the
   * <code>CodeGeneratorFactory</code>.
   *
   * @return An instance of <code>CPPCodeGenerator</code>.
   */
  static CodeGenerator* create();

  /** Virtual destructor. */
  virtual ~CPPCodeGenerator();

 protected:
  /**
   * Get the default suffix to use for generated files and classes.
   */
  virtual std::string defaultGenSuffix() const;

  /**
   * Get the prefix for a namespace nesting.
   *
   * @param namespaces The nested namespace names.
   * @return Prefix for nested namespaces.
   */
  virtual std::string getNamespacePrefix(const StringVector& namespaces) const;

  /**
   * Get the string representation for a given type.
   *
   * @param type Reference to the <code>TypeInfo</code> for the type.
   * @param prependNS Whether to prepend the namespace to the type.
   * @param postVarStr Returns a string that may be required after
   *                      the variable (for C++ arrays). If this is
   *                      NULL, then it is assumed to be in return type
   *                      where there is no variable.
   * @return The string representation of the type.
   */
  virtual std::string getTypeString(const TypeInfo& type,
                                    bool prependNS = false,
                                    std::string* postVarStr = NULL,
                                    StringSet* templateArgs = NULL) const;

  /**
   * Generate the namespace header for the given list of namespaces.
   *
   * @param namespaces The vector of namespaces.
   * @param formatter The formatter to use for generating the output.
   */
  virtual void genNamespaceHeader(const StringVector& namespaces,
                                  OutputFormatter* formatter);

  /**
   * Generate the function header with the given name, arguments,
   * return type and in the given class.
   *
   * @param functionName The name of the function.
   * @param className The name of the class containing the function.
   * @param returnType The return type of the function.
   * @param arguments The list of arguments to the function.
   * @param isDefinition Whether to generate a definition or declaration.
   * @param isConst Whether the method is a const method.
   * @param formatter The formatter to use for generating the output.
   */
  virtual void genFunctionHeader(const std::string& functionName,
                                 const std::string& className,
                                 const std::string& returnType,
                                 const StringVector& arguments,
                                 bool isDefinition, bool isConst,
                                 OutputFormatter* formatter,
                                 const std::string& methodPrefix);

  /**
   * Generate the function footer.
   *
   * @param formatter The formatter to use for generating the footer.
   */
  virtual void genFunctionFooter(OutputFormatter* formatter);

  /**
   * Generate the namespace footer for the given list of namespaces.
   *
   * @param namespaces The vector of namespaces.
   * @param formatter The formatter to use for generating the output.
   */
  virtual void genNamespaceFooter(const StringVector& namespaces,
                                  OutputFormatter* formatter);

  /**
   * Default constructor -- this is not exposed to public which should
   * use the {@link CPPCodeGenerator::create} function.
   */
  CPPCodeGenerator();

  /**
   * The <code>OutputFormatter</code> to be used for writing the output
   * cpp file.
   */
  OutputFormatter* m_cppFormatter;

  /**
   * <code>TypeInfo</code> of the class for which <code>toData</code>
   * and <code>fromData</code> methods are to be generated.
   */
  TypeInfo m_classInfo;

  /** The name of the output directory. */
  std::string m_outDir;

  /**
   * The suffix to be used for generated files and classes -- default is
   * given by <code>defaultGenSuffix</code>.
   */
  std::string m_genSuffix;

  /**
   * The directory of the header file to be used in the '#include' in the
   * generated files.
   */
  std::string m_headerDir;

  /**
   * The name of the variable of this class that is passed to static
   * <code>writeObject/readObject</code> methods.
   */
  std::string m_objPrefix;

  /** The name of this module to be used for logging. */
  std::string m_moduleName;

  /** The current classId being used for this class. */
  static int s_classId;

  // Constants

  /**
   * The namespace containing the global overloaded <code>writeObject</code>
   * and <code>readObject</code> methods for builtin types.
   */
  static std::string s_GFSerializerNamespace;

  /**
   * The prefix to use for declaring temporary and function argument
   * variables.
   */
  static std::string s_TempVarPrefix;

  /** The option name for classId. */
  static std::string s_ClassIdOption;

  /** The option name for the output directory. */
  static std::string s_OutDirOption;

  /**
   * The directory to be used in generated files for the included headers.
   * If not provided the path of the header file as provided on
   * command-line is used.
   */
  static std::string s_HeaderDirOption;

  /**
   * The option name for the suffix to use for generated files and classes.
   */
  static std::string s_GenSuffixOption;
};
}
}

#endif  // _GFAS_CPPCODEGENERATOR_HPP_
