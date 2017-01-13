/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

#include "../base_types.hpp"
#include "../InputParserFactory.hpp"
#include "../CodeGeneratorFactory.hpp"
#include "../CodeGenerator.hpp"
#include "Log.hpp"
#include "Helper.hpp"
#include <iostream>

/**
* @namespace gemfire::pdx_auto_serializer The namespace containing the
*                                     auto-serializer classes.
*/
using namespace gemfire::pdx_auto_serializer;

/** The name used for invocation of the program on command-line. */
std::string progName;
/** The parser factory object. */
InputParserFactory parserFactory;
/** The code generator factory object. */
CodeGeneratorFactory generatorFactory;
/** The parser returned by the parser factory. */
InputParser* parser = NULL;
/** The list of classes given by the parser. */
ASClassVector classes;
/** The <code>CodeGenerator</code> object returned by the factory. */
CodeGenerator* codeGenerator = NULL;
/**
* The options with their usage for the tool with current parser and
* code generator.
*/
OptionMap toolOptions;

/** Name of this module. */
static std::string ModuleName = "PdxAutoSerializer";

/**
* The option used to specify the language of the code to be auto-serialized.
*/
static const std::string LanguageOption = "language";

/**
* The option used to specify the language in which the auto-serialized code
* is to be generated.
*/
static const std::string GeneratorOption = "generator";

/** The option name for class name. */
static const std::string ClassNameOption = "className";

/** The option name for class name String */
static const std::string getClassNameOption = "classNameStr";

/** The option used to obtain usage information. */
static const std::string UsageOption = "usage";

/** The option for help -- gives same output as usage for now. */
static const std::string HelpOption = "help";

/** the option for directory to generate auto-generated files */
static const std::string outDirectoryOption = "outDir";

/** Fill in the default top-level options required for the tool. */
void addMainOptions();

/**
* Parse the command-line and get the resource names and the properties.
*
* @param argc The number of command-line arguments.
* @param argv The string array containing the command-line arguments.
* @param resources The names of the resources.
* @param properties Output parameter containing the properties as a map.
*/
void parseCommandLine(int argc, char** argv, StringVector& resources,
                      PropertyMap& properties);

/**
* Show the program usage. If any language/generator has been selected then
* its options are also shown.
*/
void showUsage();

/** Cleanly destroy all the global objects. */
void cleanupObjects();

/** Cleanup any files or other artifacts in case of abnormal exit. */
void cleanup();

/**
* The main entry point of the tool.
*
* @param argc The number of command-line arguments.
* @param argv The string array containing the command-line arguments.
*/

static void Tokenize(const std::string& str, std::vector<std::string>& tokens,
                     const std::string& delimiters = " ") {
  std::string::size_type lastPos = str.find_first_not_of(delimiters, 0);
  std::string::size_type pos = str.find_first_of(delimiters, lastPos);
  while (std::string::npos != pos || std::string::npos != lastPos) {
    tokens.push_back(str.substr(lastPos, pos - lastPos));
    lastPos = str.find_first_not_of(delimiters, pos);
    pos = str.find_first_of(delimiters, lastPos);
  }
}
int main(int argc, char** argv) {
  bool success = false;
  struct CleanUp {
   private:
    bool& m_success;

   public:
    explicit CleanUp(bool& success) : m_success(success) {}
    ~CleanUp() {
      if (m_success) {
        cleanupObjects();
      } else {
        cleanup();
      }
    }
  } cleanOnExit(success);

  try {
    StringVector resources;
    PropertyMap properties;
    const std::string languageString = "C++";
    const std::string generatorString = "C++";
    StringVector classNames;

    progName = argv[0];
    std::string::size_type baseIndex = progName.find_last_of("/\\");
    if (baseIndex != std::string::npos) {
      progName = progName.substr(baseIndex + 1);
    }
    addMainOptions();
    parseCommandLine(argc, argv, resources, properties);
    std::map<std::string, std::string> classNameStringMap;

    try {
      if (languageString.length() > 0 &&
          (parser = parserFactory.getInstance(languageString)) == NULL) {
        throw std::invalid_argument("No such language: " + languageString);
      }

      if (generatorString.length() > 0 &&
          (codeGenerator = generatorFactory.getInstance(generatorString)) ==
              NULL) {
        throw std::invalid_argument("No such code generator: " +
                                    generatorString);
      }

      if (parser != NULL) {
        parser->getOptions(toolOptions);
      }
      if (codeGenerator != NULL) {
        codeGenerator->getOptions(toolOptions);
      }

      std::string usageString;
      if (Helper::getSingleProperty(properties, UsageOption, usageString) ||
          Helper::getSingleProperty(properties, HelpOption, usageString)) {
        showUsage();
        return 0;
      }

      if (parser == NULL) {
        throw std::invalid_argument("No input language specified.");
      }
      if (codeGenerator == NULL) {
        throw std::invalid_argument("No output language specified.");
      }
      if (resources.size() == 0) {
        throw std::invalid_argument("No input resources specified.");
      }

      if (!Helper::getMultiProperty(properties, ClassNameOption, classNames) ||
          classNames.size() == 0) {
        Log::warn(ModuleName,
                  "No class name specified; "
                  "will serialize all classes.");
      }

      std::string classNameString;

      std::vector<std::string> classNameStringVector;

      Helper::getMultiProperty(properties, getClassNameOption,
                               classNameStringVector);
      for (std::vector<std::string>::iterator itr =
               classNameStringVector.begin();
           itr != classNameStringVector.end(); ++itr) {
        std::vector<std::string> eachClassNameVector;
        Tokenize(*itr, eachClassNameVector, ":");
        if (eachClassNameVector.size() == 2) {
          classNameStringMap.insert(std::pair<std::string, std::string>(
              eachClassNameVector[0], eachClassNameVector[1]));
        } else {
          throw std::invalid_argument("No input class name string specified.");
        }
      }

      parser->init(properties);
      parser->selectClasses(resources, classNames);
      parser->getSelectedClasses(classes);

      codeGenerator->init(properties);

    } catch (const std::exception& ex) {
      std::cerr << Helper::typeName(ex) << ": " << ex.what() << std::endl;
      std::cerr << "Use --help or --usage option for help" << std::endl;
      return 1;
    }

    if (properties.size() > 0) {
      showUsage();
      return 1;
    }

    Log::info(ModuleName, "Classes being serialized:");
    for (ASClassVector::const_iterator classIterator = classes.begin();
         classIterator != classes.end(); ++classIterator) {
      Log::info(ModuleName, '\t' + (*classIterator)->getName());
    }

    for (ASClassVector::const_iterator classIterator = classes.begin();
         classIterator != classes.end(); ++classIterator) {
      ReferenceVector references;
      TypeInfo classInfo;
      VariableVector members;
      std::string varName = "__var";

      const ClassInfo* currentClass = *classIterator;

      try {
        currentClass->getTypeInfo(classInfo);
        codeGenerator->initClass(classInfo);
      } catch (const std::exception& ex) {
        std::cerr << Helper::typeName(ex) << ": " << ex.what() << std::endl;
        std::cerr << "Use --help or --usage option for help" << std::endl;
        return 1;
      }

      currentClass->getReferences(references);
      currentClass->getMembers(members);

      codeGenerator->addFileHeader(argc, argv);
      codeGenerator->addReferences(references);
      codeGenerator->startClass(members);

      std::vector<CodeGenerator::Method::Type> methods;
      methods.push_back(CodeGenerator::Method::TODATA);
      methods.push_back(CodeGenerator::Method::FROMDATA);
      for (size_t index = 0; index < methods.size(); ++index) {
        CodeGenerator::Method::Type method = methods[index];
        codeGenerator->startMethod(method, varName,
                                   currentClass->getMethodPrefix());
        // Adding try block
        // Ticket #905 Changes starts here
        codeGenerator->addTryBlockStart(method);
        // Ticket #905 Changes ends here
        for (VariableVectorIterator memberIterator = members.begin();
             memberIterator != members.end(); ++memberIterator) {
          codeGenerator->genMethod(method, varName, *memberIterator);
        }
        // Finish try block
        // Ticket #905 Changes starts here
        codeGenerator->finishTryBlock(method);
        // Ticket #905 Changes ends here
        codeGenerator->endMethod(method, varName);
      }
      codeGenerator->genClassNameMethod(classNameStringMap,
                                        currentClass->getMethodPrefix());
      codeGenerator->genCreateDeserializable(currentClass->getMethodPrefix());
      codeGenerator->genTypeId(currentClass->getMethodPrefix());
      codeGenerator->endClass();
    }
  } catch (const std::invalid_argument& ex) {
    std::cerr << "Use --help or --usage option for help" << std::endl;
    return 1;
  } catch (const std::exception& ex) {
    std::cerr << Helper::typeName(ex) << ": " << ex.what() << std::endl;
    return 1;
  } catch (...) {
    std::cerr << "Caught an unknown exception." << std::endl;
    return 1;
  }
  success = true;
  return 0;
}

void addMainOptions() {
  std::pair<bool, std::string> optionPair;
  // std::string languageStr;
  // StringVector languages = parserFactory.getParsers();
  // assert(languages.size() > 0);
  // StringVectorIterator languageIterator = languages.begin();
  // languageStr = *languageIterator;
  // while (++languageIterator != languages.end()) {
  //    languageStr += ',' + *languageIterator;
  //}
  // optionPair.first = true;
  ///*optionPair.second = "Generate code for the given language "
  //    "-- one of " + languageStr + " (SINGLE)";
  // toolOptions[LanguageOption] = optionPair;*/

  // std::string generatorStr;
  // StringVector generators = generatorFactory.getGenerators();
  // assert(generators.size() > 0);
  // StringVectorIterator generatorIterator = generators.begin();
  // generatorStr = *generatorIterator;
  // while (++generatorIterator != generators.end()) {
  //    generatorStr += ',' + *generatorIterator;
  //}
  /*optionPair.second = "Generate code in the given generator "
  "-- one of " + generatorStr + " (SINGLE)";
  toolOptions[GeneratorOption] = optionPair;*/

  optionPair.first = true;
  optionPair.second =
      "Name of the class for which to generate "
      "auto-serialization code (MULTIPLE,OPTIONAL)";
  toolOptions[ClassNameOption] = optionPair;

  optionPair.first = false;
  optionPair.second = (std::string) "\t\tThis usage message.";
  toolOptions[UsageOption] = optionPair;

  optionPair.first = false;
  optionPair.second = (std::string) "\t\tThis help message.";
  toolOptions[HelpOption] = optionPair;

  optionPair.first = true;
  optionPair.second = (std::string) "Name of the class in string representation"
    "(MULTIPLE,OPTIONAL)";
  toolOptions[getClassNameOption] = optionPair;

  optionPair.first = false;
  optionPair.second = (std::string) "\tName of the directory where auto-generated"
    "files will be written, default is current working directory";
  toolOptions[outDirectoryOption] = optionPair;
}

void parseCommandLine(int argc, char** argv, StringVector& resources,
                      PropertyMap& properties) {
  if (argc < 2) {
    throw std::invalid_argument("");
  }
  for (int propertyIndex = 1; propertyIndex < argc; propertyIndex++) {
    char* arg = argv[propertyIndex];
    if (arg[0] == '-' && arg[1] == '-') {
      // Is a property value option.
      std::string prop = arg + 2;
      std::string propertyName;
      std::string propertyValue;
      std::string::size_type valPos;
      if ((valPos = prop.find('=')) != std::string::npos) {
        propertyName = prop.substr(0, valPos);
        propertyValue = prop.substr(valPos + 1);
      } else {
        propertyName = prop;
      }
      PropertyMap::iterator propertyFind = properties.find(propertyName);
      if (propertyFind == properties.end()) {
        StringVector propertyValues;
        if (propertyValue.length() > 0) {
          propertyValues.push_back(propertyValue);
        }
        properties[propertyName] = propertyValues;
      } else if (propertyValue.length() > 0) {
        propertyFind->second.push_back(propertyValue);
      }
    } else {
      resources.push_back(arg);
    }
  }
}

void showUsage() {
  std::cout << "Usage: " << progName << " [OPTIONS] <resources e.g. "
                                        "header> ...\n\n";

  std::cout << "Resource name should be the path to the header "
               "containing the classes to be auto-serialized.\n\n";
  std::cout
      << "Options may be one of those given below.\nSINGLE denotes "
         "that the option should be specified only once.\nMULTIPLE denotes "
         "that the "
         "option can be specified more than once.\nOPTIONAL denotes that the "
         "option may be skipped in which case the default for that shall be "
         "chosen.";
  std::cout << '\n' << std::endl;
  for (OptionMap::const_iterator optionIterator = toolOptions.begin();
       optionIterator != toolOptions.end(); ++optionIterator) {
    std::cout << "--" << optionIterator->first;
    if (optionIterator->second.first) {
      std::cout << "=VALUE";
    }
    std::cout << '\t' << optionIterator->second.second << '\n';
  }
  std::cout << std::endl;

  std::cout << "Examples:" << std::endl;
  std::cout << "\t pdxautoserializer -outDir=<DIR NAME> <RESOURCE>"
            << std::endl;
  std::cout
      << "\t pdxautoserializer -outDir=<DIR NAME> --className=<CLASSNAME1> ";
  std::cout << "--className=<CLASSNAME2> <RESOURCE>" << std::endl;
  std::cout << "\t pdxautoserializer -outDir=<DIR NAME> "
               "--classNameStr=<CLASSNAME1:User defined String> ";
  std::cout << "--classNameStr=<CLASSNAME:User defined String> <RESOURCE>"
            << std::endl
            << std::endl;
  std::cout << "Helper Macros to be defined in Input Header File : "
            << std::endl;
  std::cout << "GFINCLUDE\t for including a specific member for serialization"
            << std::endl;
  std::cout << "GFEXCLUDE\t for excluding a specific member for serialization"
            << std::endl;
  std::cout << "GFID\t\t for considering a member as Identify Field"
            << std::endl;
  std::cout << "GFARRAYSIZE\t for specifying a array length member"
            << std::endl;
  std::cout << "GFIGNORE\t for ignoring certain keywords" << std::endl;
  std::cout << "For more details refer to documentation on this utility."
            << std::endl;
}

void cleanupObjects() {
  Helper::deleteASClasses(classes);
  if (parser != NULL) {
    delete parser;
    parser = NULL;
  }
  if (codeGenerator != NULL) {
    delete codeGenerator;
    codeGenerator = NULL;
  }
}

void cleanup() {
  if (codeGenerator != NULL) {
    codeGenerator->cleanup();
  }
  cleanupObjects();
}
