/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GFAS_INPUTPARSERFACTORY_HPP_
#define _GFAS_INPUTPARSERFACTORY_HPP_

#include "InputParser.hpp"
#include <utility>

namespace gemfire {
namespace pdx_auto_serializer {
/**
 * Defines a function returning a pointer to an
 * <code>InputParser</code> object.
 *
 * @return The <code>InputParser</code> object.
 */
typedef InputParser* (*InputParserFn)(void);

/**
 * Factory class to obtain instances of <code>InputParser</code>
 * implementations.
 */
class InputParserFactory {
 public:
  /**
   * Default constructor that registers all the available
   * <code>InputParser</code> implementations.
   */
  InputParserFactory();

  /**
   * Get an instance of an <code>InputParser</code> using name of a parser.
   *
   * @param parserName The name of the parser frontend.
   * @return An instance of <code>InputParser</code>.
   */
  InputParser* getInstance(const std::string& parserName) const;

  /** Get a list of all registered parser frontends. */
  StringVector getParsers() const;

  /** Virtual destructor. */
  virtual ~InputParserFactory();

 private:
  /**
   * The map containing the mappings from the names of parsers to their
   * factory functions.
   */
  std::map<std::string, InputParserFn> m_parserMap;
};
}
}

#endif  // _GFAS_INPUTPARSERFACTORY_HPP_
