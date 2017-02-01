#pragma once

#ifndef APACHE_GEODE_GUARD_6c288c7ce306f6376599e765eb252f29
#define APACHE_GEODE_GUARD_6c288c7ce306f6376599e765eb252f29

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


#include "InputParser.hpp"
#include <utility>

namespace apache {
namespace geode {
namespace client {
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
}  // namespace pdx_auto_serializer
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // APACHE_GEODE_GUARD_6c288c7ce306f6376599e765eb252f29
