#pragma once

#ifndef GEODE_PDXTYPES_H_
#define GEODE_PDXTYPES_H_

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

#include <gfcpp/gfcpp_globals.hpp>

class PdxTypes {
 public:
  static const int8_t BYTE_SIZE = 1;

  static const int8_t BOOLEAN_SIZE = 1;

  static const int8_t CHAR_SIZE = 2;

  static const int8_t SHORT_SIZE = 2;

  static const int8_t INTEGER_SIZE = 4;

  static const int8_t FLOAT_SIZE = 4;

  static const int8_t LONG_SIZE = 8;

  static const int8_t DOUBLE_SIZE = 8;

  static const int8_t DATE_SIZE = 8;
};

#endif // GEODE_PDXTYPES_H_
