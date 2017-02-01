#pragma once

#ifndef APACHE_GEODE_GUARD_eb7690d50321df9f22960c0ceecdeea5
#define APACHE_GEODE_GUARD_eb7690d50321df9f22960c0ceecdeea5

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


#include "DataOutput.hpp"
#include "DataInput.hpp"
#include "Serializer.hpp"
#include <string>
#include <vector>
#include <map>

namespace apache {
namespace geode {
namespace client {
namespace serializer {
/** write an <code>std::string</code> object to <code>DataOutput</code> */
inline void writeObject(apache::geode::client::DataOutput& output,
                        const std::string& value) {
  output.writeASCIIHuge(value.data(), value.length());
}

/** read an <code>std::string</code> object from <code>DataInput</code> */
inline void readObject(apache::geode::client::DataInput& input,
                       std::string& value) {
  char* str;
  uint32_t len;
  input.readASCIIHuge(&str, &len);
  value.assign(str, len);
  input.freeUTFMemory(str);
}

/** write an <code>std::wstring</code> object to <code>DataOutput</code> */
inline void writeObject(apache::geode::client::DataOutput& output,
                        const std::wstring& value) {
  output.writeUTFHuge(value.data(), value.length());
}

/** read an <code>std::wstring</code> object from <code>DataInput</code> */
inline void readObject(apache::geode::client::DataInput& input,
                       std::wstring& value) {
  wchar_t* str;
  uint32_t len;
  input.readUTFHuge(&str, &len);
  value.assign(str, len);
  input.freeUTFMemory(str);
}

/** write an <code>std::vector</code> object to <code>DataOutput</code> */
template <typename TObj>
inline void writeObject(apache::geode::client::DataOutput& output,
                        const std::vector<TObj>& value) {
  output.writeInt(value.size());
  for (std::vector<TObj>::const_iterator valIterator = value.begin();
       valIterator != value.end(); ++valIterator) {
    writeObject(output, *valIterator);
  }
}

/** read an <code>std::vector</code> object from <code>DataInput</code> */
template <typename TObj>
inline void readObject(apache::geode::client::DataInput& input,
                       std::vector<TObj>& value) {
  std::vector<TObj>::size_type len;
  input.readInt(&len);
  if (len > 0) {
    TObj obj;
    for (std::vector<TObj>::size_type index = 0; index < len; ++index) {
      readObject(input, obj);
      value.push_back(obj);
    }
  }
}

/** write an <code>std::map</code> object to <code>DataOutput</code> */
template <typename TKey, typename TVal>
inline void writeObject(apache::geode::client::DataOutput& output,
                        const std::map<TKey, TVal>& value) {
  output.writeInt(value.size());
  for (std::map<TKey, TVal>::const_iterator valIterator = value.begin();
       valIterator != value.end(); ++valIterator) {
    writeObject(output, valIterator->first);
    writeObject(output, valIterator->second);
  }
}

/** read an <code>std::map</code> object from <code>DataInput</code> */
template <typename TKey, typename TVal>
inline void readObject(apache::geode::client::DataInput& input,
                       std::map<TKey, TVal>& value) {
  std::map<TKey, TVal>::size_type len;
  input.readInt(&len);
  if (len > 0) {
    TKey key;
    TVal val;
    for (std::vector<TObj>::size_type index = 0; index < len; ++index) {
      readObject(input, key);
      readObject(input, val);
      value[key] = val;
    }
  }
}
}
}
}
}


#endif // APACHE_GEODE_GUARD_eb7690d50321df9f22960c0ceecdeea5
