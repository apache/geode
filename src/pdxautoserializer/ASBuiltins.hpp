/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef _GFAS_ASBUILTINS_HPP_
#define _GFAS_ASBUILTINS_HPP_

#include "DataOutput.hpp"
#include "DataInput.hpp"
#include "Serializer.hpp"
#include <string>
#include <vector>
#include <map>

namespace gemfire {
namespace serializer {
/** write an <code>std::string</code> object to <code>DataOutput</code> */
inline void writeObject(gemfire::DataOutput& output, const std::string& value) {
  output.writeASCIIHuge(value.data(), value.length());
}

/** read an <code>std::string</code> object from <code>DataInput</code> */
inline void readObject(gemfire::DataInput& input, std::string& value) {
  char* str;
  uint32_t len;
  input.readASCIIHuge(&str, &len);
  value.assign(str, len);
  input.freeUTFMemory(str);
}

/** write an <code>std::wstring</code> object to <code>DataOutput</code> */
inline void writeObject(gemfire::DataOutput& output,
                        const std::wstring& value) {
  output.writeUTFHuge(value.data(), value.length());
}

/** read an <code>std::wstring</code> object from <code>DataInput</code> */
inline void readObject(gemfire::DataInput& input, std::wstring& value) {
  wchar_t* str;
  uint32_t len;
  input.readUTFHuge(&str, &len);
  value.assign(str, len);
  input.freeUTFMemory(str);
}

/** write an <code>std::vector</code> object to <code>DataOutput</code> */
template <typename TObj>
inline void writeObject(gemfire::DataOutput& output,
                        const std::vector<TObj>& value) {
  output.writeInt(value.size());
  for (std::vector<TObj>::const_iterator valIterator = value.begin();
       valIterator != value.end(); ++valIterator) {
    writeObject(output, *valIterator);
  }
}

/** read an <code>std::vector</code> object from <code>DataInput</code> */
template <typename TObj>
inline void readObject(gemfire::DataInput& input, std::vector<TObj>& value) {
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
inline void writeObject(gemfire::DataOutput& output,
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
inline void readObject(gemfire::DataInput& input, std::map<TKey, TVal>& value) {
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

#endif  // _GFAS_ASBUILTINS_HPP_
