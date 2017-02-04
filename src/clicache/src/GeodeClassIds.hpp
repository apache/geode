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

#pragma once



#include "gf_defs.hpp"
#include <gfcpp/GeodeTypeIds.hpp>


namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      struct PdxTypes
      {
        enum PdxTypesInternal
        {
          BOOLEAN,
          BYTE,
          CHAR,
          SHORT,
          INT,
          LONG,
          FLOAT,
          DOUBLE,
          DATE,
          STRING,
          OBJECT,
          BOOLEAN_ARRAY,
          CHAR_ARRAY,
          BYTE_ARRAY,
          SHORT_ARRAY,
          INT_ARRAY,
          LONG_ARRAY,
          FLOAT_ARRAY,
          DOUBLE_ARRAY,
          STRING_ARRAY,
          OBJECT_ARRAY,
          ARRAY_OF_BYTE_ARRAYS
        };
      };

      /// <summary>
      /// Static class containing the classIds of the built-in cacheable types.
      /// </summary>
      public ref class GeodeClassIds
      {
      public:

        /// <summary>
        /// ClassId of <c>Properties</c> class
        /// </summary>
        literal uint32_t Properties =
          apache::geode::client::GeodeTypeIds::Properties + 0x80000000;

        /// <summary>        
        /// ClassId of <c>CharArray</c> class
        /// </summary>
        literal uint32_t CharArray =
          apache::geode::client::GeodeTypeIds::CharArray + 0x80000000;

        /// <summary>
        /// ClassId of <c>BooleanArray</c> class
        /// </summary>
        literal uint32_t BooleanArray =
          apache::geode::client::GeodeTypeIds::BooleanArray + 0x80000000;

        /// <summary>
        /// ClassId of <c>RegionAttributes</c> class
        /// </summary>
        literal uint32_t RegionAttributes =
          apache::geode::client::GeodeTypeIds::RegionAttributes + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableUndefined</c> class
        /// Implementation note: this has DSFID of FixedIDByte hence a
        /// different increment.
        /// </summary>
        literal uint32_t CacheableUndefined =
          apache::geode::client::GeodeTypeIds::CacheableUndefined + 0xa0000000;

        literal uint32_t EnumInfo =
          apache::geode::client::GeodeTypeIds::EnumInfo + 0xa0000000;

        /// <summary>
        /// ClassId of <c>Struct</c> class
        /// </summary>
        literal uint32_t Struct =
          apache::geode::client::GeodeTypeIds::Struct + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableString</c> class
        /// </summary>
        literal uint32_t CacheableString =
          apache::geode::client::GeodeTypeIds::CacheableString + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableString</c> class for huge strings
        /// </summary>
        literal uint32_t CacheableStringHuge =
          apache::geode::client::GeodeTypeIds::CacheableStringHuge + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableBytes</c> class
        /// </summary>
        literal uint32_t CacheableBytes =
          apache::geode::client::GeodeTypeIds::CacheableBytes + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableInt16Array</c> class
        /// </summary>
        literal uint32_t CacheableInt16Array =
          apache::geode::client::GeodeTypeIds::CacheableInt16Array + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableInt32Array</c> class
        /// </summary>
        literal uint32_t CacheableInt32Array =
          apache::geode::client::GeodeTypeIds::CacheableInt32Array + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableInt64Array</c> class
        /// </summary>
        literal uint32_t CacheableInt64Array =
          apache::geode::client::GeodeTypeIds::CacheableInt64Array + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableFloatArray</c> class
        /// </summary>
        literal uint32_t CacheableFloatArray =
          apache::geode::client::GeodeTypeIds::CacheableFloatArray + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableDoubleArray</c> class
        /// </summary>
        literal uint32_t CacheableDoubleArray =
          apache::geode::client::GeodeTypeIds::CacheableDoubleArray + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableVector</c> class for object arrays
        /// </summary>
        literal uint32_t CacheableObjectArray =
          apache::geode::client::GeodeTypeIds::CacheableObjectArray + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableBoolean</c> class
        /// </summary>
        literal uint32_t CacheableBoolean =
          apache::geode::client::GeodeTypeIds::CacheableBoolean + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableInt16</c> class for wide-characters
        /// </summary>
        literal uint32_t CacheableCharacter =
          apache::geode::client::GeodeTypeIds::CacheableWideChar + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableByte</c> class
        /// </summary>
        literal uint32_t CacheableByte =
          apache::geode::client::GeodeTypeIds::CacheableByte + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableInt16</c> class
        /// </summary>
        literal uint32_t CacheableInt16 =
          apache::geode::client::GeodeTypeIds::CacheableInt16 + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableInt32</c> class
        /// </summary>
        literal uint32_t CacheableInt32 =
          apache::geode::client::GeodeTypeIds::CacheableInt32 + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableInt64</c> class
        /// </summary>
        literal uint32_t CacheableInt64 =
          apache::geode::client::GeodeTypeIds::CacheableInt64 + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableFloat</c> class
        /// </summary>
        literal uint32_t CacheableFloat =
          apache::geode::client::GeodeTypeIds::CacheableFloat + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableDouble</c> class
        /// </summary>
        literal uint32_t CacheableDouble =
          apache::geode::client::GeodeTypeIds::CacheableDouble + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableDate</c> class
        /// </summary>
        literal uint32_t CacheableDate =
          apache::geode::client::GeodeTypeIds::CacheableDate + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableFileName</c> class
        /// </summary>
        literal uint32_t CacheableFileName =
          apache::geode::client::GeodeTypeIds::CacheableFileName + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableStringArray</c> class
        /// </summary>
        literal uint32_t CacheableStringArray =
          apache::geode::client::GeodeTypeIds::CacheableStringArray + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableVector</c> class
        /// </summary>
        literal uint32_t CacheableVector =
          apache::geode::client::GeodeTypeIds::CacheableVector + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableStack</c> class
        /// </summary>
        literal uint32_t CacheableStack =
          apache::geode::client::GeodeTypeIds::CacheableStack + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableArrayList</c> class
        /// </summary>
        literal uint32_t CacheableArrayList =
          apache::geode::client::GeodeTypeIds::CacheableArrayList + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableArrayList</c> class
        /// </summary>
        literal uint32_t CacheableLinkedList =
          apache::geode::client::GeodeTypeIds::CacheableLinkedList + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableHashSet</c> class
        /// </summary>
        literal uint32_t CacheableHashSet =
          apache::geode::client::GeodeTypeIds::CacheableHashSet + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableLinkedHashSet</c> class
        /// </summary>
        literal uint32_t CacheableLinkedHashSet =
          apache::geode::client::GeodeTypeIds::CacheableLinkedHashSet + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableHashMap</c> class
        /// </summary>
        literal uint32_t CacheableHashMap =
          apache::geode::client::GeodeTypeIds::CacheableHashMap + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableHashTable</c> class
        /// </summary>
        literal uint32_t CacheableHashTable =
          apache::geode::client::GeodeTypeIds::CacheableHashTable + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableIdentityHashMap</c> class
        /// </summary>
        literal uint32_t CacheableIdentityHashMap =
          apache::geode::client::GeodeTypeIds::CacheableIdentityHashMap + 0x80000000;

        /// <summary>
        /// Not used.
        /// </summary>
        literal uint32_t CacheableTimeUnit =
          apache::geode::client::GeodeTypeIds::CacheableTimeUnit + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableString</c> class for null strings
        /// </summary>
        literal uint32_t CacheableNullString =
          apache::geode::client::GeodeTypeIds::CacheableNullString + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableString</c> class for ASCII strings
        /// </summary>
        literal uint32_t CacheableASCIIString =
          apache::geode::client::GeodeTypeIds::CacheableASCIIString + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableString</c> class for huge ASCII strings
        /// </summary>
        literal uint32_t CacheableASCIIStringHuge =
          apache::geode::client::GeodeTypeIds::CacheableASCIIStringHuge + 0x80000000;


        // Built-in managed types.

        /// <summary>
        /// ClassId of <c>CacheableObject</c> class
        /// </summary>
        literal uint32_t CacheableManagedObject = 7 + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableObjectXml</c> class
        /// </summary>
        literal uint32_t CacheableManagedObjectXml = 8 + 0x80000000;
      internal:

        literal uint32_t PdxType = apache::geode::client::GeodeTypeIds::PdxType + 0x80000000;

        literal uint32_t DATA_SERIALIZABLE = 45;
        literal uint32_t JAVA_CLASS = 43;

        //internal geode typeids..
        /*  literal Byte USERCLASS = 40;
          literal Byte USERMAP = 94;
          literal Byte USERCOLLECTION = 95;
          literal Byte ARRAYOFBYTEARRAYS = 91;
          literal Byte GEODEREGION =  98;

          literal Byte BOOLEAN_TYPE = 17;
          literal Byte CHARACTER_TYPE = 18;
          literal Byte BYTE_TYPE = 19;
          literal Byte SHORT_TYPE = 20;
          literal Byte INTEGER_TYPE = 21;
          literal Byte LONG_TYPE = 22;
          literal Byte FLOAT_TYPE = 23;
          literal Byte DOUBLE_TYPE = 24;
          literal Byte VOID_TYPE = 25;   */

        literal Byte PDX = 93;
        literal Byte PDX_ENUM = 94;

        literal Byte BYTE_SIZE = 1;

        literal Byte BOOLEAN_SIZE = 1;

        literal Byte CHAR_SIZE = 2;

        literal Byte SHORT_SIZE = 2;

        literal Byte INTEGER_SIZE = 4;

        literal Byte FLOAT_SIZE = 4;

        literal Byte LONG_SIZE = 8;

        literal Byte DOUBLE_SIZE = 8;

        literal Byte DATE_SIZE = 8;
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache


