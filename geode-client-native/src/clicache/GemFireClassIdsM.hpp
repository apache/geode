/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <cppcache/GemfireTypeIds.hpp>


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      /// <summary>
      /// Static class containing the classIds of the built-in cacheable types.
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class GemFireClassIds
      {
      public:

        /// <summary>
        /// ClassId of <c>Properties</c> class
        /// </summary>
        literal uint32_t Properties =
          gemfire::GemfireTypeIds::Properties + 0x80000000;

        /// <summary>        
        /// ClassId of <c>CharArray</c> class
        /// </summary>
        literal uint32_t CharArray =
          gemfire::GemfireTypeIds::CharArray + 0x80000000;

        /// <summary>
        /// ClassId of <c>BooleanArray</c> class
        /// </summary>
        literal uint32_t BooleanArray =
          gemfire::GemfireTypeIds::BooleanArray + 0x80000000;

        /// <summary>
        /// ClassId of <c>RegionAttributes</c> class
        /// </summary>
        literal uint32_t RegionAttributes =
          gemfire::GemfireTypeIds::RegionAttributes + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableUndefined</c> class
        /// Implementation note: this has DSFID of FixedIDByte hence a
        /// different increment.
        /// </summary>
        literal uint32_t CacheableUndefined =
          gemfire::GemfireTypeIds::CacheableUndefined + 0xa0000000;

        /// <summary>
        /// ClassId of <c>Struct</c> class
        /// </summary>
        literal uint32_t Struct =
          gemfire::GemfireTypeIds::Struct + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableString</c> class
        /// </summary>
        literal uint32_t CacheableString =
          gemfire::GemfireTypeIds::CacheableString + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableString</c> class for huge strings
        /// </summary>
        literal uint32_t CacheableStringHuge =
          gemfire::GemfireTypeIds::CacheableStringHuge + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableBytes</c> class
        /// </summary>
        literal uint32_t CacheableBytes =
          gemfire::GemfireTypeIds::CacheableBytes + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableInt16Array</c> class
        /// </summary>
        literal uint32_t CacheableInt16Array =
          gemfire::GemfireTypeIds::CacheableInt16Array + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableInt32Array</c> class
        /// </summary>
        literal uint32_t CacheableInt32Array =
          gemfire::GemfireTypeIds::CacheableInt32Array + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableInt64Array</c> class
        /// </summary>
        literal uint32_t CacheableInt64Array =
          gemfire::GemfireTypeIds::CacheableInt64Array + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableFloatArray</c> class
        /// </summary>
        literal uint32_t CacheableFloatArray =
          gemfire::GemfireTypeIds::CacheableFloatArray + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableDoubleArray</c> class
        /// </summary>
        literal uint32_t CacheableDoubleArray =
          gemfire::GemfireTypeIds::CacheableDoubleArray + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableVector</c> class for object arrays
        /// </summary>
        literal uint32_t CacheableObjectArray =
          gemfire::GemfireTypeIds::CacheableObjectArray + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableBoolean</c> class
        /// </summary>
        literal uint32_t CacheableBoolean =
          gemfire::GemfireTypeIds::CacheableBoolean + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableInt16</c> class for wide-characters
        /// </summary>
        literal uint32_t CacheableCharacter =
          gemfire::GemfireTypeIds::CacheableWideChar + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableByte</c> class
        /// </summary>
        literal uint32_t CacheableByte =
          gemfire::GemfireTypeIds::CacheableByte + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableInt16</c> class
        /// </summary>
        literal uint32_t CacheableInt16 =
          gemfire::GemfireTypeIds::CacheableInt16 + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableInt32</c> class
        /// </summary>
        literal uint32_t CacheableInt32 =
          gemfire::GemfireTypeIds::CacheableInt32 + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableInt64</c> class
        /// </summary>
        literal uint32_t CacheableInt64 =
          gemfire::GemfireTypeIds::CacheableInt64 + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableFloat</c> class
        /// </summary>
        literal uint32_t CacheableFloat =
          gemfire::GemfireTypeIds::CacheableFloat + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableDouble</c> class
        /// </summary>
        literal uint32_t CacheableDouble =
          gemfire::GemfireTypeIds::CacheableDouble + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableDate</c> class
        /// </summary>
        literal uint32_t CacheableDate =
          gemfire::GemfireTypeIds::CacheableDate + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableFileName</c> class
        /// </summary>
        literal uint32_t CacheableFileName =
          gemfire::GemfireTypeIds::CacheableFileName + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableStringArray</c> class
        /// </summary>
        literal uint32_t CacheableStringArray =
          gemfire::GemfireTypeIds::CacheableStringArray + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableVector</c> class
        /// </summary>
        literal uint32_t CacheableVector =
          gemfire::GemfireTypeIds::CacheableVector + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableStack</c> class
        /// </summary>
        literal uint32_t CacheableStack =
          gemfire::GemfireTypeIds::CacheableStack + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableArrayList</c> class
        /// </summary>
        literal uint32_t CacheableArrayList =
          gemfire::GemfireTypeIds::CacheableArrayList + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableHashSet</c> class
        /// </summary>
        literal uint32_t CacheableHashSet =
          gemfire::GemfireTypeIds::CacheableHashSet + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableLinkedHashSet</c> class
        /// </summary>
        literal uint32_t CacheableLinkedHashSet =
          gemfire::GemfireTypeIds::CacheableLinkedHashSet + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableHashMap</c> class
        /// </summary>
        literal uint32_t CacheableHashMap =
          gemfire::GemfireTypeIds::CacheableHashMap + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableHashTable</c> class
        /// </summary>
        literal uint32_t CacheableHashTable =
          gemfire::GemfireTypeIds::CacheableHashTable + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableIdentityHashMap</c> class
        /// </summary>
        literal uint32_t CacheableIdentityHashMap =
          gemfire::GemfireTypeIds::CacheableIdentityHashMap + 0x80000000;

        /// <summary>
        /// Not used.
        /// </summary>
        literal uint32_t CacheableTimeUnit =
          gemfire::GemfireTypeIds::CacheableTimeUnit + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableString</c> class for null strings
        /// </summary>
        literal uint32_t CacheableNullString =
          gemfire::GemfireTypeIds::CacheableNullString + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableString</c> class for ASCII strings
        /// </summary>
        literal uint32_t CacheableASCIIString =
          gemfire::GemfireTypeIds::CacheableASCIIString + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableString</c> class for huge ASCII strings
        /// </summary>
        literal uint32_t CacheableASCIIStringHuge =
          gemfire::GemfireTypeIds::CacheableASCIIStringHuge + 0x80000000;


        // Built-in managed types.

        /// <summary>
        /// ClassId of <c>CacheableObject</c> class
        /// </summary>
        literal uint32_t CacheableManagedObject = 7 + 0x80000000;

        /// <summary>
        /// ClassId of <c>CacheableObjectXml</c> class
        /// </summary>
        literal uint32_t CacheableManagedObjectXml = 8 + 0x80000000;
      };

    }
  }
}
