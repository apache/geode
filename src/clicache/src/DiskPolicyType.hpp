/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once



#include "gf_defs.hpp"
#include <gfcpp/DiskPolicyType.hpp>


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Enumerated type for disk policy.
      /// Contains values for setting the disk policy type.
      /// </summary>
      public enum class DiskPolicyType
      {
        /// <summary>No policy.</summary>
        None = 0,

        /// <summary>Overflow to disk.</summary>
        Overflows
      };


      /// <summary>
      /// Static class containing convenience methods for <c>DiskPolicyType</c>.
      /// </summary>
      /// <seealso cref="RegionAttributes.DiskPolicy" />
      /// <seealso cref="AttributesFactory.SetDiskPolicy" />
      public ref class DiskPolicy STATICCLASS
      {
      public:

        /// <summary>
        /// True if the current policy is <c>Overflows</c>.
        /// </summary>
        inline static bool IsOverflow( DiskPolicyType type )
        {
          return (type == DiskPolicyType::Overflows);
        }

        /// <summary>
        /// True if the current policy is <c>None</c>.
        /// </summary>
        inline static bool IsNone( DiskPolicyType type )
        {
          return (type == DiskPolicyType::None);
        }

        ///// <summary>
        ///// True if the current policy is <c>Persist</c>.
        ///// </summary> -- Persist is NOT YET IMPLEMENTED IN C++
        //inline static bool IsPersist( DiskPolicyType type )
        //{
        //  return (type == DiskPolicyType::Persist);
        //}
      };

    }
  }
}
 } //namespace 

