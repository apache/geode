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
#include <gfcpp/DiskPolicyType.hpp>


using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
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
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache


