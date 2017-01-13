/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
using namespace System;
using namespace System::Reflection;


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
			namespace Generic
			{    
        ///<summary>        
        /// PdxIdentityField attribute one can specify on member fields.
        /// This attribute is used by <see cref="ReflectionBasedAutoSerializer">,
        /// When it serializes the fields in Pdx <see cref="IPdxSerializable"> format.
        /// This fields will be treated as identity fields for hashcode and equals methods.
        ///<summary>        

      [AttributeUsage(AttributeTargets::Field)]
      public ref class PdxIdentityFieldAttribute : Attribute
      {
      public:

        PdxIdentityFieldAttribute()
        {
        }
      };
      }
    }
  }
}