/*=========================================================================
 ///Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once
using namespace System;
using namespace System::Collections::Generic;
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
			namespace Generic
			{         
			
         /// <summary>
         /// Application can implement this interface to map pdx type name to local type name.
        /// Need to set this using <see cref="Serializable.SetPdxTypeMapper" />
         /// </summary>
        public interface class IPdxTypeMapper
        {
          public:
           /// <summary> 
           /// To map the local type name to pdx type
           /// <param name="localTypeName"> local type name </param>
           /// @return the pdx type name.
           /// </summary>
          String^ ToPdxTypeName(String^ localTypeName);

           /// <summary>
           /// To map the pdx type name to local type
           /// <param name="pdxTypeName"> pdx type name </param>
           /// @return the local type name.
           /// </summary>          
            String^ FromPdxTypeName(String^ pdxTypeName);
        };

			}
    }
  }
}
