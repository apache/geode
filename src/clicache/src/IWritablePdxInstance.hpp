/*=========================================================================
 ///Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

using namespace System;
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
			namespace Generic
			{     
				/// <summary>
	      /// WritablePdxInstance is a <see cref="IPdxInstance" /> that also supports field modification 
        /// using the <see cref="SetField" />method. 
        /// To get a WritablePdxInstance call <see cref="IPdxInstance.CreateWriter" />.
 				/// </summary>
				public interface class IWritablePdxInstance
				{
        public:
          /// <summary>
          ///Set the existing named field to the given value.
          ///The setField method has copy-on-write semantics.
          /// So for the modifications to be stored in the cache the WritablePdxInstance 
          ///must be put into a region after setField has been called one or more times.
          /// </summary>
          ///
          ///<param name="fieldName"> name of the field whose value will be set </param>
          ///<param name="value"> value that will be assigned to the field </param>
          ///<exception cref="IllegalStateException"/> if the named field does not exist
          ///or if the type of the value is not compatible with the field </exception>
          
          void SetField(String^ fieldName, Object^ value);
				};
			}
    }
  }
}
