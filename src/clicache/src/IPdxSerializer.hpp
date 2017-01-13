/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "IPdxWriter.hpp"
#include "IPdxReader.hpp"

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
			namespace Generic
			{      
				/// <summary>
				/// The IPdxSerializer interface allows domain classes to be 
        /// serialized and deserialized as PDXs without modification 
        /// of the domain class.
				/// A domain class should register delgate <see cref="Serializable.RegisterPdxType" /> to create new 
				/// instance of type for de-serilization.
				/// </summary>
				public interface class IPdxSerializer
				{
				public:

					/// <summary>
					/// Serializes this object in gemfire PDX format.
					/// </summary>
          /// <param name="o">
					/// the object which need to serialize
					/// </param>
					/// <param name="writer">
					/// the IPdxWriter object to use for serializing the object
					/// </param>
					bool ToData( Object^ o,IPdxWriter^ writer );

					/// <summary>
					/// Deserialize this object.
					/// </summary>
          /// <param name="classname">
					/// the classname whose object need to de-serialize
					/// </param>
					/// <param name="reader">
					/// the IPdxReader stream to use for reading the object data
					/// </param>
					Object^ FromData(String^ classname, IPdxReader^ reader );

				};
			}
    }
  }
}
