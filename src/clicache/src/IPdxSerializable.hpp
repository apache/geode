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
				/// When a domain class implements PdxSerializable it marks 
				/// itself as a PDX. 
				/// The implementation of toData provides the serialization 
				/// code and fromData provides the deserialization code. 
				/// These methods also define each field name and field type 
				/// of the PDX. Domain classes should serialize and de-serialize 
				/// all its member fields in same order in toData and fromData 
				/// method. 
				/// A domain class which implements this interface should register delgate <see cref="Serializable.RegisterPdxType" /> to create new 
				/// instance of type for de-serilization.
				/// </summary>
				public interface class IPdxSerializable
				{
				public:

					/// <summary>
					/// Serializes this object in gemfire PDX format.
					/// </summary>
					/// <param name="writer">
					/// the IPdxWriter object to use for serializing the object
					/// </param>
					void ToData( IPdxWriter^ writer );

					/// <summary>
					/// Deserialize this object.
					/// </summary>
					/// <param name="reader">
					/// the IPdxReader stream to use for reading the object data
					/// </param>
					void FromData( IPdxReader^ reader );

				};
			}
    }
  }
}
