/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
			namespace Generic
			{     
				/// <summary>
				/// Serialize the data in gemfire Portable Data eXchange(Pdx) Format.
				/// This format provides class versioning(forward and backward compability of types) in cache.
				/// This provides ability to query .NET domian objects.
				/// </summary>
				public interface class IPdxUnreadFields
				{

				};
			}
    }
  }
}
