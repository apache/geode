#ifndef GEMFIRE_FIXED_PARTITION_RESOLVER
#define GEMFIRE_FIXED_PARTITION_RESOLVER
/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

#include "PartitionResolver.hpp"
#include "CacheableBuiltins.hpp"

namespace gemfire {

class EntryEvent;

/**
 * Implementers of interface <code>FixedPartitionResolver</code> helps to
 * achieve explicit mapping of a "user defined" partition to a data member node.
 * <p>
 * GemFire uses the partition name returned by
 * {@link FixedPartitionResolver#getPartitionName(EntryEvent,
 * CacheableHashSetPtr)}
 * to determine on which member the data is being managed. Say, for example, you
 * want to
 * partition all Trades according to quarters. You can implement
 * FixedPartitionResolver to get the name of the quarter based on the date given
 * as part of {@link EntryEvent}.
 * </p>
 *
 * public class QuarterPartitionResolver implements FixedPartitionResolver{<br>
 * &nbsp &nbsp public const char* getPartitionName(EntryEvent event,
 * CacheableHashSetPtr
 * allAvailablePartitions) {<br>
 * &nbsp &nbsp Date date = sdf.parse((String)opDetails.getKey());<br>
 * &nbsp &nbsp Calendar cal = Calendar.getInstance();<br>
 * &nbsp &nbsp cal.setTime(date);<br>
 * &nbsp &nbsp int month = cal.get(Calendar.MONTH);<br>
 * &nbsp &nbsp if (month == 0 || month == 1 || month == 2) {<br>
 * &nbsp &nbsp &nbsp return "Quarter1";<br>
 * &nbsp &nbsp }<br>
 * &nbsp &nbsp else if (month == 3 || month == 4 || month == 5) {<br>
 * &nbsp &nbsp &nbsp return "Quarter2";<br>
 * &nbsp &nbsp }<br>
 * &nbsp &nbsp else if (month == 6 || month == 7 || month == 8) {<br>
 * &nbsp &nbsp &nbsp return "Quarter3";<br>
 * &nbsp &nbsp }<br>
 * &nbsp &nbsp else if (month == 9 || month == 10 || month == 11) {<br>
 * &nbsp &nbsp &nbsp return "Quarter4";<br>
 * &nbsp &nbsp }<br>
 * &nbsp &nbsp else {<br>
 * &nbsp &nbsp &nbsp return "Invalid Quarter";<br>
 * &nbsp &nbsp }<br>
 * &nbsp }<br>
 *
 * @see PartitionResolver
 *
 */
class CPPCACHE_EXPORT FixedPartitionResolver : public PartitionResolver {
  /**
  * @brief public methods
  */

 public:
  /**
  * This method is used to get the name of the partition for the given entry
  * operation.
  *
  * @param opDetails
  *          the details of the entry event e.g. {@link Region#get(Object)}
  *
  * @return partition-name associated with node which allows mapping of given
  *         data to user defined partition
  */
  virtual const char* getPartitionName(const EntryEvent& opDetails) = 0;
};
}
#endif
