#ifndef __GEMFIRE_SCOPETYPE_H__
#define __GEMFIRE_SCOPETYPE_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/**
 * @file
 */
#include "gfcpp_globals.hpp"
#include <string.h>

namespace gemfire {
/**
 * @class ScopeType ScopeType.hpp
 * Enumerated type for region distribution scope.
 *
 * For Native Clients:
 * LOCAL scope is invalid (it is a non-native client local region), and
 * DISTRIBUTED_ACK and DISTRIBUTED_NO_ACK have the same behavior.
 *
 * @see RegionAttributes::getScope
 * @see AttributesFactory::setScope
 */
class CPPCACHE_EXPORT ScopeType {
     // public static methods
 public:   
     /**
      * Values for setting Scope.
      */
    typedef enum {
    LOCAL /** no distribution. */ = 0,
    DISTRIBUTED_NO_ACK /** Distribute without waiting for acknowledgement. */,
    DISTRIBUTED_ACK /** Distribute and wait for all peers to acknowledge. */,
    GLOBAL /** Distribute with full interprocess synchronization -- not yet implemented. */,
    INVALID} Scope;

    
    /** Returns the Name of the Scope represented by specified ordinal. */
    static const char* fromOrdinal(const uint8_t ordinal) ;

    /** Returns the type of the Scope represented by name. */
    static  Scope fromName(const char* name) ;
    
    /** Returns whether this is local scope.
     * @return true if this is LOCAL
     */    
    inline static bool isLocal(const Scope type) 
    {
       return (type == ScopeType::LOCAL);
    }
    
    /** Returns whether this is one of the distributed scopes.
     * @return true if this is any scope other than LOCAL
     */    
    inline static bool isDistributed(const Scope type) 
    {
       return (type != ScopeType::LOCAL);
    }
    
    /** Returns whether this is distributed no ack scope.
     * @return true if this is DISTRIBUTED_NO_ACK
     */    
    inline static bool isDistributedNoAck(const Scope type) 
    {
       return (type == ScopeType::DISTRIBUTED_NO_ACK);
    }
    
    /** Returns whether this is distributed ack scope.
     * @return true if this is DISTRIBUTED_ACK
     */    
    inline static bool isDistributedAck(const Scope type) 
    {
       return (type == ScopeType::DISTRIBUTED_ACK);
    }
    
    /** Returns whether this is global scope.
     * @return true if this is GLOBAL
     */    
    inline static bool isGlobal(const Scope type) 
    {
       return (type == ScopeType::GLOBAL);
    }
    
    /** Returns whether acknowledgements are required for this scope.
     * @return true if this is DISTRIBUTED_ACK or GLOBAL, false otherwise
     */    
    inline static bool isAck(const Scope type)
    {
       return (type == ScopeType::GLOBAL || type == ScopeType::DISTRIBUTED_ACK);
    }
    
    private:
    /** No instance allowed. */
    ScopeType() {};
    static char* names[];
};
}; //namespace gemfire
#endif //ifndef __GEMFIRE_SCOPETYPE_H__
