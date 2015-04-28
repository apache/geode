package com.gemstone.gemfire.cache.query.internal;

import java.util.List;

public interface MapIndexable
{
   CompiledValue  getMapLookupKey();
   CompiledValue  getRecieverSansIndexArgs();
   List<CompiledValue> getIndexingKeys();
}
