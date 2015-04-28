package com.gemstone.gemfire.internal.tools.gfsh.app.cache.index;

import java.util.Map;


public interface Indexer
{
	public Map query(Object queryKey);
	
	public int size(Object queryKey);
	
	public IndexInfo getIndexInfo();
}
