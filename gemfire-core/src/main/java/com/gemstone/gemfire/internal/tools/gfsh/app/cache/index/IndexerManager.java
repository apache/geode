package com.gemstone.gemfire.internal.tools.gfsh.app.cache.index;

import java.util.HashMap;


public class IndexerManager
{
	private static final IndexerManager indexerManager = new IndexerManager();
	
	private HashMap<String, Indexer> indexerMap = new HashMap();
	
	public static IndexerManager getIndexerManager()
	{
		return indexerManager;
	}

	private IndexerManager()
	{
//		indexerManager = this;
	}
	
	void putIndxer(String regionPath, Indexer indexer)
	{
		indexerMap.put(regionPath, indexer);
	}
	
	public Indexer getIndexer(String regionPath)
	{
		return indexerMap.get(regionPath);
	}

}