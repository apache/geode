package com.gemstone.gemfire.internal.tools.gfsh.app.function.command;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexStatistics;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.ServerExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.MapMessage;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.Mappable;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;

public class index implements ServerExecutable
{
	public static enum DeleteType {
		DELETE_INDEX,
		DELETE_REGION_INDEXES,
		DELETE_ALL_INDEXES
	}
	
	private byte code = AggregateResults.CODE_NORMAL;
	private String codeMessage = null;
	
	public Object execute(String command, String regionPath, Object arg) throws Exception
	{
		GfshData data = new GfshData(null);
		
		Cache cache = CacheFactory.getAnyInstance();
		
		Region region = null;
		if (regionPath != null) {
			region = cache.getRegion(regionPath);
		}
		
		String thisMemberId = cache.getDistributedSystem().getDistributedMember().getId();
		QueryService qs = cache.getQueryService();
		
		Object[] args = (Object[])arg;
		String memberId = null;
		String indexName = null;
		IndexType indexType;
		boolean isFunctional;
		String indexExpression;
		String fromClause;
		String imports;
		
		String operationType = (String)args[0];
		if (operationType.equals("-create")) {
			
			memberId = (String)args[1];
			if (memberId != null && memberId.equals(thisMemberId) == false) {
				return data;
			}
			
			indexName = (String)args[2];
			isFunctional = (Boolean)args[3];
			if (isFunctional) {
				indexType = IndexType.FUNCTIONAL;
			} else {
				indexType = IndexType.PRIMARY_KEY;
			}
			indexExpression = (String)args[4];
			fromClause = (String)args[5];
			imports = (String)args[6];
			
			try {
				Index index = qs.createIndex(indexName, indexType, indexExpression, fromClause, imports);
				codeMessage = "index created: " + indexName;
			} catch (Exception ex) {
				while (ex.getCause() != null) {
					ex = (Exception)ex.getCause();
				}
				codeMessage = ex.getMessage();
				if (codeMessage != null) 
					codeMessage = codeMessage.trim();
				if (codeMessage == null || codeMessage.length() == 0) {
					codeMessage = ex.getClass().getSimpleName();
				}
			}
			
			data.setDataObject(codeMessage);
			
		} else if (operationType.equals("-delete")) {
			
			DeleteType deleteType = (DeleteType)args[1];
			
			memberId = (String)args[2];
			if (memberId != null && memberId.equals(thisMemberId) == false) {
				return data;
			}
			indexName = (String)args[3];
			
			switch (deleteType) {
			case DELETE_ALL_INDEXES:
				qs.removeIndexes();
				codeMessage = "all indexes deleted from the member";
				break;
				
			case DELETE_REGION_INDEXES:
				try {
					qs.removeIndexes(region);
					codeMessage = "all indexes deleted from " + region.getFullPath();
				} catch (Exception ex) {
					codeMessage = ex.getMessage();
				}
				break;
				
			case DELETE_INDEX:
				Index index = qs.getIndex(region, indexName);
				if (index == null) {
					codeMessage = "index does not exist";
				} else {
					try {
						qs.removeIndex(index);
						codeMessage = "index deleted from " + region.getFullPath();
					} catch (Exception ex) {
						codeMessage = ex.getMessage();
						if (codeMessage != null) 
							codeMessage = codeMessage.trim();
						if (codeMessage == null || codeMessage.length() == 0) {
							codeMessage = ex.getClass().getSimpleName();
						}
					}
				}
				break;
			}
			data.setDataObject(codeMessage);

		} else if (operationType.equals("-list")) {
			
			memberId = (String)args[1];
			if (memberId != null && memberId.equals(thisMemberId) == false) {
				return data;
			}
			
			boolean isAll = (Boolean)args[2];
			boolean isStats = (Boolean)args[3];
			
			Collection<Index> col = null;
			if (isAll) {
				col = qs.getIndexes();
			} else if (region != null) {
				col = qs.getIndexes(region);
			} else {
				codeMessage = "Invalid index command. Region path not specified.";
				data.setDataObject(codeMessage);
				return data;
			}
			
			List<Mappable> mappableList = new ArrayList();
			for (Index index : col) {
				indexName = index.getName();
				String type = index.getType().toString();
				indexExpression = index.getIndexedExpression();
				fromClause = index.getFromClause();
				
				MapMessage mapMessage = new MapMessage();
				mapMessage.put("Name", indexName);
				mapMessage.put("Type", type);
				mapMessage.put("Expression", indexExpression);
				mapMessage.put("From", fromClause);
				if (isStats) {
					try {
						IndexStatistics stats = index.getStatistics();
						mapMessage.put("Keys", stats.getNumberOfKeys());
						mapMessage.put("Values", stats.getNumberOfValues());
						mapMessage.put("Updates", stats.getNumUpdates());
						mapMessage.put("TotalUpdateTime", stats.getTotalUpdateTime());
						mapMessage.put("TotalUses", stats.getTotalUses());
					} catch (Exception ex) {
						// index not supported for pr
					}
				}
				
				mappableList.add(mapMessage);
			}
			data.setDataObject(mappableList);
		}
		
		return data;
	}

	public byte getCode()
	{
		return code;
	}
	
	public String getCodeMessage()
	{
		return codeMessage;
	}
}
