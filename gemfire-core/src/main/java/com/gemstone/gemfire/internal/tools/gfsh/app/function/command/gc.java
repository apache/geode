package com.gemstone.gemfire.internal.tools.gfsh.app.function.command;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.ServerExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.function.GfshData;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class gc implements ServerExecutable
{
	private byte code = AggregateResults.CODE_NORMAL;
	private String codeMessage = null;
	
	
	@SuppressFBWarnings(value="DM_GC",justification="This is the desired functionality")
	public Object execute(String command, String regionPath, Object arg) throws Exception
	{
		String memberId = (String)arg;
		
		if (memberId != null) {
			Cache cache = CacheFactory.getAnyInstance();
			String thisMemberId = cache.getDistributedSystem().getDistributedMember().getId();
			if (memberId.equals(thisMemberId) == false) {
				return new GfshData(null);
			}
		}
		
		try {
			Runtime.getRuntime().gc(); //FindBugs - extremely dubious except in benchmarking code
		} catch (Exception ex) {
			code = AggregateResults.CODE_ERROR;
			codeMessage = ex.getMessage();
		}
		
		return new GfshData(null);
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
