package com.gemstone.gemfire.internal.tools.gfsh.app.command.task;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.tools.gfsh.app.cache.data.MapMessage;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandResults;
import com.gemstone.gemfire.internal.tools.gfsh.command.CommandTask;

/**
 * RegionSizeTask returns the region size. CommandResults.getDataObject()
 * returns an integer, the region size. Use
 * @author dpark
 *
 */
public class RegionSizeTask implements CommandTask {

	private static final long serialVersionUID = 1;

	private String regionFullPath;

	public RegionSizeTask() {
	}

	public RegionSizeTask(String regionFullPath) {
		this.regionFullPath = regionFullPath;
	}

	public CommandResults runTask(Object userData) {
		CommandResults results = new CommandResults();
		try {
			Cache cache = CacheFactory.getAnyInstance();
			MapMessage message = new MapMessage();
			message.put("MemberId", cache.getDistributedSystem().getDistributedMember().getId());
			message.put("MemberName", cache.getDistributedSystem().getName());
			message.put("Host", cache.getDistributedSystem().getDistributedMember().getHost());
			message.put("Pid", cache.getDistributedSystem().getDistributedMember().getProcessId());
			Region region = cache.getRegion(regionFullPath);
			if (region == null) {
				results.setCode(CommandResults.CODE_ERROR);
				results.setCodeMessage("Undefined region: " + regionFullPath);
			} else {
				message.put("RegionSize", region.size());
				results.setDataObject(message);
			}
			return results;
		} catch (CacheException ex) {
			results.setCode(CommandResults.CODE_ERROR);
			results.setCodeMessage(ex.getMessage());
			results.setException(ex);
		}
		return results;
	}

	public void fromData(DataInput input) throws IOException,
	ClassNotFoundException {
		regionFullPath = DataSerializer.readString(input);
	}

	public void toData(DataOutput output) throws IOException {
		DataSerializer.writeString(regionFullPath, output);
	}

}
