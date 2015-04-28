package com.gemstone.gemfire.internal.tools.gfsh.app.commands;

import java.util.LinkedList;
import java.util.List;

import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.internal.tools.gfsh.app.CommandExecutable;
import com.gemstone.gemfire.internal.tools.gfsh.app.Gfsh;
import com.gemstone.gemfire.internal.tools.gfsh.app.Nextable;
import com.gemstone.gemfire.internal.tools.gfsh.app.util.PrintUtil;

public class local implements CommandExecutable, Nextable
{
	private Gfsh gfsh;
	private SelectResults selectResults;
	int lastRowPrinted = 0;
	
	public local(Gfsh gfsh)
	{
		this.gfsh = gfsh;
	}
	
	public void help()
	{
		gfsh.println("local [-?] <query>");
		gfsh.println("     Execute the specified query on the local cache.");
		gfsh.println();
	}
	
	public void execute(String command) throws Exception
	{
		if (command.startsWith("local -?")) {
			help();
		} else {
			local(command);
		}
	}
	
	public List next(Object userData) throws Exception
	{
		if (selectResults == null) {
			return null;
		}
	
		if (gfsh.isShowResults()) {
			int rowsPrinted = PrintUtil.printSelectResults(selectResults, lastRowPrinted, lastRowPrinted+1, gfsh.getFetchSize());
			lastRowPrinted = lastRowPrinted + rowsPrinted;
			gfsh.println("Fetch size: " + gfsh.getFetchSize());
			gfsh.println("   Results: " + selectResults.size()
					+ ", Returned: " + lastRowPrinted + "/" + selectResults.size());
		} else {
			gfsh.println("Fetch size: " + gfsh.getFetchSize());
			gfsh.println("    Results: " + selectResults.size());
		}
		return null;
	}
	
	private void local(String command) throws Exception
	{
		LinkedList list = new LinkedList();
		gfsh.parseCommand(command, list);
		if (list.size() < 2) {
			gfsh.println("Error: local requires a query statment");
		} else {
			String queryString = "";
			for (int i = 1; i < list.size(); i++) {
				queryString += list.get(i) + " ";
			}
			QueryService queryService = gfsh.getCache().getQueryService();
			Query query = queryService.newQuery(queryString);
			
			long startTime = System.currentTimeMillis();
			Object obj = query.execute();
			long stopTime = System.currentTimeMillis();
			
			selectResults = null;
			if (obj instanceof SelectResults) {
				selectResults = (SelectResults)obj;
				if (gfsh.isShowResults()) {
					int rowsPrinted = PrintUtil.printSelectResults(selectResults, 0, 1, gfsh.getFetchSize());
					lastRowPrinted = rowsPrinted;
					gfsh.println("Fetch size: " + gfsh.getFetchSize());
					gfsh.println("   Results: " + selectResults.size()
							+ ", Returned: " + lastRowPrinted + "/" + selectResults.size());
					next n = (next)gfsh.getCommand("next");
					n.setCommand(getClass().getSimpleName());
				} else {
					gfsh.println("Fetch size: " + gfsh.getFetchSize());
					gfsh.println("   Results: " + selectResults.size());
				}
			} else {
				gfsh.println("Results: " + obj);
			}
			if (gfsh.isShowTime()) {
				gfsh.println("elapsed (msec): " + (stopTime - startTime));
			}
		}
	}
}
