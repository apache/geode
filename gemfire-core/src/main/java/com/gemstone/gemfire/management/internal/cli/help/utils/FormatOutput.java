package com.gemstone.gemfire.management.internal.cli.help.utils;

import java.util.*;

public class FormatOutput {

	public static String converListToString(List<String> outputStringList) {
		Iterator<String> iters = outputStringList.iterator();
		
		StringBuilder sb = new StringBuilder(200);
		
		while (iters.hasNext()) {
		  sb.append("\n");
			sb.append((String)iters.next());
		}
		
		return sb.toString();
	}
}
