package com.gemstone.gemfire.internal.tools.gfsh.app.misc.util;

public class StringUtil
{
    /**
     * Returns the short name of the fully-qualified class name.
     * @param className The fully-qualified class name.
     */
    public static String getShortClassName(String className)
    {
        if (className == null) {
            return null;
        }
        String shortName = null;
        int index = className.lastIndexOf('.');
        if (index == -1 || index >= className.length()-1) {
            shortName = className;
        } else {
            shortName = className.substring(index+1);
        }
        return shortName;
    }

    /**
     * Returns the short class name of the specified object.
     * @param obj   The object from which its short name is to be derived.
     */
    public static String getShortClassName(Object obj)
    {
        if (obj == null) {
            return null;
        }
        return getShortClassName(obj.getClass().getName());
    }

    /**
     * Trims the matching character found in the left end of the string.
     * @param str   The string to trim.
     * @param c     The character remove.
     */
    public static String trimLeft(String str, char c)
    {
        int len = str.length();
        int index = 0;
        while (index < len && str.charAt(index++) == c);
        index--;
        if (index < 0) {
            return "";
        } else if (index < len) {
            return str.substring(index);
        } else {
            return str;
        }
    }

    /**
     * Trims the matching character found in right end of the string.
     * @param str   The string to trim.
     * @param c     The character remove.
     */
    public static String trimRight(String str, char c)
    {
        int len = str.length();
        int index = len - 1;
        while (index >= 0 && str.charAt(index--) == c);
        index++;
        if (index > len - 1) {
            return str;
        } else if (index >= 0) {
            return str.substring(0, index + 1);
        } else  {
            return "";
        }
    }

    /**
     * Trims all of the matching character in the string.
     * @param str   The string to trim.
     * @param c     The character remove.
     */
    public static String trim(String str, char c)
    {
        return trimRight(trimLeft(str, c), c);
    }

    /**
     * Replaces the all of the matching oldValue in the string with the newValue.
     * @param str   The string to replace matching substring.
     * @param oldValue  The old value to match and replace.
     * @param newValue  The new value to replace the old value with.
     */
    public static String replace(String str, String oldValue, String newValue)
    {
        if (str == null || oldValue == null || newValue == null) {
            return null;
        }

        int index = str.indexOf(oldValue);
        if (index != -1) {
            int oldValueLen = oldValue.length();
            int newValueLen = newValue.length();
            String head;
            String tail = str;
            StringBuffer buffer = new StringBuffer(str.length() + newValueLen);
            do {
                head = tail.substring(0, index);
                buffer.append(head);
                buffer.append(newValue);
                tail = tail.substring(index+oldValueLen);
                index = tail.indexOf(oldValue);
            } while (index != -1);
            buffer.append(tail);

            str = buffer.toString();
        }

        return str;
    }
    
    public static String getLeftPaddedString(String value, int maxSize, char pad)
	{
		int diff = maxSize - value.length();
		StringBuffer buffer = new StringBuffer(maxSize);
		for (int i = 0; i < diff; i++) {
			buffer.append(pad);
		}
		buffer.append(value);
		return buffer.toString();
	}
	
    public static String getRightPaddedString(String value, int maxSize, char pad)
	{
		int diff = maxSize - value.length();
		StringBuffer buffer = new StringBuffer(maxSize);
		buffer.append(value);
		for (int i = 0; i < diff; i++) {
			buffer.append(pad);
		}
		return buffer.toString();
	}
	
}
