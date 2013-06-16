package org.db.export.common.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.db.export.common.Constants;
import org.db.export.common.log.Logger;
import org.db.export.common.log.LoggerFactory;

public final class CommonUtils {
	private static final Logger log = LoggerFactory.getLogger(CommonUtils.class);
	
	private static final char CONST_COLON = ':';
	private static final char CONST_COMMA = ',';
	private static final char CONST_DOUBLE_QUOTE = '\"';
	private static final char CONST_SINGLE_QUOTE = '\'';
	private static final char CONST_OBJECT_END = '}';
	private static final char CONST_OBJECT_START = '{';
	private static final char CONST_ARR_END = ']';
	private static final char CONST_ARR_START = '[';
	
	private CommonUtils() {
		;
	}

	public static <T> List<List<T>> slice(List<T> pms, final int size) {
		List<List<T>> rtn = new ArrayList<List<T>>(1);
		if(pms == null || pms.size() <= 0)
			return rtn;
		if(size <= 0) {
			rtn.add(pms);

			return rtn;
		}

		if(pms.size() <= size) {
			rtn.add(pms);
			return rtn;
		}

		rtn = new ArrayList<List<T>>(pms.size() / size + 1);
		for(int i = 0; i < pms.size();) {
			List<T> cell = new ArrayList<T>(size);
			for(int j = 0; j < size && i < pms.size(); j++, i++) {
				cell.add(pms.get(i));
			}

			rtn.add(cell);
		}

		return rtn;
	}
	
	/**
	 * Shell confirm functionality.<br/>
	 * 
	 * @param message prompt message let user to input
	 * @param yesMessage if yes will output this message
	 * @param noMessage if no will output this message
	 * @return
	 */
	public static boolean confirm(String message, String yesMessage, String noMessage) {
		System.out.print(message);
		
		boolean yes = false;
		try {
			byte[] values = new byte[10];
			int num = System.in.read(values);
			
			String value = null;
			if(num <= 0) {
				value = "";
			} else {
				value = new String(values, 0 , num);
			}
			if(Constants.CONST_YES.equals(value.trim())) {
				yes = true;
			}
		} catch (IOException e) {
			yes = false;
		}
		
		if(yes) {
			log.info(yesMessage);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				; // ignore;
			}
		} else {
			log.info(noMessage);
		}
		
		return yes;
	}
	
	public static <T> boolean isEmpty(Collection<T> coll) {
		return coll == null || coll.isEmpty();
	}
	
	public static <T> boolean isNotEmpty(Collection<T> coll) {
		return coll != null && !coll.isEmpty();
	}
	
	public static <T, U> boolean isEmpty(Map<T, U> map) {
		return map == null || map.isEmpty();
	}
	
	public static <T, U> boolean isNotEmpty(Map<T, U> map) {
		return map != null && !map.isEmpty();
	}
	
	public static boolean isEmpty(String str) {
		return str == null || str.isEmpty();
	}
	
	public static boolean isNotEmpty(String str) {
		return str != null && !str.isEmpty();
	}
	
	public static boolean isBlank(String str) {
		return isEmpty(str) || isEmpty(str.trim());
	}
	
	public static <T> boolean isEmpty(T[] arr) {
		return arr == null || arr.length <= 0;
	}
	
	public static Map<String,Object> toJSON(String json) {
		if(isEmpty(json)
				|| isEmpty(json = json.trim())) {
			throw new IllegalArgumentException("JSON string is empty.");
		}
		
		json = json.trim();
		
		Map<String, Object> value = new HashMap<String, Object>();
		if(!isJsonObject(json)) {
			throw new IllegalArgumentException("JSON string is invalid. json = " + json);
		}
		toJsonObject(json, value);
		
		return value;
	}
	
	private static void toJsonObject(String json, Map<String, Object> map) {
		String[] properties = parserElements(json);
		for(String property : properties) {
			String[] pair = propSplit(property);
			if(isEmpty(pair) || pair.length <= 1
					|| isBlank(pair[0]) || isBlank(pair[1])) {
				throw new IllegalArgumentException("JSON string is invalid. property = " + property);
			}
			
			final String propKey = pair[0].trim();
			final String propValue = pair[1].trim();
			if(isJsonObject(propValue)) {
				Map<String, Object> value = new HashMap<String, Object>();
				toJsonObject(propValue, value);
				map.put(propKey, value);
			} else if(isJsonArray(propValue)) {
				List<Object> array = new ArrayList<Object>();
				toJsonArray(propValue, array);
				map.put(propKey, array);
			} else {
				map.put(propKey, parseJsonObject(propValue));
			}
		}
	}
	
	//JSON has nested format, can't use string.split() method;
	private static String[] propSplit(String property) {
		int index = property.indexOf(CONST_COLON);
		if(index <= 0) {
			throw new IllegalArgumentException("Json property format is invalid: property = " + property);
		}
		
		String[] pair = new String[2];
		pair[0] = property.substring(0, index);
		pair[1] = property.substring(index + 1);
		
		return pair;
	}
	
	private static void toJsonArray(String json, List<Object> array) {
		String[] eles = parserElements(json);
		
		for(String ele : eles) {
			array.add(parseJsonObject(ele));
		}
	}
	
	private static Object parseJsonObject(String value) {
		if(isJsonX(value, CONST_SINGLE_QUOTE, CONST_SINGLE_QUOTE)
			|| isJsonX(value, CONST_DOUBLE_QUOTE, CONST_DOUBLE_QUOTE)) {
			return value.substring(1, value.length() - 1);
		} else {
			try {
				Long v =  Long.parseLong(value);
				if(v.toString().equals(value)) { // else is a float-point value;
					return v;
				}
			} catch (NumberFormatException e) {
				; // ignore, maybe a float-point value;
			}
			
			try {
				return Double.parseDouble(value);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("json value must be a number, value = " + value);
			}
		}
	}
	
	private static String[] parserElements(String json) {
		json = json.substring(1, json.length() - 1);
		
		List<String> tokens = new ArrayList<String>();
		
		StringBuilder separators = new StringBuilder();
		int i = 0;
		int len = json.length();
		while(i < len) {
			//ignore special chars in string: {、}、[、];
			if(separators.length() != 0
					&& ((separators.charAt(0) == CONST_SINGLE_QUOTE && json.charAt(i) != CONST_SINGLE_QUOTE)
							|| (separators.charAt(0) == CONST_DOUBLE_QUOTE && json.charAt(i) != CONST_DOUBLE_QUOTE)
							)) {
				++i;
				continue;
			}
			
			//dedicate '、"、{、[ object is ended;
			if(separators.length() != 0 &&
					((separators.charAt(0) == CONST_OBJECT_START && json.charAt(i) == CONST_OBJECT_END)
					|| (separators.charAt(0) == CONST_ARR_START && json.charAt(i) == CONST_ARR_END)
					|| (separators.charAt(0) == CONST_SINGLE_QUOTE && json.charAt(i) == CONST_SINGLE_QUOTE)
					|| (separators.charAt(0) == CONST_DOUBLE_QUOTE && json.charAt(i) == CONST_DOUBLE_QUOTE)
					)) { //go ahead after object/array/string ended;
				++i;
				separators.deleteCharAt(0);
				
				continue;
			}
			
			//special chars started: '、"、{、[;
			if(json.charAt(i) == CONST_SINGLE_QUOTE || json.charAt(i) == CONST_DOUBLE_QUOTE
					|| json.charAt(i) == CONST_OBJECT_START || json.charAt(i) == CONST_ARR_START) {
				separators.insert(0, json.charAt(i));
				++i;
				continue;
			}
			
			if(json.charAt(i) != CONST_COMMA
					// {}/[] ignore ','
					|| (separators.length() != 0 
							&& (separators.charAt(0) == CONST_OBJECT_START 
								|| separators.charAt(0) == CONST_ARR_START))) {
				++i;
			} else {
				tokens.add(json.substring(0, i));
				++i; //ignore ','
				
				// reallocate the pointer;
				json = json.substring(i).trim();
				i = 0;
				len = json.length();
			}
		}
		if(!isEmpty(json)) {
			tokens.add(json);
		}
		
		return tokens.toArray(new String[tokens.size()]);
		
	}
	
	private static boolean isJsonArray(String json) {
		return isJsonX(json, CONST_ARR_START, CONST_ARR_END);
	}
	
	private static boolean isJsonObject(String json) {
		return isJsonX(json, CONST_OBJECT_START, CONST_OBJECT_END);
	}
	
	private static boolean isJsonX(String json, char start, char end) {
		return json.charAt(0) == start && json.charAt(json.length() - 1) == end;
	}
}
