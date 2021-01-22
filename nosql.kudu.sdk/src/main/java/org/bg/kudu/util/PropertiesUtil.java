package org.bg.kudu.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * 创建人：xiatiansong <br>
 * 创建时间：2013-8-7 <br>
 * 功能描述： <br>
 * 版本： <br>
 */
public class PropertiesUtil {

	private static final Log LOG = LogFactory.getLog(PropertiesUtil.class);

	private static final Map<String, String> propMap = new HashMap<String, String>();

	private static class ResourceHolder {
		public static PropertiesUtil resource = new PropertiesUtil();
	}

	public static PropertiesUtil getInstance() {
		return PropertiesUtil.ResourceHolder.resource;
	}

	static class Resource {
	}

	private PropertiesUtil() {
	}

	public PropertiesUtil loadProperty(String path) {
		try {
			InputStream in = PropertiesUtil.class.getResourceAsStream(path);
			if (null == in) {
				in = PropertiesUtil.class.getClassLoader().getResourceAsStream(path);
				if (null == in) {
					in = PropertiesUtil.class.getResourceAsStream(path);
					if (null == in) {
						in = PropertiesUtil.class.getClassLoader().getResourceAsStream(path);
					}
				}
			}
			if (in != null) {
				Properties p = new Properties();
				p.load(in);
				propMap.putAll(convertToMap(p));
			}
		} catch (IOException e) {
			LOG.error("加载Property文件出错" + e);
		}
		return this;
	}

	public String getProperty(String key) {
		return propMap.get(key);
	}

	public String getProperty(String key, String defaultValue) {
		String value = propMap.get(key);
		if (value == null) {
			value = defaultValue;
		}
		return value;
	}

	/**
	 * 转换Properties为Map对象
	 * 
	 * @param prop
	 * @return Map<String,String>
	 * @date:2013-8-7
	 */
	public Map<String, String> convertToMap(Properties prop) {
		if (prop == null) {
			return null;
		}
		Map<String, String> result = new HashMap<String, String>();
		for (Object eachKey : prop.keySet()) {
			if (eachKey == null) {
				continue;
			}
			String key = eachKey.toString();
			String value = (String) prop.get(key);
			if (value == null) {
				value = "";
			} else {
				value = value.trim();
			}
			result.put(key, value);
			System.setProperty(key, value);
		}
		return result;
	}
}