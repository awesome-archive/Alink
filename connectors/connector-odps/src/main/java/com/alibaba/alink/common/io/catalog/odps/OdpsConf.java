package com.alibaba.alink.common.io.catalog.odps;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class OdpsConf implements Serializable {
	String accessId;
	String accessKey;
	String securityToken;
	String endpoint;
	String tunnelEndpoint;
	String project;
	Map<String, String> properties;

	public OdpsConf(String accessId, String accessKey, String endpoint) {
		this(accessId, accessKey, endpoint, "", "");
	}

	public OdpsConf(String accessId, String accessKey, String endpoint, String project) {
		this(accessId, accessKey, endpoint, project, "");
	}

	public OdpsConf(String accessId, String accessKey, String endpoint, String project, String tunnelEndpoint) {
		this(accessId, accessKey, endpoint, project, "", "");
	}

	public OdpsConf(String accessId, String accessKey, String endpoint, String project, String tunnelEndpoint, String securityToken) {
		this.accessId = accessId;
		this.accessKey = accessKey;
		this.endpoint = endpoint;
		this.project = project;
		this.tunnelEndpoint = tunnelEndpoint;
		this.securityToken = securityToken;
		this.properties = new HashMap();
	}

	public void setAccessId(String accessId) {
		this.accessId = accessId;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}

	public void setProject(String project) {
		this.project = project;
	}

	public void setTunnelEndpoint(String tunnelEndpoint) {
		this.tunnelEndpoint = tunnelEndpoint;
	}

	public String getTunnelEndpoint() {
		return this.tunnelEndpoint;
	}

	public String getAccessId() {
		return this.accessId;
	}

	public String getAccessKey() {
		return this.accessKey;
	}

	public String getEndpoint() {
		return this.endpoint;
	}

	public String getProject() {
		return this.project;
	}

	public Map<String, String> getProperties() {
		return this.properties;
	}

	public void setProperty(String key, String value) {
		this.properties.put(key, value);
	}

	public String getProperty(String key) {
		return (String)this.properties.get(key);
	}

	public String getProperty(String key, String defaultValue) {
		return this.properties.containsKey(key) ? this.getProperty(key) : defaultValue;
	}

	public String getSecurityToken() {
		return this.securityToken;
	}

	public void setSecurityToken(String securityToken) {
		this.securityToken = securityToken;
	}
}
