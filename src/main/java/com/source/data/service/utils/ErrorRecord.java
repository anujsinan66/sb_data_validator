package com.source.data.service.utils;

public class ErrorRecord {
	private String errorJson = "";
	private StringBuffer errorMsg = new StringBuffer("");
	public String getErrorJson() {
		return errorJson;
	}

	public void setErrorJson(String errorJson) {
		this.errorJson = errorJson;
	}
	
	public void setErrorMsg(String msg) {
		if( null == msg ) {
			this.errorMsg.append(msg);
		}else {
			this.errorMsg.append(msg.replaceAll("\"", "")) ;
		}
		
	}

	public String toJson(){
	     return  errorMsg +"||"+ errorJson ;
	}
	public String toErrorJson(){
	     return  errorMsg +"||"+ errorJson;
	}
}
