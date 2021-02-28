package com.source.data.service.utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Parser;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DataValidation {
	static SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss", Locale.ENGLISH);
	static List<String> unmapField = new ArrayList<String>();
	public static DataObject validateJson(String json, String schemaString ) throws Exception {
		
		DataObject obj = null;
		ErrorRecord errorRecord = new ErrorRecord();
		try {
			boolean isValidJson = true;
			Schema schema =  new Parser().parse(schemaString);
			ObjectMapper mapper = new ObjectMapper();
			JsonNode jsonObject = mapper.readTree( json );
			Map<String, String> record = new LinkedHashMap<String, String>();
			Iterator<JsonNode> it = jsonObject.elements();
			Iterator<String> itn = jsonObject.fieldNames();
			while (it.hasNext()) {
				JsonNode child = it.next();
				String name = itn.next();
				Field field = schema.getField(name);
				if (field == null) {
					if(!unmapField.contains(name)) {
						unmapField.add(name);
						StringBuffer strBuff = new StringBuffer();
						for(String data : unmapField) {
							strBuff.append(data+" ");
						}
					}
					continue;
				}
				Schema childSchema = field.schema();
				if (child.isInt()) {
					if (acceptsInt(childSchema)) {
						record.put("\"" + name  + "\"", String.valueOf( child.intValue() ));
					} else if (acceptsLong(childSchema)) {
						record.put("\"" + name  + "\"", String.valueOf( (long) child.intValue() ));
					} else if (acceptsFloat(childSchema)) {
						record.put("\"" + name  + "\"", String.valueOf( (float) child.intValue() ));
					} else if (acceptsDouble(childSchema)) {
						record.put("\"" + name  + "\"", String.valueOf( (double) child.intValue() ));
					} else if (acceptsText(childSchema)) {
						record.put("\"" + name  + "\"", String.valueOf( child.intValue() ));
					}else {
						errorRecord.setErrorMsg("   Can't store an int in field : [" + name + " : "+ child.intValue() + "] ::: Accepted Types : "+childSchema);
					}
				}else if (child.isBigInteger()) {
						if (acceptsInt(childSchema)) {
							record.put("\"" + name  + "\"", String.valueOf(  child.bigIntegerValue() ));
						} else if (acceptsLong(childSchema)) {
							record.put("\"" + name  + "\"", String.valueOf(  child.bigIntegerValue() ));
						}else {
							record.put("\"" + name  + "\"", String.valueOf(  child.bigIntegerValue() ));
						}
				}else if (child.isLong()) {
					if (acceptsLong(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf(  child.longValue()));
					} else if (acceptsInt(childSchema)) {
						if( child.longValue() > 0) {
							record.put("\"" + name  + "\"",  String.valueOf( Integer.MAX_VALUE));
						}else {
							record.put("\"" + name  + "\"",  String.valueOf( Integer.MIN_VALUE));
						}
					} else if (acceptsFloat(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf(  (float) child.longValue()));
					} else if (acceptsDouble(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf(  ( double ) child.longValue()));
					} else if (acceptsText(childSchema)) {
						record.put("\"" + name  + "\"", String.valueOf( child.longValue() ));
					}else {
						errorRecord.setErrorMsg("   Can't store a long in field :[" + name + " : "+ child.longValue() + "] ::: Accepted Type : " + childSchema);
						isValidJson = false;
					}
				} else if (child.isBinary()) {
					try {
						if (acceptsBinary(childSchema))
						record.put("\"" + name  + "\"",  String.valueOf( child.binaryValue()));
					} catch (IOException e) {
						errorRecord.setErrorMsg("   Can't store a binary in field :[" + name + " : "+ child.binaryValue() + "] ::: Accepted Type : " + childSchema);
					}
				} else if (child.isBoolean()) {
					if (acceptsBoolean(childSchema)) {
					record.put("\"" + name  + "\"",  String.valueOf( child.booleanValue()));
					}else if (acceptsText(childSchema)) {
						record.put("\"" + name  + "\"", String.valueOf( child.booleanValue() ));
					}
					else {
						errorRecord.setErrorMsg("   Can't store a boolean in field :[" + name + " : "+ child.booleanValue() + "] ::: Accepted Type : " + childSchema);
						isValidJson = false;
					}
				
				} else if (child.isDouble()) {
					if (acceptsDouble(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf( child.doubleValue()));
					} else if (acceptsFloat(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf( child.doubleValue()));
					} else if (acceptsInt(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf( (int) child.doubleValue()));
					}else if (acceptsLong(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf( (long) child.doubleValue()));
					}else if (acceptsText(childSchema)) {
						record.put("\"" + name  + "\"", String.valueOf( child.doubleValue() ));
					}else {
						errorRecord.setErrorMsg("   Can't store a double in field :[" + name+ " : "+ child.doubleValue() + "] ::: Accepted Type : " + childSchema);
						isValidJson = false;
					}
				} else if (child.isFloat()) {
					if (acceptsFloat(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf( child.floatValue()));
					} else if (acceptsDouble(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf( child.floatValue()));
					} else if (acceptsInt(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf( (int)  child.floatValue()));
					}else if (acceptsLong(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf( (long)  child.floatValue()));
					}else if (acceptsText(childSchema)) {
						record.put("\"" + name  + "\"", String.valueOf( child.floatValue() ));
					}else {
						errorRecord.setErrorMsg("  Can't store a Float in field :[" + name+ " : "+  child.floatValue() + "] ::: Accepted Type : " + childSchema);
						isValidJson = false;
					}
				}
				else if (child.isTextual()) {
					if (acceptsText(childSchema)) {
						record.put("\"" + name  + "\"", "\"" + String.valueOf( child.textValue()) + "\"");
					}else if (acceptsDouble(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf( Double.parseDouble(child.textValue())));
					} else if (acceptsFloat(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf( Float.parseFloat(child.textValue())));
					} else if (acceptsInt(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf( Integer.parseInt(child.textValue())));
					}else if (acceptsLong(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf( Long.parseLong(child.textValue())));
					}else if (acceptsBoolean(childSchema)) {
						record.put("\"" + name  + "\"",  String.valueOf( Boolean.parseBoolean(child.textValue())));
					} else {
						errorRecord.setErrorMsg("  Can't store a text in field :[" + name+ " : "+ child.textValue() + "] ::: Accepted Type : " + childSchema);
						isValidJson = false;
					}
				}
			}
			   
			
			StringBuffer result = new StringBuffer();
	        // Get flatten json, for that transform map -> json
	        result.append( "{\n" + record.entrySet().stream().map(entry -> entry.getKey() + " : " + entry.getValue()).collect(Collectors.joining(",\n"))+ "\n}" ) ;
	
	        obj = new DataObject();
			obj.setValid( isValidJson );
			if( isValidJson ) {
				 obj.setJson( result.toString() );
			} else {
				errorRecord.setErrorJson(json);
				obj.setJson(errorRecord.toErrorJson());
			}
		}catch( Exception e) {
			errorRecord.setErrorJson(json);
			obj = new DataObject();
			obj.setValid( false );
			errorRecord.setErrorMsg( e.getMessage() );
			obj.setJson(errorRecord.toErrorJson());
		}
		
		return obj;
	}
	private static boolean accepts(Schema childSchema, Schema.Type type) {
		if (childSchema.getType() == type)
			return true;
		if (childSchema.getType() == Schema.Type.UNION) {
			for (Schema s : childSchema.getTypes()) {
				if (s.getType() == type) {
					return true;
				}
			}
		}
		return false;
	}
	private static boolean acceptsFloat(Schema childSchema) {
		return accepts(childSchema, Schema.Type.FLOAT);
	}
	private static boolean acceptsDouble(Schema childSchema) {
		return accepts(childSchema, Schema.Type.DOUBLE);
	}
	private static boolean acceptsLong(Schema childSchema) {
		return accepts(childSchema, Schema.Type.LONG);
	}
	private static boolean acceptsInt(Schema childSchema) {
		return accepts(childSchema, Schema.Type.INT);
	}
	
	private static boolean acceptsBoolean(Schema childSchema) {
		return accepts(childSchema, Schema.Type.BOOLEAN);
	}
	
	private static boolean acceptsBinary(Schema childSchema) {
		return accepts(childSchema, Schema.Type.BYTES);
	}
	private static boolean acceptsText(Schema childSchema) {
		return accepts(childSchema, Schema.Type.STRING);
	}
	
	public static String getDateTime() {
		format.setTimeZone( TimeZone.getTimeZone("UTC") );
		return format.format( new Date() );
	}
	

	
}

