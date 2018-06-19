package etl.api.parameter;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class Parameters {

    private final Map<String,Parameter> parameters;

    public Parameters() {
        this.parameters = new LinkedHashMap<>();
    }

    public Parameters(Map<String,Parameter> parameters) {
       this.parameters = parameters;
    }

    public void setLong(String name, Long value) {
        parameters.put(name, new LongParameter(name, value));
    }

    public void setLong(String name, String value) {
        setLong(name, Long.parseLong(value));
    }

    public void setString(String name, String value) {
        parameters.put(name, new StringParameter(name, value));
    }

    public void setInt(String name, Integer value) {
        parameters.put(name, new IntParameter(name, value));
    }

    public void setInt(String name, String value) {
        setInt(name, value == null ? null : Integer.parseInt(value));
    }

    public void setBoolean(String key,boolean value) {
        parameters.put(key, new BooleanParameter(key, value));
    }

    public Boolean getBoolean(String key) {
        if (parameters.containsKey(key)) {
            return (Boolean) parameters.get(key).getValue();
        }
        return false;
    }

    public void setDate(String name, Date value) {
        parameters.put(name, new DateParameter(name, value));
    }

    public void setDate(String name, String value, String format) throws ParseException {
        SimpleDateFormat df = new SimpleDateFormat(format);
        setDate(name, value == null ? null : df.parse(value));
    }

    public void setDouble(String name, Double value) {
        parameters.put(name, new DoubleParameter(name, value));
    }

    public void setDouble(String name, String value) {
        setDouble(name, value == null ? null : Double.parseDouble(value));
    }

    /**
     * Typesafe Getter for the Long represented by the provided key.
     *
     * @param key The key to get a value for
     * @return The <code>Long</code> value
     */
    public Long getLong(String key){
        if (!parameters.containsKey(key)) {
            return 0l;
        }
        return (Long) parameters.get(key).getValue();

    }

    /**
     * Typesafe Getter for the Long represented by the provided key.  If the
     * key does not exist, the default value will be returned.
     *
     * @param key to return the value for
     * @param defaultValue to return if the value doesn't exist
     * @return the parameter represented by the provided key, defaultValue
     * otherwise.
     */
    public Long getLong(String key, long defaultValue){
        if(parameters.containsKey(key)){
            return getLong(key);
        }
        else{
            return defaultValue;
        }
    }

    /**
     * Typesafe Getter for the String represented by the provided key.
     *
     * @param key The key to get a value for
     * @return The <code>String</code> value
     */
    public String getString(String key){
        Parameter value = parameters.get(key);
        return value.getValue() == null ? null : value.getValue().toString();
    }

    public Integer getInt(String key) {
        if (!parameters.containsKey(key)) {
            return 0;
        }
        Parameter value =  parameters.get(key);
        return (Integer) value.getValue();
    }

    public Integer getInt(String key, Integer defaultValue) {
        if (!parameters.containsKey(key)) {
            return defaultValue;
        }
        return getInt(key);
    }

    /**
     * Typesafe Getter for the String represented by the provided key.  If the
     * key does not exist, the default value will be returned.
     *
     * @param key to return the value for
     * @param defaultValue to return if the value doesn't exist
     * @return the parameter represented by the provided key, defaultValue
     * otherwise.
     */
    public String getString(String key, String defaultValue){
        if(parameters.containsKey(key)){
            return getString(key);
        }
        else{
            return defaultValue;
        }
    }

    /**
     * Typesafe Getter for the Long represented by the provided key.
     *
     * @param key The key to get a value for
     * @return The <code>Double</code> value
     */
    public Double getDouble(String key){
        if (!parameters.containsKey(key)) {
            return 0.0;
        }
        return (Double) parameters.get(key).getValue();
    }

    /**
     * Typesafe Getter for the Double represented by the provided key.  If the
     * key does not exist, the default value will be returned.
     *
     * @param key to return the value for
     * @param defaultValue to return if the value doesn't exist
     * @return the parameter represented by the provided key, defaultValue
     * otherwise.
     */
    public Double getDouble(String key, double defaultValue){
        if(parameters.containsKey(key)){
            return getDouble(key);
        }
        else{
            return defaultValue;
        }
    }

    /**
     * Typesafe Getter for the Date represented by the provided key.
     *
     * @param key The key to get a value for
     * @return The <code>java.util.Date</code> value
     */
    public Date getDate(String key){
        return this.getDate(key,null);
    }

    /**
     * Typesafe Getter for the Date represented by the provided key.  If the
     * key does not exist, the default value will be returned.
     *
     * @param key to return the value for
     * @param defaultValue to return if the value doesn't exist
     * @return the parameter represented by the provided key, defaultValue
     * otherwise.
     */
    public Date getDate(String key, Date defaultValue){
        if(parameters.containsKey(key)){
            return (Date)parameters.get(key).getValue();
        }
        else{
            return defaultValue;
        }
    }

	public Object getObject(String key) {
		if (parameters.containsKey(key)) {
			return parameters.get(key).getValue();
		} else {
			return null;
		}
	}

    /**
     * Get a map of all parameters, including string, long, and date.
     *
     * @return an unmodifiable map containing all parameters.
     */
    public Map<String, Parameter> getParameters(){
        return Collections.unmodifiableMap(parameters);
    }

    /**
     * @return true if the parameters is empty, false otherwise.
     */
	@JsonIgnore
    public boolean isEmpty(){
        return parameters.isEmpty();
    }



}
