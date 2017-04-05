package com.emc.pravega.example.statesynchronizer;

import java.util.Map;

public interface SharedConfig<K,V> {
    
    /**
     * Update the internal representation of the SharedConfig object from the synchronized state
     */
    public void synchronize();
    
    /**
     * Return a Map of key/value pairs containing the properties in the SharedConfig
     * 
     * @return A Map of properties
     */
    public Map<K, V> getProperties();
    
    /**
     * Return a value corresponding to a given property key.
     * 
     * @param key = a representation of a configuration property key
     * @return value = An configuration object associated with the given key, or null if no such property can be found
     */
    public V getProperty(K key);
    
    /**
     * Create or modify the SharedConfig by inserting the given key value pair.
     * If a property with the given key exists in the SharedConfig, update its value with the given value.
     * If a property with the given key does not exist, insert it into the Sharedconfig with the given value.
     * 
     * @param key - the key of the property to be created/updated.
     * @param value - the value of the new/updated property.
     * @return oldvalue - the previous value of the configuration property, or null if no such property previously existed.
     */
    public V putProperty(K key, V value);
    
    /**
     * Remove the property identified by the given key from the SharedConfig.
     * 
     * @param key - the key of the property to be removed.
     * @return oldvalue - the value of the removed configuration property, or null if no such property existed.
     */
    public V removeProperty(K key);

    /**
     * Close the SharedConfig object, freeing up any resources used (like the Synchronizer)
     */
    public void close();
}
