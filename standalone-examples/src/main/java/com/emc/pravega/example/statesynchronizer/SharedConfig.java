/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.example.statesynchronizer;

import java.io.Serializable;
import java.util.Map;
import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;

/**
 * An example Configuration object that wrappers a properties map implemented by a SharedMap.
 *
 * @param <K> - Key of the properties map.
 * @param <V> - Value of the properties map.
 */
public class SharedConfig<K extends Serializable, V extends Serializable> {
    private final SharedMap<K,V> properties; //This is the shared state
    
    public SharedConfig(ClientFactory clientFactory, StreamManager streamManager, String scope, String name){
        this.properties = new SharedMap<K, V>(clientFactory, streamManager, scope, name);
    }

    public void synchronize() {
        properties.refresh();
    }

    public Map<K, V> getProperties() {
        return properties.clone();
    }

    public V getProperty(K key) {
        return properties.get(key);
    }

    public V putProperty(K key, V value) {
        return properties.put(key, value);
    }
    
    public V putPropertyIfAbsent(K key, V value) {
        return properties.putIfAbsent(key, value);
    }

    public V removeProperty(K key) {
        return properties.remove(key);
    }
    
    public boolean removeProperty(K key, V oldValue) {
        return properties.remove(key, oldValue);
    }
    
    public V replaceProperty(K key, V value){
        return properties.replace(key, value);
    }
    
    public boolean replaceProperty(K key, V oldValue, V newValue) {
        return properties.replace(key, oldValue, newValue);
    }

    public void clear() {
        properties.clear();
    }
    
    public void close() {
        properties.close();
    }
}
