package com.emc.pravega.example.statesynchronizer;

import java.io.Serializable;
import java.util.Map;
import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;

/**
 * An example Configuration object that wrappers a properties map implemented by a SynchronizedMap.
 *
 * @param <K> - Key of the properties map.
 * @param <V> - Value of the properties map.
 */
public class SharedConfig<K extends Serializable, V extends Serializable> {
    private final SynchronizedMap<K,V> properties; //This is the shared state
    
    public SharedConfig(ClientFactory clientFactory, StreamManager streamManager, String scope, String name){
        this.properties = new SynchronizedMap<K, V>(clientFactory, streamManager, scope, name);
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

    public V removeProperty(K key) {
        return properties.remove(key);
    }

    public void clear() {
        properties.clear();
    }
    
    public void close() {
        properties.close();
    }
}
