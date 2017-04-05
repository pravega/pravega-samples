package com.emc.pravega.example.statesynchronizer;

import java.io.Serializable;
import java.util.Map;
import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;

/**
 * An example implementation of the SharedConfig interface that works against the shared state copy of the map.
 * Although access to properties is less efficient than a version with an in memory cache, the access is never stale.
 *
 * @param <K>
 * @param <V>
 */
public class SharedConfigSynchronizedImpl<K extends Serializable, V extends Serializable> implements SharedConfig<K, V> {
    private final SynchronizedMap<K,V> properties; //This is the shared state
    
    public SharedConfigSynchronizedImpl(ClientFactory clientFactory, StreamManager streamManager, String scope, String name){
        this.properties = new SynchronizedMap<K, V>(clientFactory, streamManager, scope, name);
    }

    /*
     * Update the local in memory representation by fetching from the Synchronized state and then copying to local
     * (non-Javadoc)
     * @see com.emc.pravega.example.statesynchronizer.SharedConfig#synchronize()
     */
    @Override
    public void synchronize() {
        properties.refresh();
    }

    @Override
    public Map<K, V> getProperties() {
        return properties.clone();
    }

    @Override
    public V getProperty(K key) {
        return properties.get(key);
    }

    @Override
    public V putProperty(K key, V value) {
        return properties.put(key, value);
    }

    @Override
    public V removeProperty(K key) {
        return properties.remove(key);
    }

    @Override
    public void close() {
    }

}
