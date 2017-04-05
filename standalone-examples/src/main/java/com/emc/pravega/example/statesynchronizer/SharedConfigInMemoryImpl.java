package com.emc.pravega.example.statesynchronizer;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;

/**
 * An example implementation of the SharedConfig interface that uses a local in memory map as a cache of the shared state.
 * Although access to properties is more efficient via an in memory cache of the state, the access could be stale.
 * Requires the consumer to occasionally refresh the cache from syncronized (shared) state.
 *
 * @param <K>
 * @param <V>
 */
public class SharedConfigInMemoryImpl<K extends Serializable, V extends Serializable> implements SharedConfig<K, V> {
    private final SynchronizedMap<K,V> properties; //This is the shared state
    private Map<K,V> localProperties;   //This is the local in memory copy of the shared state
    
    public SharedConfigInMemoryImpl(ClientFactory clientFactory, StreamManager streamManager, String scope, String name){
        this.properties = new SynchronizedMap<K, V>(clientFactory, streamManager, scope, name);
        this.localProperties = new ConcurrentHashMap<K,V>(properties.clone());
    }

    /*
     * Update the local in memory representation by fetching from the Synchronized state and then copying to local
     * (non-Javadoc)
     * @see com.emc.pravega.example.statesynchronizer.SharedConfig#synchronize()
     */
    @Override
    public void synchronize() {
        properties.refresh();
        localProperties = new ConcurrentHashMap<K,V>(properties.clone());
    }

    @Override
    public Map<K, V> getProperties() {
        return new ConcurrentHashMap<K,V>(localProperties);
    }

    @Override
    public V getProperty(K key) {
        return localProperties.get(key);
    }

    /*
     * After any update, resync the local in memory copy to make sure the in memory copy reflects the shared state post modification.
     * 
     * (non-Javadoc)
     * @see com.emc.pravega.example.statesynchronizer.SharedConfig#putProperty(java.lang.Object, java.lang.Object)
     */
    @Override
    public V putProperty(K key, V value) {
        V oldValue = properties.put(key, value);
        synchronize();
        return oldValue;
    }


    /*
     * After any update, resync the local in memory copy to make sure the in memory copy reflects the shared state post modification.
     * 
     * (non-Javadoc)
     * @see com.emc.pravega.example.statesynchronizer.SharedConfig#putProperty(java.lang.Object, java.lang.Object)
     */
    @Override
    public V removeProperty(K key) {
        V oldValue = properties.remove(key);
        synchronize();
        return oldValue;
    }

    @Override
    public void close() {
       //TODO how to close a synchronizer?
       //properties.close();
    }

}
