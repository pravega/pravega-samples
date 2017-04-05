package com.emc.pravega.example.statesynchronizer;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.mutable.MutableBoolean;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;
import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revision;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.JavaSerializer;

/**
 * A Map<K, V> that uses Pravega StateSynchronizer to maintain a shared, consistent, optimistically locked, concurrently
 * accessible representation.
 * 
 * @oaram <K> - the type of key used in the map.
 * @param <V> - the value type used in the map.
 *
 */
public class SynchronizedMap<K extends Serializable, V extends Serializable> {

    /*
     * The internal Map representation that gets synchronized.  This is the "shared state" object.
     */
    private static class SharedStateMap<K, V> implements Revisioned, Serializable {
        private static final long serialVersionUID = 1L;
        private final String scopedStreamName;
        private final ConcurrentHashMap<K, V> impl;
        private final Revision currentRevision;

        public SharedStateMap(String scopedStreamName, ConcurrentHashMap<K,V> impl, Revision revision){
            this.scopedStreamName = scopedStreamName;
            this.impl = impl;
            this.currentRevision = revision;
        }
        
        @Override
        public Revision getRevision() {
            return currentRevision;
        }

        @Override
        public String getScopedStreamName() {
            return scopedStreamName;
        }

        public int size() {
            return impl.size();
        }

        public boolean containsKey(K key) {
            return impl.containsKey(key);
        }

        public boolean containsValue(V value) {
            return impl.containsValue(value);
        }

        public Set<Entry<K, V>> entrySet() {
            return impl.entrySet();
        }

        public V get(K key) {
            return impl.get(key);
        }

        public Collection<V> values() {
            return impl.values();
        }
        
        public Map<K,V> clone() {
            return new ConcurrentHashMap<K,V>(impl);
        }
    }
    
    /**
     * Create a Map. This is used by StateSynchronizer to initialize shared state.
     */
    private static class CreateState<K, V> implements InitialUpdate<SharedStateMap<K,V>>, Serializable {
        private static final long serialVersionUID = 1L;
        private final ConcurrentHashMap<K, V> impl;
        
        public CreateState(ConcurrentHashMap<K, V> impl) {
            this.impl = impl;
        }

        @Override
        public SharedStateMap<K, V> create(String scopedStreamName, Revision revision) {
            return new SharedStateMap<K, V>(scopedStreamName, impl, revision);
        }
    }
    
    /**
     * A base class for all updates to the shared state. This allows for several different types of updates.
     */
    private static abstract class StateUpdate<K,V> implements Update<SharedStateMap<K,V>>, Serializable {
        private static final long serialVersionUID = 1L;
        
        @Override
        public SharedStateMap<K,V> applyTo(SharedStateMap<K,V> oldState, Revision newRevision) {
            ConcurrentHashMap<K, V> newState = new ConcurrentHashMap<K, V>(oldState.impl);
            process(newState);
            return new SharedStateMap<K,V>(oldState.getScopedStreamName(), newState, newRevision);
        }

        public abstract void process(ConcurrentHashMap<K, V> updatableList);
    }
    
    /**
     * Clear the State.
     */
    private static class Clear<K, V> extends StateUpdate<K,V> {
        private static final long serialVersionUID = 1L;

        public Clear() {
        }

        @Override
        public void process(ConcurrentHashMap<K, V> impl) {
            impl.clear();
        }
    }
    
    /**
     * Add a key/value pair to the State.
     */
    private static class Put<K,V> extends StateUpdate<K,V> {
        private static final long serialVersionUID = 1L;
        private final K key;
        private final V value;

        public Put(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void process(ConcurrentHashMap<K, V> impl) {
            impl.put(key, value);
        }
    }
    
    /**
     * Add a number of key/value pairs to the State.
     */
    private static class PutAll<K,V> extends StateUpdate<K,V> {
        private static final long serialVersionUID = 1L;
        private final Map<? extends K, ? extends V> map;

        public PutAll(Map<? extends K, ? extends V> map) {
            this.map = map;
        }

        @Override
        public void process(ConcurrentHashMap<K, V> impl) {
            impl.putAll(map);
        }
    }
    
    /**
     * Remove a key/value pair from the State.
     */
    private static class Remove<K,V> extends StateUpdate<K,V> {
        private static final long serialVersionUID = 1L;
        private final K key;

        public Remove(K key) {
            this.key = key;
        }

        @Override
        public void process(ConcurrentHashMap<K, V> impl) {
            impl.remove(key);
        }
    }
    
    //+++++++++++++++++++++++++++++++++ SynchronizedMap Behavior +++++++++++++++++++++++++++++++++
    private static final int REMOVALS_BEFORE_COMPACTION = 5;
    

    private final StateSynchronizer<SharedStateMap<K,V>> stateSynchronizer;
    private final AtomicInteger countdownToCompaction = new AtomicInteger(REMOVALS_BEFORE_COMPACTION);
    
    /*
     * Creates the shared state using a synchronizer based on the given stream name.
     */
    public SynchronizedMap(ClientFactory clientFactory, StreamManager streamManager, String scope, String name){
        streamManager.createScope(scope);
        
        StreamConfiguration streamConfig = StreamConfiguration.builder().scope(scope).streamName(name)
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        
        streamManager.createStream(scope, name, streamConfig);
        
        this.stateSynchronizer = clientFactory.createStateSynchronizer(name,
                                                new JavaSerializer<StateUpdate<K,V>>(),
                                                new JavaSerializer<CreateState<K,V>>(),
                                                SynchronizerConfig.builder().build());
        
        stateSynchronizer.initialize(new CreateState<K,V>(new ConcurrentHashMap<K,V>()));
    }
    
    //++++++++++++++++++++++++++++++++ Synchronizer-oriented API ++++++++++++++++++++++++++++++++
    
    /**
     * Make sure the internal representation of the state is current with respect to the stateSynchronizer.
     */
    public void refresh() {
        stateSynchronizer.fetchUpdates();
    }
    
    /*
     * Use the StateSynchronizer to compact the shared state after some number of removals
     */
    private void compact() {
        countdownToCompaction.set(REMOVALS_BEFORE_COMPACTION);
        stateSynchronizer.compact(state -> new CreateState<K,V>(state.impl));
    }
    
    //+++++++++++++++++++++++++++++++++++ Map-oriented API ++++++++++++++++++++++++++++++++++++
    
    /*
     * Removes all of the mappings from this map.
     * Use the StateSynchronizer to compact the shared state.
     */
    public void clear(){
        stateSynchronizer.updateState(state -> { 
            if (state.size() > 0) {
                return Collections.singletonList(new Clear<K,V>());
            } else {
                return Collections.emptyList();
            }
        });
        compact();
    }
    
    /*
     * Returns a copy of the map
     */
    public Map<K,V> clone() {
        return stateSynchronizer.getState().clone();
    }
    
    /*
     * Returns true if this map contains a mapping for the specified key.
     */
    public boolean containsKey(K key){
        return stateSynchronizer.getState().containsKey(key);
    }
    
    /*
     * Returns true if this map maps one or more keys to the specified value.
     */
    public boolean containsValue(V value){
        return stateSynchronizer.getState().containsValue(value);
    }

    /*
     * Returns a Set view of the mappings contained in this map.
     */
    public Set<Map.Entry<K,V>> entrySet(){
        return stateSynchronizer.getState().entrySet();
    }
    
    /*
     * Returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
     */
    public V get(K key){
        return stateSynchronizer.getState().get(key);
    }
    
    /*
     * Associates the specified value with the specified key in this map.
     */
    public V put(K key, V value){
        final V oldValue = get(key);
        stateSynchronizer.updateState(state -> {
            return Collections.singletonList(new Put<K,V>(key,value));
        });
        return oldValue;
    }
    
    /*
     * Copies all of the mappings from the specified map to this map.
     */
    public void putAll(Map<K,V> map){
        stateSynchronizer.updateState(state -> {
            return Collections.singletonList(new PutAll<K,V>(map));
        });
    }
    
    /*
     * If the specified key is not already associated with a value (or is mapped to null) associates it with the given 
     * value and returns null, else returns the current value.
     */
    public V putIfAbsent(K key, V value){
        @SuppressWarnings("unchecked")
        final V[] ret = (V[]) new Object[1];
        stateSynchronizer.updateState(state -> {
            if (state.containsKey(key) && state.get(key) != null) {
                ret[0] = state.get(key);
                return Collections.emptyList();
            } else {
                ret[0] = null;
                return Collections.singletonList(new Put<K,V>(key,value));
            }
        });
        return ret[0];
    }
    
    /*
     * Removes the mapping for the specified key from this map if present.
     * Uses the countDown to determine if it is also time to compact the SharedState after removal
     */
    public V remove(K key) {
        final V oldValue = get(key);
        stateSynchronizer.updateState(state -> {
            if (state.impl.containsKey(key)) {
                return  Collections.singletonList(new Remove<K,V>(key));
            } else {
                return Collections.emptyList();
            }
        });
        
        if (countdownToCompaction.decrementAndGet() <= 0) {
            compact();
        }
        return oldValue;
    }
    
    /*
     * Removes the entry for the specified key only if it is currently mapped to the specified value.
     */
    public boolean remove(K key, V value){
        MutableBoolean ret = new MutableBoolean(false);
        stateSynchronizer.updateState(state -> {
            if (state.impl.containsKey(key) && state.impl.get(key).equals(value)) {
                ret.setValue(true);
                return  Collections.singletonList(new Remove<K,V>(key));
            } else {
                return Collections.emptyList();
            }
        });
        
        if (countdownToCompaction.decrementAndGet() <= 0) {
            compact();
        }
        return ret.isTrue();
    }
    
    /*
     * Replaces the entry for the specified key only if it is currently mapped to some value.
     */
    public V replace(K key, V value){
        final V oldValue = get(key);
        stateSynchronizer.updateState(state -> {
            if (state.impl.containsKey(key)) {
                return  Collections.singletonList(new Put<K,V>(key,value));
            } else {
                return Collections.emptyList();
            }
        });
        return oldValue;
    }
    
    /*
     * Replaces the entry for the specified key only if currently mapped to the specified value.
     */
    public boolean replace(K key, V value, V newValue){
        MutableBoolean ret = new MutableBoolean(false);
        stateSynchronizer.updateState(state -> {
            if (state.impl.containsKey(key) && state.impl.get(key).equals(value)) {
                ret.setValue(true);
                return  Collections.singletonList(new Put<K,V>(key, newValue));
            } else {
                return Collections.emptyList();
            }
        });
        return ret.isTrue();
    }
    
    /*
     * Returns the number of key-value mappings in this map.
     */
    public int size(){
        return stateSynchronizer.getState().size();
    }
    
    /*
     * Returns a Collection view of the values contained in this map.
     */
    public Collection<V> values(){
        return stateSynchronizer.getState().values();
    }

    public void close() {
        if( stateSynchronizer != null) {
            //TODO how do you close a state synchronizer?
            //stateSynchronizer.close();
        }
    }
}
