/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   
 */
package io.pravega.example.statesynchronizer;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revision;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;

/**
 * A Map<K, V> that uses Pravega StateSynchronizer to maintain a shared, consistent, optimistically locked, concurrently
 * accessible representation.
 * 
 * @param <K> - the type of key used in the map.
 * @param <V> - the value type used in the map.
 *
 */
public class SharedMap<K extends Serializable, V extends Serializable> {

    /**
     * The internal Map representation that gets synchronized.  This is the "shared state" object.
     * 
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
    
    //+++++++++++++++++++++++++++++++++++ SharedMap Behavior +++++++++++++++++++++++++++++++++++
    
    private static final int REMOVALS_BEFORE_COMPACTION = 5;
    
    private final StateSynchronizer<SharedStateMap<K,V>> stateSynchronizer;
    private final AtomicInteger countdownToCompaction = new AtomicInteger(REMOVALS_BEFORE_COMPACTION);
    
    /**
     * Creates the shared state using a synchronizer based on the given stream name.
     * 
     * @param clientFactory - the Pravega EventStreamClientFactory to use to create the StateSynchronizer.
     * @param streamManager - the Pravega StreamManager to use to create the Scope and the Stream used by the StateSynchronizer
     * @param scope - the Scope to use to create the Stream used by the StateSynchronizer.
     * @param name - the name of the Stream to be used by the StateSynchronizer.
     */
    public SharedMap(SynchronizerClientFactory clientFactory, StreamManager streamManager, String scope, String name){
        streamManager.createScope(scope);
        
        StreamConfiguration streamConfig = StreamConfiguration.builder()
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
    
    /**
     * Use the StateSynchronizer to compact the shared state after some number of removal operations.
     */
    private void compact() {
        countdownToCompaction.set(REMOVALS_BEFORE_COMPACTION);
        stateSynchronizer.compact(state -> new CreateState<K,V>(state.impl));
    }
    
    //+++++++++++++++++++++++++++++++++++ Map-oriented API ++++++++++++++++++++++++++++++++++++
    
    /**
     * Removes all of the mappings from this map.
     * Use the StateSynchronizer to compact the shared state.
     */
    public void clear(){
        stateSynchronizer.updateState((state, updates) -> {
            if (state.size() > 0) {
                updates.add(new Clear<K,V>());
            }
        });
        compact();
    }
    
    /**
     * Create a copy of the map.
     * 
     * @return - the copy of the shared state map.
     */
    public Map<K,V> clone() {
        return stateSynchronizer.getState().clone();
    }
    
    /**
     * Determine if the given key appears in the map.
     * 
     * @param key - the key to search for.
     * @return - true if this map contains a mapping for the specified key.
     */
    public boolean containsKey(K key){
        return stateSynchronizer.getState().containsKey(key);
    }
    
    /**
     * Determine if the given value appears in the map.
     * 
     * @param value - the value to search for.
     * @returns - true if this map maps one or more keys to the specified value.
     */
    public boolean containsValue(V value){
        return stateSynchronizer.getState().containsValue(value);
    }

    /**
     * Render the map as a Set of entries.
     * 
     * @return - a Set view of the mappings contained in this map.
     */
    public Set<Map.Entry<K,V>> entrySet(){
        return stateSynchronizer.getState().entrySet();
    }
    
    /**
     * Return the value for the given key.
     * 
     * @param key - the key to search for.
     * @returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
     */
    public V get(K key){
        return stateSynchronizer.getState().get(key);
    }
    
    /**
     * Associates the specified value with the specified key in this map.
     * 
     * @param key - the key at which the value should be found.
     * @param value - the value to be entered into the map.
     * @return - the previous value (if it existed) for the given key or null if the key did not exist before this operation.
     */
    public V put(K key, V value){
        final AtomicReference<V> oldValue = new AtomicReference<V>(null);
        stateSynchronizer.updateState((state, updates) -> {
            oldValue.set(state.get(key));
            updates.add(new Put<K,V>(key,value));
        });
        return oldValue.get();
    }
    
    /**
     * Copies all of the mappings from the specified map to this map.
     * 
     * @param map - the map to copy.
     */
    public void putAll(Map<K,V> map){
        stateSynchronizer.updateState((state, updates) -> {
            updates.add(new PutAll<K,V>(map));
        });
    }
    
    /**
     * If the specified key is not already associated with a value (or is mapped to null) associates it with the given 
     * value and returns null, else returns the current value.
     * 
     * @param key - the key at which the value should be found.
     * @param value - the value to be entered into the map.
     * @return - the previous value (if it existed) for the given key or null if the key did not exist before this operation.
     */
    public V putIfAbsent(K key, V value){
        final AtomicReference<V> ret = new AtomicReference<V>();
        
        refresh();  //this is a conditional modifying operation, need to update local state with current shared state before checking the condition
        stateSynchronizer.updateState((state, updates) -> {
            if (state.containsKey(key) && state.get(key) != null) {
                ret.set(state.get(key));
            } else {
                ret.set(null);
                updates.add(new Put<K,V>(key,value));
            }
        });
        return ret.get();
    }
    
    /**
     * Removes the mapping for the specified key from this map if present.
     * Uses the countDown to determine if it is also time to compact the SharedState after removal.
     * 
     * @param key - the key to be removed.
     * @return - the previous value (if it existed) for the key to be removed or null if that key does not exist in the map.
     */
    public V remove(K key) {
        final AtomicReference<V> oldValue = new AtomicReference<V>(null);
        stateSynchronizer.updateState((state, updates) -> {
            if (state.impl.containsKey(key)) {
                oldValue.set(state.get(key));
                updates.add(new Remove<K,V>(key));
            } else {
                oldValue.set(null);
            }
        });
        
        if (countdownToCompaction.decrementAndGet() <= 0) {
            compact();
        }
        return oldValue.get();
    }
    
    /**
     * Removes the entry for the specified key only if it is currently mapped to the specified value.
     * Uses the countDown to determine if it is also time to compact the SharedState after removal
     * 
     * @param - the key to be removed.
     * @param - the expected value of the key to be removed.
     * @return - true if the key was removed, false otherwise.
     */
    public boolean remove(K key, V value){
        AtomicBoolean ret = new AtomicBoolean(false);
        stateSynchronizer.updateState((state, updates) -> {
            if (state.impl.containsKey(key) && state.impl.get(key).equals(value)) {
                ret.set(true);
                updates.add(new Remove<K,V>(key));
            } else {
                ret.set(false);
            }
        });
        
        if (countdownToCompaction.decrementAndGet() <= 0) {
            compact();
        }
        return ret.get();
    }
    
    /**
     * Replaces the entry for the specified key only if it is currently mapped to some value.
     * 
     * @param key - the key whose value is to be replaced.
     * @param value - the value to assign to the key.
     * @return - the previous value associated with the key, or null if the key does not exist in the map.
     */
    public V replace(K key, V value){
        final AtomicReference<V> oldValue = new AtomicReference<V>(null);
        stateSynchronizer.updateState((state, updates) -> {
            if (state.impl.containsKey(key)) {
                oldValue.set(state.get(key));
                updates.add(new Put<K,V>(key,value));
            } else {
                oldValue.set(null);
            }
        });
        return oldValue.get();
    }
    
    /**
     * Replaces the entry for the specified key only if currently mapped to the specified value.
     * 
     * @param key - the key whose value is to be replaced.
     * @param oldValue - the expected value of the key prior to replacement.
     * @param newValue - the value to be assigned to the key.
     * @return - true if the current value of the key matches oldvalue and therefore the key is updated with newValue.  False otherwise.
     */
    public boolean replace(K key, V oldValue, V newValue){
        AtomicBoolean ret = new AtomicBoolean(false);
        stateSynchronizer.updateState((state, updates) -> {
            if (state.impl.containsKey(key) && state.impl.get(key).equals(oldValue)) {
                ret.set(true);
                updates.add(new Put<K,V>(key, newValue));
            } else {
                ret.set(false);
            }
        });
        return ret.get();
    }
    
    /**
     * Determine the number of key-value mappings in the map.
     * 
     * @return - the number of key-value mappings in the map.
     */
    public int size(){
        return stateSynchronizer.getState().size();
    }
    
    /**
     * Render the set of values as a Collection.
     * 
     * @return - a Collection view of the values contained in this map.
     */
    public Collection<V> values(){
        return stateSynchronizer.getState().values();
    }

    /**
     * Close the SharedMap.
     */
    public void close() {
        if( stateSynchronizer != null) {
            stateSynchronizer.close();
        }
    }
}
