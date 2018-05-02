/*
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.pravega.examples.flink.primer.datatype;

import java.io.Serializable;

/*
 * WordCount event that contains the word and the summary count
 */
public class IntegerEvent implements Serializable {

    public static final long start = -1;
    public static final long end = -9999;

    private long value;

    public IntegerEvent() {}

    public IntegerEvent(long value)
    {
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public boolean isStart() {
        return value == start;
    }

    public boolean isEnd() {
        return value == end;
    }

    @Override
    public String toString() {
        return "IntegerEvent: " + value;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (value ^ (value >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(this==obj){
            return true;
        }
        if(!(obj instanceof IntegerEvent)){
            return false;
        }
        IntegerEvent event = (IntegerEvent) obj;
        return  event.getValue() == value
                ;
    }

}
