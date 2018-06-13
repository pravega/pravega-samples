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

package io.pravega.example.flink.wordcount;

import java.io.Serializable;

/*
 * WordCount event that contains the word and the summary count
 */
public class WordCount implements Serializable {

    private String word;
    private int count;

    public WordCount() {}

    public WordCount(String word, int count) {
        this.count = count;
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getWord() {return word;}

    public void setWord(String word) {this.word = word;}

    @Override
    public String toString() {
        return "Word: "+ word + ": " + " Count: " + count;
    }
}
