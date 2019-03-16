package io.pravega.example.streamprocessing;

import java.io.Serializable;

class State implements Serializable {
    private static final long serialVersionUID = -275148988691911596L;

    long sum;

    public State() {
        this.sum = 0;
    }

    @Override
    public String toString() {
        return "State{" +
                "sum=" + sum +
                '}';
    }
}
