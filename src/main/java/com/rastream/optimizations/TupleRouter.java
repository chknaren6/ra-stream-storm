package com.rastream.optimizations;

import java.util.List;

public class TupleRouter {
    public String routeByKey(String key, List<String> workers) {
        int idx = Math.abs(key.hashCode()) % workers.size();
        return workers.get(idx);
    }
}