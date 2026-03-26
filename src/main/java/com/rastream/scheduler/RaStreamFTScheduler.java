package com.rastream.scheduler;

import com.rastream.faulttolerance.RecoveryManager;
import java.util.*;

public class RaStreamFTScheduler {
    private final RecoveryManager recoveryManager;

    public RaStreamFTScheduler(RecoveryManager recoveryManager) {
        this.recoveryManager = recoveryManager;
    }

    public Map<String, String> assignInitial(List<String> tasks, List<String> workers) {
        Map<String, String> m = new LinkedHashMap<>();
        int i = 0;
        for (String t : tasks) {
            String w = workers.get(i++ % workers.size());
            m.put(t, w);
            recoveryManager.registerTask(t, w);
        }
        return m;
    }

    public Map<String, String> onWorkerFailure(String failedWorker, List<String> aliveWorkers) {
        return recoveryManager.recoverNodeFailure(failedWorker, aliveWorkers);
    }
}