package com.rastream.topology;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class ExperimentConfig {
    public static final long SA_SEED = Long.getLong("RA_SA_SEED", 42L);
    public static final String RUN_ID = System.getProperty("RA_RUN_ID", UUID.randomUUID().toString());
    public static final String PROFILE = System.getProperty("RA_PROFILE", "default");
    public static final int WARMUP_SEC = Integer.getInteger("RA_WARMUP_SEC", 30);
    public static final int STAGE_SEC = Integer.getInteger("RA_STAGE_SEC", 60);
    public static final List<Integer> RATE_SEQUENCE = Arrays.asList(1000, 2000, 3000, 4500, 2700, 2000);
}