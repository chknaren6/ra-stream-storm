package com.rastream.experiments;

public class FluctuatingStreamExperiment {
    public static int rateAtMinute(int minute) {
        if (minute < 10) return 1800;
        if (minute < 20) return 1800 + (minute - 10) * (4500 - 1800) / 10;
        if (minute < 30) return 4500;
        if (minute < 40) return 4500 - (minute - 30) * (4500 - 2700) / 10;
        if (minute < 50) return 2700 - (minute - 40) * (2700 - 2000) / 10;
        return 2000 + (minute - 50) * (3500 - 2000) / 10;
    }
}