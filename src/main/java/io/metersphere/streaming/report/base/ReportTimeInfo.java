package io.metersphere.streaming.report.base;

import lombok.Data;

import java.util.Map;

@Data
public class ReportTimeInfo {

    private long duration;
    private long startTime;
    private long endTime;

    // 实时报告计算内部使用
    private Map<Integer, Long> realtimeSumDurations;
}
