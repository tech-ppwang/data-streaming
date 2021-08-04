package io.metersphere.streaming.report.realtime;

import io.metersphere.streaming.base.domain.LoadTestReportResultRealtime;

@FunctionalInterface
public interface SummaryRealtimeAction {
    void execute(LoadTestReportResultRealtime resultRealtime);
}
