package io.metersphere.streaming.report.realtime;

public interface SummaryRealtime<T> {
    String getReportKey();

    T execute(String reportId, int resourceIndex);
}
