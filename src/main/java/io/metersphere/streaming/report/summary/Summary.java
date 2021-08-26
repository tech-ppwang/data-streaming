package io.metersphere.streaming.report.summary;

public interface Summary<T> {
    String getReportKey();

    T execute(String reportId);
}
