package io.metersphere.streaming.report.summary;

import io.metersphere.streaming.base.domain.LoadTestReportResultPart;

@FunctionalInterface
public interface SummaryAction {
    void execute(LoadTestReportResultPart part);
}
