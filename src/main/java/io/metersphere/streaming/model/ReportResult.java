package io.metersphere.streaming.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ReportResult {
    private String reportId;
    private String reportKey;
    private String resourceIndex;
    private Boolean completed;
    private Object content;
}
