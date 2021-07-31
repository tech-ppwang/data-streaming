package io.metersphere.streaming.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.metersphere.streaming.base.domain.LoadTestReportResult;
import io.metersphere.streaming.base.domain.LoadTestReportResultPart;
import io.metersphere.streaming.base.domain.LoadTestReportWithBLOBs;
import io.metersphere.streaming.base.mapper.LoadTestReportMapper;
import io.metersphere.streaming.base.mapper.LoadTestReportResultMapper;
import io.metersphere.streaming.base.mapper.LoadTestReportResultPartMapper;
import io.metersphere.streaming.base.mapper.ext.ExtLoadTestMapper;
import io.metersphere.streaming.base.mapper.ext.ExtLoadTestReportMapper;
import io.metersphere.streaming.base.mapper.ext.ExtLoadTestReportResultMapper;
import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.commons.constants.TestStatus;
import io.metersphere.streaming.commons.utils.LogUtil;
import io.metersphere.streaming.report.summary.SummaryFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.UUID;

@Service
@Transactional(rollbackFor = Exception.class)
public class TestResultSaveService {
    @Resource
    private LoadTestReportResultMapper loadTestReportResultMapper;
    @Resource
    private LoadTestReportResultPartMapper loadTestReportResultPartMapper;
    @Resource
    private ExtLoadTestReportResultMapper extLoadTestReportResultMapper;
    @Resource
    private ExtLoadTestReportMapper extLoadTestReportMapper;
    @Resource
    private ExtLoadTestMapper extLoadTestMapper;
    @Resource
    private LoadTestReportMapper loadTestReportMapper;
    @Resource
    private ObjectMapper objectMapper;

    public void saveResult(LoadTestReportResult record) {
        int i = extLoadTestReportResultMapper.updateReportValue(record);
        if (i == 0) {
            loadTestReportResultMapper.insertSelective(record);
        }
    }

    public boolean isReportingSet(String reportId) {
        int i = extLoadTestReportResultMapper.updateReportStatus(reportId, ReportKeys.ResultStatus.name(), "Ready", "Reporting");
        return i != 0;
    }

    public void saveReportReadyStatus(String reportId) {
        extLoadTestReportResultMapper.updateReportStatus(reportId, ReportKeys.ResultStatus.name(), "Reporting", "Ready");
    }

    public void saveReportCompletedStatus(String reportId) {
        // 保存最终 为 completed
        extLoadTestReportResultMapper.updateReportStatus(reportId, ReportKeys.ResultStatus.name(), "Reporting", "Completed");
        extLoadTestReportResultMapper.updateReportStatus(reportId, ReportKeys.ResultStatus.name(), "Ready", "Completed");
    }

    public void saveResultPart(LoadTestReportResultPart testResult) {
        if (loadTestReportResultPartMapper.updateByPrimaryKeyWithBLOBs(testResult) == 0) {
            loadTestReportResultPartMapper.insert(testResult);
        }
        extLoadTestReportMapper.updateStatus(testResult.getReportId(), TestStatus.Running.name(), TestStatus.Starting.name());
        LoadTestReportWithBLOBs report = loadTestReportMapper.selectByPrimaryKey(testResult.getReportId());
        extLoadTestMapper.updateStatus(report.getTestId(), TestStatus.Running.name(), TestStatus.Starting.name());
    }

    @Async
    public void saveSummary(String reportId, String reportKey) {
        try {
            Object summary = SummaryFactory.getSummaryExecutor(reportKey).execute(reportId);
            LoadTestReportResult record = new LoadTestReportResult();
            record.setId(UUID.randomUUID().toString());
            record.setReportId(reportId);
            record.setReportKey(reportKey);
            record.setReportValue(objectMapper.writeValueAsString(summary));
            saveResult(record);
        } catch (Exception e) {
            LogUtil.error(e);
        }
    }
}
