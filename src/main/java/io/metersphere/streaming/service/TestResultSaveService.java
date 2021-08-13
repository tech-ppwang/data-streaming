package io.metersphere.streaming.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.metersphere.streaming.base.domain.*;
import io.metersphere.streaming.base.mapper.LoadTestReportMapper;
import io.metersphere.streaming.base.mapper.LoadTestReportResultMapper;
import io.metersphere.streaming.base.mapper.LoadTestReportResultPartMapper;
import io.metersphere.streaming.base.mapper.LoadTestReportResultRealtimeMapper;
import io.metersphere.streaming.base.mapper.ext.ExtLoadTestMapper;
import io.metersphere.streaming.base.mapper.ext.ExtLoadTestReportMapper;
import io.metersphere.streaming.base.mapper.ext.ExtLoadTestReportResultMapper;
import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.commons.constants.TestStatus;
import io.metersphere.streaming.commons.utils.LogUtil;
import io.metersphere.streaming.report.base.ReportTimeInfo;
import io.metersphere.streaming.report.base.TestOverview;
import io.metersphere.streaming.report.realtime.SummaryRealtimeFactory;
import io.metersphere.streaming.report.summary.SummaryFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
@Transactional(rollbackFor = Exception.class)
public class TestResultSaveService {
    @Resource
    private LoadTestReportResultMapper loadTestReportResultMapper;
    @Resource
    private LoadTestReportResultPartMapper loadTestReportResultPartMapper;
    @Resource
    private LoadTestReportResultRealtimeMapper loadTestReportResultRealtimeMapper;
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

    private final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(30, 30,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());

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

    public void saveReportPartReportingStatus(String reportId, int resourceIndex) {
        LoadTestReportResultPart testResult = new LoadTestReportResultPart();
        testResult.setReportId(reportId);
        testResult.setReportKey(ReportKeys.ResultStatus.name());
        testResult.setResourceIndex(resourceIndex);
        testResult.setReportValue(TestStatus.Reporting.name());
        saveResultPart(testResult);
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
    }

    public void saveSummary(String reportId, String reportKey) {
        try {
            Object summary = SummaryFactory.getSummaryExecutor(reportKey).execute(reportId);
//            LoadTestReportResult record = new LoadTestReportResult();
//            record.setId(UUID.randomUUID().toString());
//            record.setReportId(reportId);
//            record.setReportKey(reportKey);
//            record.setReportValue(objectMapper.writeValueAsString(summary));
//            saveResult(record);
        } catch (Exception e) {
            LogUtil.error("保存 [" + reportId + "], [" + reportKey + "] 报错了", e);
        }
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void forceSaveAllSummary(String reportId, List<String> reportKeys) {
        CountDownLatch countDownLatch = new CountDownLatch(reportKeys.size());
        for (String key : reportKeys) {
            threadPoolExecutor.execute(() -> {
                try {
                    saveSummary(reportId, key);
                } catch (Exception e) {
                    LogUtil.error("reportId: " + reportId + ", key:" + key, e);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (Exception e) {
            LogUtil.error(e);
        } finally {
            saveReportOverview(reportId);
            saveReportTimeInfo(reportId);
            saveReportReadyStatus(reportId);
        }
    }

    public void saveAllSummary(String reportId, List<String> reportKeys) {
        if (!isReportingSet(reportId)) {
            LogUtil.info("有其他线程正在执行汇总 {}", reportId);
            return;
        }
        forceSaveAllSummary(reportId, reportKeys);
    }

    public boolean checkReportStatus(String reportId) {
        LoadTestReportWithBLOBs report = loadTestReportMapper.selectByPrimaryKey(reportId);
        if (report == null) {
            LogUtil.warn("报告不存在: {}", reportId);
            return false;
        }
        extLoadTestReportMapper.updateStatus(reportId, TestStatus.Running.name(), TestStatus.Starting.name());
        extLoadTestMapper.updateStatus(report.getTestId(), TestStatus.Running.name(), TestStatus.Starting.name());
        return true;
    }

    public void saveReportOverview(String reportId) {
        LoadTestReportResultExample example1 = new LoadTestReportResultExample();
        example1.createCriteria().andReportIdEqualTo(reportId).andReportKeyEqualTo(ReportKeys.Overview.name());
        List<LoadTestReportResult> loadTestReportResults = loadTestReportResultMapper.selectByExampleWithBLOBs(example1);
        if (loadTestReportResults.size() > 0) {
            LoadTestReportResult loadTestReportResult = loadTestReportResults.get(0);
            String reportValue = loadTestReportResult.getReportValue();
            try {
                TestOverview testOverview = objectMapper.readValue(reportValue, TestOverview.class);
                LoadTestReportWithBLOBs report = new LoadTestReportWithBLOBs();
                report.setId(reportId);
                report.setMaxUsers(testOverview.getMaxUsers());
                report.setAvgResponseTime(testOverview.getAvgResponseTime());
                report.setTps(testOverview.getAvgTransactions());
                loadTestReportMapper.updateByPrimaryKeySelective(report);
            } catch (JsonProcessingException e) {
                LogUtil.error(e);
            }
        }
    }

    public void saveReportTimeInfo(String reportId) {
        LoadTestReportResultExample example1 = new LoadTestReportResultExample();
        example1.createCriteria().andReportIdEqualTo(reportId).andReportKeyEqualTo(ReportKeys.TimeInfo.name());
        List<LoadTestReportResult> loadTestReportResults = loadTestReportResultMapper.selectByExampleWithBLOBs(example1);
        if (loadTestReportResults.size() > 0) {
            LoadTestReportResult loadTestReportResult = loadTestReportResults.get(0);
            String reportValue = loadTestReportResult.getReportValue();
            try {
                ReportTimeInfo timeInfo = objectMapper.readValue(reportValue, ReportTimeInfo.class);
                LoadTestReportWithBLOBs report = new LoadTestReportWithBLOBs();
                report.setId(reportId);
                report.setTestStartTime(timeInfo.getStartTime());
                report.setTestEndTime(timeInfo.getEndTime());
                report.setTestDuration(timeInfo.getDuration());
                loadTestReportMapper.updateByPrimaryKeySelective(report);
            } catch (JsonProcessingException e) {
                LogUtil.error(e);
            }
        }
    }

    public void saveResultRealtime(LoadTestReportResultRealtime testResult) {
        loadTestReportResultRealtimeMapper.insert(testResult);
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    public void saveAllSummaryRealtime(String reportId, Integer resourceIndex, List<String> reportKeys) {
        CountDownLatch countDownLatch = new CountDownLatch(reportKeys.size());
        for (String key : reportKeys) {
            threadPoolExecutor.execute(() -> {
                try {
                    saveSummaryRealtime(reportId, key, resourceIndex);
                } catch (Exception e) {
                    LogUtil.error("reportId: " + reportId + ", key:" + key, e);
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
            saveAllSummary(reportId, reportKeys);
        } catch (Exception e) {
            LogUtil.error(e);
        }
    }

    public void saveSummaryRealtime(String reportId, String reportKey, Integer resourceIndex) {
        try {
            Object summary = SummaryRealtimeFactory.getSummaryExecutor(reportKey).execute(reportId, resourceIndex);
//            LoadTestReportResultPart record = new LoadTestReportResultPart();
//            record.setReportId(reportId);
//            record.setReportKey(reportKey);
//            record.setResourceIndex(resourceIndex);
//            record.setReportValue(objectMapper.writeValueAsString(summary));
//            saveResultPart(record);
        } catch (Exception e) {
            LogUtil.error("保存 [" + reportId + "], [" + reportKey + "] 报错了", e);
        }
    }
}
