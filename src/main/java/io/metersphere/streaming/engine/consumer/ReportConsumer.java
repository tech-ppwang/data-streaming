package io.metersphere.streaming.engine.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.metersphere.streaming.base.domain.LoadTestReportResultPart;
import io.metersphere.streaming.commons.utils.LogUtil;
import io.metersphere.streaming.model.ReportResult;
import io.metersphere.streaming.service.TestResultSaveService;
import io.metersphere.streaming.service.TestResultService;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.*;

@Service
public class ReportConsumer {
    public static final String CONSUME_ID = "report-data";
    @Resource
    private ObjectMapper objectMapper;
    @Resource
    private TestResultSaveService testResultSaveService;
    @Resource
    private TestResultService testResultService;

    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());

    private final ThreadPoolExecutor summaryExecutor = new ThreadPoolExecutor(30, 30,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>());

    private final ConcurrentHashMap<String, Boolean> reportRunning = new ConcurrentHashMap<>();

    @KafkaListener(id = CONSUME_ID, topics = "${kafka.report.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<?, String> record) throws Exception {
        List<ReportResult> content = objectMapper.readValue(record.value(), new TypeReference<List<ReportResult>>() {
        });
        if (CollectionUtils.isEmpty(content)) {
            return;
        }
        ReportResult reportResult = content.get(0);
        String reportId = reportResult.getReportId();
        int resourceIndex = reportResult.getResourceIndex();
        if (BooleanUtils.toBoolean(reportResult.getCompleted())) {
            testResultService.completeReport(reportId);
            // 最后汇总所有的信息
            Runnable task = getTask(content, reportId, resourceIndex);
            executor.submit(task);
            return;
        }
        String key = reportId + "_" + resourceIndex;
        if (reportRunning.getOrDefault(key, false)) {
            // 正在处理
            LogUtil.info("别的线程处理报告: reportId_resourceIndex: {}", key);
            return;
        }
        LogUtil.info("处理报告: reportId_resourceIndex: {}", key);
        Runnable task = getTask(content, reportId, resourceIndex);
        reportRunning.put(key, true);
        executor.submit(task);
    }

    private Runnable getTask(List<ReportResult> content, String reportId, int resourceIndex) {
        return () -> {

            String key = reportId + "_" + resourceIndex;
            boolean b = testResultSaveService.checkReportStatus(reportId);
            if (!b) {
                // 报告不存在
                reportRunning.remove(key);
                return;
            }

            long start = System.currentTimeMillis();
            List<String> reportKeys = new CopyOnWriteArrayList<>();
            CountDownLatch countDownLatch = new CountDownLatch(content.size());
            content.forEach(result -> summaryExecutor.submit(() -> {
                String reportKey = result.getReportKey();
                try {
                    long summaryStart = System.currentTimeMillis();
                    reportKeys.add(reportKey);

                    LoadTestReportResultPart testResult = new LoadTestReportResultPart();
                    testResult.setReportId(result.getReportId());
                    testResult.setReportKey(reportKey);
                    testResult.setResourceIndex(result.getResourceIndex());
                    testResult.setReportValue(objectMapper.writeValueAsString(result.getContent()));
                    testResultSaveService.saveResultPart(testResult);
                    LogUtil.debug("报告: " + reportId + ", 保存" + reportKey + "耗时: " + (System.currentTimeMillis() - summaryStart));
                } catch (Exception e) {
                    LogUtil.error("接收结果处理异常: " + reportId + "reportKey: " + reportKey, e);
                } finally {
                    countDownLatch.countDown();
                }
            }));
            try {
                countDownLatch.await();
                long summaryStart = System.currentTimeMillis();
                LogUtil.debug("报告: " + reportId + ", 保存耗时: " + (summaryStart - start));
                // 汇总信息
                testResultSaveService.saveAllSummary(reportId, reportKeys);
                // 处理完成重置
                reportRunning.remove(key);
                LogUtil.debug("报告: " + reportId + ", 汇总耗时: " + (System.currentTimeMillis() - summaryStart));
            } catch (InterruptedException e) {
                LogUtil.error(e);
            }
        };
    }
}
