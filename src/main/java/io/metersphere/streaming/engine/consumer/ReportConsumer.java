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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
public class ReportConsumer {
    public static final String CONSUME_ID = "report-data";
    @Resource
    private ObjectMapper objectMapper;
    @Resource
    private TestResultSaveService testResultSaveService;
    @Resource
    private TestResultService testResultService;

    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    @KafkaListener(id = CONSUME_ID, topics = "${kafka.report.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<?, String> record) throws Exception {
        List<ReportResult> content = objectMapper.readValue(record.value(), new TypeReference<List<ReportResult>>() {
        });
        if (CollectionUtils.isEmpty(content)) {
            return;
        }
        ReportResult reportResult = content.get(0);
        LogUtil.info("处理报告: reportId:{}", reportResult.getReportId());
        if (BooleanUtils.toBoolean(reportResult.getCompleted())) {
            testResultService.completeReport(reportResult.getReportId());
            return;
        }
        Runnable task = () -> content.forEach(result -> {
            try {
                LoadTestReportResultPart testResult = new LoadTestReportResultPart();
                testResult.setReportId(result.getReportId());
                testResult.setReportKey(result.getReportKey());
                testResult.setResourceIndex(result.getResourceIndex());
                testResult.setReportValue(objectMapper.writeValueAsString(result.getContent()));
                testResultSaveService.saveResultPart(testResult);
                // 汇总信息
                testResultSaveService.saveSummary(testResult.getReportId(), testResult.getReportKey());
            } catch (Exception e) {
                LogUtil.error("接收结果处理异常: ", e);
            }
        });
        executorService.submit(task);
    }
}
