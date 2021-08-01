package io.metersphere.streaming.engine.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.metersphere.streaming.base.domain.LoadTestReportResultPart;
import io.metersphere.streaming.commons.utils.LogUtil;
import io.metersphere.streaming.model.ReportResult;
import io.metersphere.streaming.service.TestResultSaveService;
import io.metersphere.streaming.service.TestResultService;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

@Service
public class ReportConsumer {
    public static final String CONSUME_ID = "report-data";
    @Resource
    private ObjectMapper objectMapper;
    @Resource
    private TestResultSaveService testResultSaveService;
    @Resource
    private TestResultService testResultService;
    private boolean isRunning = true;

    @KafkaListener(id = CONSUME_ID, topics = "${kafka.report.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(ConsumerRecord<?, String> record) throws Exception {
        ReportResult reportResult = objectMapper.readValue(record.value(), ReportResult.class);
        LogUtil.debug("报告: {}, reportKey:{}", reportResult.getReportId(), reportResult.getReportKey());
        if (BooleanUtils.toBoolean(reportResult.getCompleted())) {
            testResultService.completeReport(reportResult.getReportId());
            return;
        }
        try {

            LoadTestReportResultPart testResult = new LoadTestReportResultPart();
            testResult.setReportId(reportResult.getReportId());
            testResult.setReportKey(reportResult.getReportKey());
            testResult.setResourceIndex(reportResult.getResourceIndex());
            testResult.setReportValue(objectMapper.writeValueAsString(reportResult.getContent()));
            testResultSaveService.saveResultPart(testResult);
            // 汇总信息
            testResultSaveService.saveSummary(testResult.getReportId(), testResult.getReportKey());
        } catch (Exception e) {
            LogUtil.error("接收结果处理异常: ", e);
        }

    }
}
