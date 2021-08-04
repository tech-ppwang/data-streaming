package io.metersphere.streaming.report.summary;

import com.fasterxml.jackson.core.type.TypeReference;
import io.metersphere.streaming.commons.constants.ReportKeys;
import io.metersphere.streaming.commons.utils.LogUtil;
import io.metersphere.streaming.report.base.Errors;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component("errorsSummary")
public class ErrorsSummary extends AbstractSummary<List<Errors>> {
    private final BigDecimal oneHundred = new BigDecimal(100);

    @Override
    public String getReportKey() {
        return ReportKeys.Errors.name();
    }

    @Override
    public List<Errors> execute(String reportId) {
        List<Errors> result = new ArrayList<>();
        SummaryAction action = (resultPart) -> {
            try {
                String reportValue = resultPart.getReportValue();
                List<Errors> reportContent = objectMapper.readValue(reportValue, new TypeReference<List<Errors>>() {
                });
                // 第一遍不需要汇总
                if (CollectionUtils.isEmpty(result)) {
                    result.addAll(reportContent);
                    return;
                }
                // 第二遍以后
                result.addAll(reportContent);

                BigDecimal errors = result.stream().map(e -> new BigDecimal(e.getErrorNumber())).reduce(BigDecimal::add).get();

                Map<String, List<Errors>> collect = result.stream().collect(Collectors.groupingBy(Errors::getErrorType));

                List<Errors> summaryDataList = collect.keySet().stream().map(k -> {

                    List<Errors> errorsList = collect.get(k);
                    BigDecimal percentOfAllSamples = errorsList.stream()
                            .map(e -> new BigDecimal(e.getPercentOfAllSamples()))
                            .reduce(BigDecimal::add)
                            .get();


                    Errors c = new Errors();
                    BigDecimal eSum = errorsList.stream().map(e -> new BigDecimal(e.getErrorNumber())).reduce(BigDecimal::add).get();
                    c.setErrorType(k);
                    c.setErrorNumber(eSum.toString());
                    c.setPercentOfErrors(format.format(eSum.divide(errors, 4, BigDecimal.ROUND_HALF_UP).multiply(oneHundred)));
                    // 这个值有误差
                    c.setPercentOfAllSamples(percentOfAllSamples.toString());
                    return c;
                }).collect(Collectors.toList());
                // 清空
                result.clear();
                // 保留前几次的结果
                result.addAll(summaryDataList);
                // 返回
            } catch (Exception e) {
                LogUtil.error("ErrorsSummary: ", e);
            }
        };
        int count = selectPartAndDoSummary(reportId, getReportKey(), action);
        result.forEach(e -> {
            // 这个值有误差
            e.setPercentOfAllSamples(format.format(new BigDecimal(e.getPercentOfAllSamples()).divide(new BigDecimal(count), 4, BigDecimal.ROUND_HALF_UP)));
        });
        return result;
    }
}
