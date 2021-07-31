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

                Map<String, List<Errors>> collect = result.stream().collect(Collectors.groupingBy(Errors::getErrorType));
                List<Errors> summaryDataList = collect.keySet().stream().map(k -> {

                    List<Errors> errorsList = collect.get(k);
                    BigDecimal samples = errorsList.stream()
                            .map(e -> {
                                BigDecimal divisor = new BigDecimal(e.getPercentOfAllSamples()).divide(oneHundred);
                                return new BigDecimal(e.getErrorNumber()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP);
                            })
                            .reduce(BigDecimal::add)
                            .get();

                    BigDecimal errors = errorsList.stream()
                            .map(e -> {
                                BigDecimal divisor = new BigDecimal(e.getPercentOfErrors()).divide(oneHundred);
                                return new BigDecimal(e.getErrorNumber()).divide(divisor, 4, BigDecimal.ROUND_HALF_UP);
                            })
                            .reduce(BigDecimal::add)
                            .get();


                    Errors c = new Errors();
                    BigDecimal eSum = collect.get(k).stream().map(e -> new BigDecimal(e.getErrorNumber())).reduce(new BigDecimal(0), BigDecimal::add);
                    c.setErrorType(k);
                    c.setErrorNumber(eSum.toString());
                    c.setPercentOfErrors(format.format(eSum.divide(errors, 4, BigDecimal.ROUND_HALF_UP).multiply(oneHundred)));
                    c.setPercentOfAllSamples(format.format(eSum.divide(samples, 4, BigDecimal.ROUND_HALF_UP).multiply(oneHundred)));
                    return c;
                }).collect(Collectors.toList());
                // 清空
                result.clear();
                // 保留前几次的结果
                result.addAll(summaryDataList);
                // 返回
            } catch (Exception e) {
                LogUtil.error(e);
            }
        };
        selectPartAndDoSummary(reportId, getReportKey(), action);
        return result;
    }
}
