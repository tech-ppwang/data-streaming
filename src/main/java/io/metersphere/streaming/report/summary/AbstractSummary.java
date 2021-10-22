package io.metersphere.streaming.report.summary;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.metersphere.streaming.base.domain.LoadTestReportResultPart;
import io.metersphere.streaming.commons.utils.CommonBeanFactory;
import io.metersphere.streaming.commons.utils.LogUtil;
import io.metersphere.streaming.report.base.ChartsData;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.batch.MyBatisCursorItemReader;
import org.mybatis.spring.batch.builder.MyBatisCursorItemReaderBuilder;
import org.springframework.batch.item.ExecutionContext;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractSummary<T> implements Summary<T> {
    protected DecimalFormat format = new DecimalFormat("0.000");

    @Resource
    protected ObjectMapper objectMapper;

    protected SummaryAction getSumAction(List<ChartsData> result) {
        return (resultPart) -> {
            try {
                String reportValue = resultPart.getReportValue();
                List<ChartsData> reportContent = objectMapper.readValue(reportValue, new TypeReference<List<ChartsData>>() {
                });
                // 第一遍不需要汇总
                if (CollectionUtils.isEmpty(result)) {
                    result.addAll(reportContent);
                    return;
                }
                // 第二遍以后
                result.addAll(reportContent);

                Map<Tuple, List<ChartsData>> collect = result.stream().collect(Collectors.groupingBy(data -> new Tuple(data.getxAxis(), data.getGroupName())));
                List<ChartsData> summaryDataList = collect.keySet().stream().map(k -> {
                    ChartsData c = new ChartsData();
                    BigDecimal y1Sum = collect.get(k).stream().map(ChartsData::getyAxis).reduce(new BigDecimal(0), BigDecimal::add);
                    BigDecimal y2Sum = collect.get(k).stream().map(ChartsData::getyAxis2).reduce(new BigDecimal(0), BigDecimal::add);
                    c.setxAxis(k.getxAxis());
                    if (y1Sum.compareTo(BigDecimal.ZERO) < 0) {
                        y1Sum = new BigDecimal(-1);
                    }
                    if (y2Sum.compareTo(BigDecimal.ZERO) < 0) {
                        y2Sum = new BigDecimal(-1);
                    }
                    c.setyAxis(y1Sum);
                    c.setyAxis2(y2Sum);
                    c.setGroupName(k.getGroupName());
                    return c;
                }).collect(Collectors.toList());
                // 清空
                result.clear();
                // 保留前几次的结果
                result.addAll(summaryDataList);
                // 返回
            } catch (Exception e) {
                LogUtil.error("getSumAction: ", e);
            }
        };
    }

    protected int selectPartAndDoSummary(String reportId, String reportKey, SummaryAction action) {
        SqlSessionFactory sqlSessionFactory = CommonBeanFactory.getBean(SqlSessionFactory.class);
        MyBatisCursorItemReader<LoadTestReportResultPart> myBatisCursorItemReader = new MyBatisCursorItemReaderBuilder<LoadTestReportResultPart>()
                .sqlSessionFactory(sqlSessionFactory)
                // 设置queryId
                .queryId("io.metersphere.streaming.base.mapper.ext.ExtLoadTestReportMapper.fetchTestReportParts")
                .build();
        int count = 0;
        try {
            Map<String, Object> param = new HashMap<>();
            param.put("reportId", reportId);
            param.put("reportKey", reportKey);
            myBatisCursorItemReader.setParameterValues(param);
            myBatisCursorItemReader.open(new ExecutionContext());
            LoadTestReportResultPart resultPart;
            while ((resultPart = myBatisCursorItemReader.read()) != null) {
                action.execute(resultPart);
                count++;
            }
        } catch (Exception e) {
            LogUtil.error("查询分布结果失败: ", e);
        } finally {
            myBatisCursorItemReader.close();
        }
        return count;
    }

    protected void handleAvgChartData(List<ChartsData> result, int count) {
        result.forEach(d -> {
            if (d.getyAxis().compareTo(new BigDecimal(0)) > 0) {
                d.setyAxis(d.getyAxis().divide(new BigDecimal(count), 4, BigDecimal.ROUND_HALF_UP));
            }

            if (d.getyAxis2().compareTo(new BigDecimal(0)) > 0) {
                d.setyAxis2(d.getyAxis2().divide(new BigDecimal(count), 4, BigDecimal.ROUND_HALF_UP));
            }
        });
    }

    protected List<ChartsData> handleAvgAction(String reportId) {
        List<ChartsData> result = new ArrayList<>();
        SummaryAction summaryAction = getSumAction(result);
        int count = selectPartAndDoSummary(reportId, getReportKey(), summaryAction);
        handleAvgChartData(result, count);
        return result;
    }

    protected List<ChartsData> handleSumAction(String reportId) {
        List<ChartsData> result = new ArrayList<>();
        SummaryAction summaryAction = getSumAction(result);
        selectPartAndDoSummary(reportId, getReportKey(), summaryAction);
        return result;
    }

    @Data
    @AllArgsConstructor
    public static class Tuple {
        String xAxis;
        String groupName;

        public String getxAxis() {
            return xAxis;
        }

        public void setxAxis(String xAxis) {
            this.xAxis = xAxis;
        }
    }
}
