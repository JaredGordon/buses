package io.jg.buses;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StreamUtils;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class BusControllerTest {

    @Autowired
    private BusController busController;

    @Autowired
    private BigQuery bigQuery;

    @Test
    public void testGetBuses() {
        List<Map<String, Object>> s = busController.getBuses().getBody();
        assertNotNull(s);
        assertTrue(s.size() > 0);
    }

    @Test
    public void testLoad() {
//        List<Map<String, Object>> buses = getBuses("buses.json");
        List<Map<String, Object>> buses = busController.getBuses().getBody();

        TableId tableId = TableId.of("buses", "buses");
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);

        for (Map<String, Object> segment : buses) {
            String rowId = segment.get("segmentid").toString() + ":" + segment.get("_last_updt");
            builder.addRow(rowId, segment);
        }

        InsertAllResponse response = bigQuery.insertAll(builder.build());
        if (response.hasErrors()) {
            for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                log.error(entry.toString());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getBuses(String fileName) throws Exception {
        String contents = StreamUtils.copyToString(new ClassPathResource(fileName).getInputStream(), Charset.defaultCharset());
        return (List<Map<String, Object>>) new ObjectMapper().readValue(contents, List.class);

    }
}
