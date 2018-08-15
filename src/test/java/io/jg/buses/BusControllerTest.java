package io.jg.buses;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Datastore;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StreamUtils;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class BusControllerTest {

    @Autowired
    private BusController busController;

    @Autowired
    private Datastore datastore;

    @Test
    public void testPublish() {
        busController.publishEvents();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getBuses(String fileName) throws Exception {
        String contents = StreamUtils.copyToString(new ClassPathResource(fileName).getInputStream(), Charset.defaultCharset());
        return (List<Map<String, Object>>) new ObjectMapper().readValue(contents, List.class);

    }

    @Test
    public void testGetLatest() {
        Set<Object[]> buses = busController.getLatest();
        assertNotNull(buses);
    }

    @Test
    public void testGet() {
        List<Map<String, Object>> buses = busController.latest().getBody();
        assertNotNull(buses);
        assertTrue(buses.size() > 0);
    }

    @Test
    public void testDateComparison() throws Exception {
        //datastore to string format
        Timestamp dsts = Timestamp.parseTimestamp("2012-11-02T13:40:04Z");

        //format coming from the buses
        String bt = "2012-11-02 13:40:04.0";

        Date gd = dsts.toDate();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date d = sdf.parse(bt);
        assertEquals(0, d.compareTo(gd));
    }
}
