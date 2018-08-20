package io.jg.buses;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.KeyFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

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

    @Test
    public void testFixTimezone() {
        String date = "2018-08-20 13:40:04.0";
        assertEquals("2018-08-20T18:40:04Z", busController.fixTimezone(date));
    }

    @Test
    @Ignore
    public void trafficMap() {
        //get latest
        List<Map<String, Object>> latest = busController.latest().getBody();
        List<Map<String, Object>> traffic = new ArrayList<>();

        //throw out traffic < 0
        for (Map<String, Object> bus : latest) {
            if ((Integer) bus.get("_traffic") > 0) {
                traffic.add(bus);
            }
        }

        //sort by _last_update


        //pick top 25 most recent

        //build out a query string
    }

    @Test
    @Ignore
    public void deleteStuff() {
        List<Map<String, Object>> buses = busController.latest().getBody();
        KeyFactory keyFactory = datastore.newKeyFactory();
        keyFactory.setKind("segment");
        for (Map<String, Object> bus : buses) {
            datastore.delete(keyFactory.newKey(bus.get("segmentid").toString()));
        }
    }
}
