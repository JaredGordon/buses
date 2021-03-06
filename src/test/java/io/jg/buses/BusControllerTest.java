package io.jg.buses;

import com.google.api.client.util.DateTime;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.KeyFactory;
import com.google.common.io.LineReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
@ActiveProfiles("test")
public class BusControllerTest {

    @Autowired
    private BusController busController;

    @Autowired
    private Datastore datastore;

    @Test
    @Ignore
    public void testPublish() {
        busController.publishEvents();
    }

    @Test
    public void testGetLatest() {
        Set<String[]> buses = busController.getLatest();
        assertNotNull(buses);
    }

    @Test
    public void testTraffic() {
        String html = busController.traffic();
        assertNotNull(html);
        log.info(html);
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
    public void deleteStuff() {
        List<Map<String, Object>> buses = busController.latest();
        KeyFactory keyFactory = datastore.newKeyFactory();
        keyFactory.setKind("segment");
        for (Map<String, Object> bus : buses) {
            datastore.delete(keyFactory.newKey(bus.get("segmentid").toString()));
        }
    }

    @Test
    public void testDeDupe() {
        List<Map<String, Object>> busData = busController.getBusData();
        assertNotNull(busData);
        int size = busData.size();
        assertTrue(size > 0);
        Map<String, Object> busMap = busController.dedupe(busData);
        assertNotNull(busMap);
        int mapSize = busMap.size();
        assertTrue(size > mapSize);
    }

    @Test
    public void testIsRecent() {
        assertFalse(busController.isRecent(null));
        assertFalse(busController.isRecent("foo"));
        assertTrue(busController.isRecent(Timestamp.now().toString()));
        long oneHourAgo = Timestamp.now().getSeconds() - 60 * 60;
        Timestamp timestamp2 = Timestamp.ofTimeSecondsAndNanos(oneHourAgo, 0);
        assertTrue(busController.isRecent(timestamp2.toString()));
        long oneDayAgo = Timestamp.now().getSeconds() - 60 * 60 * 24;
        Timestamp timestamp3 = Timestamp.ofTimeSecondsAndNanos(oneDayAgo, 0);
        assertFalse(busController.isRecent(timestamp3.toString()));
    }

    @Test
    public void testSameDate() {
        Timestamp now = Timestamp.now();
        assertTrue(busController.sameDate(now.toString(), now.toString()));
        long seconds = now.getSeconds();
        Date d = new Date(seconds * 1000);
        Timestamp timestamp1 = Timestamp.of(d);
        Timestamp timestamp2 = Timestamp.parseTimestamp(timestamp1.toString());
        assertTrue(busController.sameDate(timestamp1.toString(), timestamp2.toString()));
    }

    @Test
    public void testBuildBusURL() {
        String buses = busController.busLocations();
        assertNotNull(buses);
        log.info(buses);
    }

    @Test
    public void countLines() throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(new File("/Users/jgordon/Downloads/bus_hist.csv")));
        String s;
        int i = 0;
        while (true) {
            try {
                s = br.readLine();
                if(s == null) {
                    log.info("null found.");
                    break;
                }
                i++;
            } catch (IOException e) {
                break;
            }
            if (i % 100000 == 0) {
                log.info("100000 more lines....");
            }
        }
        log.info(i + " lines!");
    }

    @Test
    public void testRW() throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(new File("/Users/jgordon/Downloads/bus_hist.csv")));
        String line;

        File fout = new File("out.txt");
        FileOutputStream fos = new FileOutputStream(fout);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        int i = 0;
        while ((line = br.readLine()) != null) {
            if( i > 100) {
                break;
            }
            //skip the headings line
            if (i != 0) {
                StringTokenizer st = new StringTokenizer(line, ",");
                String d = st.nextToken();
                Date date = null;
                try {
                    date = sdf.parse(d);
                    Timestamp ts = Timestamp.of(date);
                    line = ts.toString() + line.substring(line.indexOf(','));
                } catch (ParseException e) {
                    log.error("invalid date: " + date);
                }
            }
            i++;

            bw.write(line);
            bw.newLine();

        }
        bw.flush();
        bw.close();

        log.info(i + " lines written!");
    }
}
