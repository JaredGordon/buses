package io.jg.buses;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
public class BusControllerTest {

    @Autowired
    private BusController busController;

    @Test
    public void testGetBuses() {
        Set<Map<String, Object>> s = busController.getBuses().getBody();
        assertNotNull(s);
        assertTrue(s.size() > 0);
    }
}
