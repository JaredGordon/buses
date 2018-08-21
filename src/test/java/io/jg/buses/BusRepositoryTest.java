package io.jg.buses;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
@ActiveProfiles("test")
public class BusRepositoryTest {

    @Autowired
    private BusRepository busRepository;

    @Autowired
    private String appToken;

    @Test
    public void testGetBuses() {
        List<Map<String, Object>> s = busRepository.getBuses(appToken);
        assertNotNull(s);
        assertTrue(s.size() > 0);
    }
}
