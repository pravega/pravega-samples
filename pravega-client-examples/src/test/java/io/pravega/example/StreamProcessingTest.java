package io.pravega.example;

import io.pravega.utils.SetupUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public class StreamProcessingTest {
    static final Logger log = LoggerFactory.getLogger(StreamProcessingTest.class);

    protected static final AtomicReference<SetupUtils> SETUP_UTILS = new AtomicReference<>();

    @BeforeClass
    public static void setup() throws Exception {
        SETUP_UTILS.set(new SetupUtils());
        if( SETUP_UTILS.get() == null) throw new RuntimeException("This is null");
        SETUP_UTILS.get().startAllServices();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        SETUP_UTILS.get().stopAllServices();
    }

    @Test
    public void basicWriterTest() throws IOException {
        String methodName = (new Object() {}).getClass().getEnclosingMethod().getName();
        log.info("Test case: {}", methodName);
    }
}
