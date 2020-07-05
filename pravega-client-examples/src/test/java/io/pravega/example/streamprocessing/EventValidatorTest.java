package io.pravega.example.streamprocessing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.pravega.test.common.AssertExtensions;

import java.util.Iterator;

public class EventValidatorTest {
    static final Logger log = LoggerFactory.getLogger(EventValidatorTest.class);

    @Test
    public void basicTest() throws Exception {
        TestEventGenerator generator = new TestEventGenerator(6);
        final Iterator<TestEvent> generated = ImmutableList.copyOf(Iterators.limit(generator, 100)).iterator();
        TestEventValidator validator = new TestEventValidator(generator);
        validator.validate(generated);
    }

    @Test
    public void missingEventTest() throws Exception {
        TestEventGenerator generator = new TestEventGenerator(6);
        final Iterator<TestEvent> generated = ImmutableList.copyOf(Iterators.limit(generator, 10)).iterator();
        TestEventValidator validator = new TestEventValidator(generator);
        AssertExtensions.assertThrows(Exception.class, () -> validator.validate(Iterators.limit(generated, 9)));
    }
}
