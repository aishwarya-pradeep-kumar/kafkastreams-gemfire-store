package kafkastreams.gemfire.store.cache;

import kafkastreams.gemfire.store.configuration.ServerProcess;
import org.apache.geode.cache.Cache;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.gemfire.tests.integration.ClientServerIntegrationTestsSupport;
import org.springframework.data.gemfire.tests.process.ProcessWrapper;
import org.springframework.data.gemfire.tests.util.FileSystemUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


@RunWith(SpringRunner.class)
@ContextConfiguration
public class GemfireKeyValueStoreTest extends ClientServerIntegrationTestsSupport {

    private static final String GEMFIRE_LOG_LEVEL = "debug";
    private static ProcessWrapper gemfireServer;
    private InternalMockProcessorContext context;
    private GemfireKeyValueStore gemfireKeyValueStore;
    private File dir;

    @Before
    public void setup() throws IOException {
        //Gemfire setup
        int availablePort = 40404;
        String serverName = GemfireKeyValueStoreTest.class.getSimpleName().concat("Server");
        File serverWorkingDirectory = new File(FileSystemUtils.WORKING_DIRECTORY, serverName.toLowerCase());
        List<String> arguments = new ArrayList<>();
        arguments.add(String.format("-Dgemfire.name=%s", serverName));
        arguments.add(String.format("-Dgemfire.log-level=%s", GEMFIRE_LOG_LEVEL));
        arguments.add(String.format("-Dspring.data.ak.gemfire.gemfire.cache.server.port=%d", availablePort));
        arguments.add(GemfireKeyValueStoreTest.class.getName()
                .replace(".", "/").concat("-server-context.xml"));
        gemfireServer = run(serverWorkingDirectory, ServerProcess.class,
                arguments.toArray(new String[arguments.size()]));
        waitForServerToStart(DEFAULT_HOSTNAME, availablePort);
        configureGemFireClient(availablePort);
        //Kafka setup
        gemfireKeyValueStore = new GemfireKeyValueStore("test-store",gemfireCache, Serdes.String(),Serdes.String());
        dir = TestUtils.tempDirectory();
        final Properties props = StreamsTestUtils.getStreamsConfig();
        context = new InternalMockProcessorContext(dir,
                Serdes.String(),
                Serdes.String(),
                new StreamsConfig(props));
    }

    private static void configureGemFireClient(int availablePort) {
        System.setProperty("ak.gemfire.gemfire.log-level", "error");
        System.setProperty("spring.data.ak.gemfire.gemfire.cache.server.port", String.valueOf(availablePort));
    }

    @After
    public void tearDown() {
        //closing gemfire store
        gemfireKeyValueStore.close();
        //Clearing gemfire cache
        stop(gemfireServer);
        System.clearProperty("ak.gemfire.gemfire.log-level");
        System.clearProperty("spring.data.ak.gemfire.gemfire.cache.server.port");
        if (Boolean.valueOf(System.getProperty("spring.ak.gemfire.gemfire.fork.clean", Boolean.TRUE.toString()))) {
            org.springframework.util.FileSystemUtils.deleteRecursively(gemfireServer.getWorkingDirectory());
        }

    }

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private Cache gemfireCache;


    @Test
    public void putTest() {
        gemfireKeyValueStore.init(context,gemfireKeyValueStore);
        gemfireKeyValueStore.put("key1","value1");
    }

    @Test
    public void putIfAbsent() {
    }

    @Test
    public void putAll() {
    }

    @Test
    public void delete() {
    }

    @Test
    public void name() {
    }

    @Test
    public void init() {
    }

    @Test
    public void flush() {
    }

    @Test
    public void close() {
    }

    @Test
    public void persistent() {
    }

    @Test
    public void isOpen() {
    }

    @Test
    public void get() {
    }

    @Test
    public void range() {
    }

    @Test
    public void all() {
    }

    @Test
    public void approximateNumEntries() {
    }
}