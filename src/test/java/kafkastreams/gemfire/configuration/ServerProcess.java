package kafkastreams.gemfire.configuration;

import lombok.extern.log4j.Log4j2;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.gemfire.tests.process.ProcessUtils;
import org.springframework.data.gemfire.tests.util.FileSystemUtils;
import org.springframework.util.Assert;

import java.io.File;
import java.io.IOException;

@Log4j2
public class ServerProcess {

    public static void main(String[] args) throws Throwable {
        ConfigurableApplicationContext applicationContext = null;

        try {
            applicationContext = newApplicationContext(args);
            waitForShutdown();
        }
        catch (Throwable e) {
            log.debug("", e);
            throw e;
        }
        finally {
            close(applicationContext);
        }
    }

    private static ConfigurableApplicationContext newApplicationContext(String[] configLocations) {
        Assert.notEmpty(configLocations, String.format("Usage: >java -cp ... %1$s %2$s",
                ServerProcess.class.getName(), "classpath:/to/applicationContext.xml"));

        ConfigurableApplicationContext applicationContext = new ClassPathXmlApplicationContext(configLocations);

        applicationContext.registerShutdownHook();

        return applicationContext;
    }

    private static boolean close(ConfigurableApplicationContext applicationContext) {
        if (applicationContext != null) {
            applicationContext.close();
            return !(applicationContext.isRunning() || applicationContext.isActive());
        }

        return true;
    }

    private static void waitForShutdown() throws IOException {
        ProcessUtils.writePid(new File(FileSystemUtils.WORKING_DIRECTORY, getServerProcessControlFilename()),
                ProcessUtils.currentPid());

        ProcessUtils.waitForStopSignal();
    }

    public static String getServerProcessControlFilename() {
        return ServerProcess.class.getSimpleName().toLowerCase().concat(".pid");
    }
}
