package il.org.osm.israelhiking;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** Test-only executors. */
final class TestExecutors {

  private TestExecutors() {}

  /** Same-thread executor: submitted tasks run inline, so counts are deterministic after submit(). */
  static ExecutorService directExecutor() {
    return new AbstractExecutorService() {
      @Override
      public void execute(Runnable command) {
        command.run();
      }

      @Override
      public void shutdown() {}

      @Override
      public List<Runnable> shutdownNow() {
        return new ArrayList<>();
      }

      @Override
      public boolean isShutdown() {
        return true;
      }

      @Override
      public boolean isTerminated() {
        return true;
      }

      @Override
      public boolean awaitTermination(long timeout, TimeUnit unit) {
        return true;
      }
    };
  }
}
