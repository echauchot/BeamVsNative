import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.example.flink.pipelines.SimpleCombinePerKey;
import org.junit.Rule;
import org.junit.Test;

public class SimpleCombinePerKeyTest {

  private static String INPUT_LINES = "19790101\tAFR\tFRA\t043\t1\t4\t1\t2.8\t\t\t\n"
    + "19790101\tAFR\tFRA\t050\t2\t9\t1\t3.5\t\t\t\t\t\t\t\t\t\n"
    + "19790101\tAFX\tFRAGOV\t043\t2\t19\t1\t2.8\t1\t46\t2\t1\t46\t2\t1\t46\t2\n";

  @Rule
  public  MiniClusterWithClientResource flinkCluster =
    new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setNumberSlotsPerTaskManager(2)
        .setNumberTaskManagers(1)
        .build());

  @Test
  public void testIncrementPipeline() throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    final SimpleCombinePerKey simpleCombinePerKey = new SimpleCombinePerKey();
    final DataSource<String> inputDatasource = env.fromElements(INPUT_LINES.split("\n"));
    final DataSet<Tuple2<String, Long>> apply = simpleCombinePerKey.apply(inputDatasource);
    final List<Tuple2<String, Long>> resultList = apply.collect();
    List<Tuple2<String, Long>> expectedList = new ArrayList<Tuple2<String, Long>>();
    expectedList.add(Tuple2.of("AFR", 2L));
    expectedList.add(Tuple2.of("AFX", 1L));
    assertTrue(resultList.containsAll(expectedList));
  }
}
