import client.ChatClient;
import client.ConnectionManager;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import model.ClientMessage;
import model.LatencyReport;
import util.BatchMessageGenerator;
import util.CSVWriter;
import util.Metrics;
import util.MetricsApiClient;
import util.MetricsPrintUtil;
import util.PhaseExecutor;

/**
 * Load test for message-distribution.
 * Structure mirrors my-websocket-project LoadTestPart2 with minimum changes:
 *   - Server URL points to the Producer WebSocket endpoint
 *   - ChatClient.onMessage() parses BroadcastMessage (not ResponseMessage)
 *   - Everything else (phases, queues, latches, CSV, metrics) is identical
 *
 * Usage: java -jar test-client.jar [mainPhaseThreads] [serverUrl]
 * Default: 32 threads, ws://localhost:8080/chat/
 */
public class LoadTest {

  public static final ConcurrentHashMap<String, LatencyReport> pendingMessages = new ConcurrentHashMap<>();
  public static final BlockingQueue<LatencyReport> resultsQueue = new LinkedBlockingQueue<>();
  public static final BlockingQueue<ClientMessage> messagesQueue = new LinkedBlockingQueue<>();
  public static final int WARMUP_COUNT = 32_000;
  public static final int WARMUP_THREADS = 32;
  public static final int TOTAL_COUNT = 500_000;
//  public static final int TOTAL_COUNT = 1_000_000;
  public static final int NUM_OF_CHAT_ROOMS = 20;
  public static final int DEFAULT_MAIN_PHASE_THREAD = 32;
  public static final String DEFAULT_SERVER_URI = "ws://producer:8080/chat/";
  public static final String DEFAULT_METRICS_URL = "http://consumer:8081/metrics";
  public static final ConnectionManager connectionManager = ConnectionManager.getInstance();

  public static void main(String[] args) throws Exception {
    int mainPhaseThreads = (args.length > 0) ? Integer.parseInt(args[0]) : DEFAULT_MAIN_PHASE_THREAD;
    String serverUri = (args.length > 1) ? args[1] : DEFAULT_SERVER_URI;
    String metricsUrl = (args.length > 2) ? args[2] : DEFAULT_METRICS_URL;

    System.out.println("===========================================");
    System.out.println("Starting v3 Load Test");
    System.out.println("Server:  " + serverUri);
    System.out.println("Warmup:  " + WARMUP_COUNT + " messages, " + WARMUP_THREADS + " threads");
    System.out.println("Main:    " + (TOTAL_COUNT - WARMUP_COUNT) + " messages, " + mainPhaseThreads + " threads");
    System.out.println("===========================================\n");

//    This wsConnectedLatch make sure the main thread wait till all websocket connections are open
    CountDownLatch wsConnectedLatch = new CountDownLatch(NUM_OF_CHAT_ROOMS);
//    This responseLatch ensures the main thread waits until all sent messages receive responses from the server, or a timeout occurs, whichever comes first.
    CountDownLatch warmupResponseLatch = new CountDownLatch(WARMUP_COUNT);

    // Connect one WebSocket per room
    connectionManager.setServerBaseUri(serverUri);
    connectionManager.setupConnectionPool(wsConnectedLatch, warmupResponseLatch,
        resultsQueue, pendingMessages, NUM_OF_CHAT_ROOMS);

    // Background threads: CSV writer + message generator
    ExecutorService backgroundExecutor = Executors.newFixedThreadPool(2);
    String outputDir = "results/v3";
    String fileName  = "v3_metrics.csv";
    Future<?> csvFuture = backgroundExecutor.submit(new CSVWriter(resultsQueue, outputDir, fileName));
    Future<?> msgGenFuture = backgroundExecutor.submit(new BatchMessageGenerator(messagesQueue, TOTAL_COUNT));

    msgGenFuture.get();
    System.out.println("Message generation complete\n");
    System.out.println("Queue size: " + messagesQueue.size());

//    Create Phase Executor to run both phases
    PhaseExecutor phaseExecutor = new PhaseExecutor();

//    Timer should start after all message generated
    long overallStartTime = System.currentTimeMillis();

    // ========================= WARMUP PHASE =========================
    System.out.println("\n>>> Phase 1: Warmup >>>");
    System.out.println("Sending " + WARMUP_COUNT + " messages with " + WARMUP_THREADS + " threads...");
    //    Log warmup start time
    long warmupStartTime = System.currentTimeMillis();
    //    Run Warmup Phase
    phaseExecutor.executePhase(WARMUP_THREADS, WARMUP_COUNT, messagesQueue);

    boolean warmupFinished = warmupResponseLatch.await(30, TimeUnit.SECONDS);
    if (!warmupFinished) {
      System.out.println("Warning: Warmup timed out before all responses received.");
    }
    long warmupEndTime = System.currentTimeMillis();
    long warmupTotalTime = warmupEndTime - warmupStartTime;
    int initialFailedMessages = (int) warmupResponseLatch.getCount();
    int initialSuccessMessages = WARMUP_COUNT - initialFailedMessages;

    // ========================= MAIN PHASE SETUP =========================
    int mainMessageCount = TOTAL_COUNT - WARMUP_COUNT;
    CountDownLatch mainResponseLatch = new CountDownLatch(mainMessageCount);
    pendingMessages.clear();
    for (ChatClient client : connectionManager.getConnectionPool().values()) {
      client.setResponseLatch(mainResponseLatch);
    }

    // ========================= MAIN PHASE =========================
    System.out.println("\n>>> Main Phase >>>");
    System.out.println("Sending " + mainMessageCount + " messages with " + mainPhaseThreads + " threads...");
    //    Log main phase start time
    long mainStartTime = System.currentTimeMillis();
    //    Run Main Phase
    phaseExecutor.executePhase(mainPhaseThreads, mainMessageCount, messagesQueue);

    boolean mainFinished = mainResponseLatch.await(600, TimeUnit.SECONDS);
    if (!mainFinished) {
      System.out.println("Warning: Main phase timed out before all responses received.");
    }
//    Calculation for main phase
    long mainEndTime = System.currentTimeMillis();
    long mainTotalTime = mainEndTime - mainStartTime;
    int mainFailedMessages = (int) mainResponseLatch.getCount();
    int mainSuccessMessages = mainMessageCount - mainFailedMessages;

    // ========================= CLEANUP =========================
    resultsQueue.put(LatencyReport.POISON_PILL);
    csvFuture.get(30, TimeUnit.SECONDS);
    backgroundExecutor.shutdown();
    connectionManager.shutdownAll();

    long overallTime = System.currentTimeMillis() - overallStartTime;

    // ========================= PRINT METRICS =========================
    System.out.println("\n=====Load test completed=====");
    MetricsPrintUtil.printPhaseMetrics("Warmup Phase",   WARMUP_COUNT,      initialSuccessMessages, initialFailedMessages, warmupTotalTime, WARMUP_THREADS);
    MetricsPrintUtil.printPhaseMetrics("Main Phase",     mainMessageCount,   mainSuccessMessages,   mainFailedMessages,   mainTotalTime,   mainPhaseThreads);
    MetricsPrintUtil.printPhaseMetrics("Overall",        TOTAL_COUNT,        initialSuccessMessages + mainSuccessMessages, initialFailedMessages + mainFailedMessages, overallTime, mainPhaseThreads);
    System.out.println("Total Connections:   " + Metrics.connections);
    System.out.println("Total Reconnections: " + Metrics.reconnections);
    System.out.println("Generating detailed statistical analysis...");
    String statsPath = outputDir + "/" + fileName;
    StatisticsGenerator.main(new String[]{statsPath});

    // ========================= METRICS API =========================
    // Wait for DB writer to finish flushing remaining buffer before querying metrics
    System.out.println("\nWaiting 30s for DB writer to finish flushing...");
    Thread.sleep(30_000);
    new MetricsApiClient(metricsUrl).fetchAndLog();
  }
}
