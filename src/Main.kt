import java.util.UUID
import kotlin.random.Random

// 1. Transaction intent model originating from the smartwatch or phone
data class TransactionIntent(
    val id: String = UUID.randomUUID().toString(),
    val action: String,
    val payload: String = "{}",
    val timestamp: Long = System.currentTimeMillis()
)

// Real-world network profiles representing African telco conditions (2G/3G/4G)
enum class NetworkQuality(val baseLatencyMs: Long) {
    EXCELLENT_4G(baseLatencyMs = 60),
    DEGRADED_3G(baseLatencyMs = 250),
    HOSTILE_2G(baseLatencyMs = 800)
}

// 2. The core Adaptive Connection Manager
class AdaptiveConnectionManager(
    private var learnedTimeoutMs: Long = 60_000L, // Initial baseline (60s)
    private val safetyMargin: Double = 0.75,      // Keep-alive sent at 75% of learned threshold
    private val alpha: Double = 0.2               // Exponential Moving Average smoothing factor
) {
    var isConnected: Boolean = false
        private set

    var consecutiveFailures: Int = 0
        private set

    var isProtected: Boolean = false
        private set

    var currentNetwork: NetworkQuality = NetworkQuality.EXCELLENT_4G
    private var lastBleActivityTimestamp: Long = System.currentTimeMillis()

    // Persistent in-memory Outbox for store-and-forward reliability
    private val outbox = mutableListOf<TransactionIntent>()

    // Event listener callback for logging and telemetry
    var onEvent: ((String) -> Unit)? = null

    fun connect() {
        isConnected = true
        consecutiveFailures = 0
        onEvent?.invoke("Connected to backend streaming gateway [Network: $currentNetwork].")
        flushOutbox()
    }

    fun disconnect() {
        isConnected = false
        onEvent?.invoke("Connection dropped/closed.")
    }

    // Calculates adaptive heartbeat interval: (learnedTimeout * safetyMargin) + randomized jitter
    fun computeNextHeartbeatInterval(): Long {
        val baseInterval = (learnedTimeoutMs * safetyMargin).toLong()
        // Randomize +/- 5% to prevent synchronized thundering herds on carrier towers
        val jitterPercent = Random.nextInt(-5, 6) / 100.0
        val jitter = (jitterPercent * baseInterval).toLong()
        return baseInterval + jitter
    }

    // Exponential Moving Average (EMA) update when a silent carrier NAT drop is observed
    fun recordSilentDrop(observedIdleTimeMs: Long) {
        consecutiveFailures++
        val oldTimeout = learnedTimeoutMs
        learnedTimeoutMs = ((alpha * observedIdleTimeMs) + ((1.0 - alpha) * oldTimeout)).toLong()

        onEvent?.invoke("Silent NAT drop after ${observedIdleTimeMs}ms! Updated learned baseline: ${oldTimeout}ms -> ${learnedTimeoutMs}ms")
        disconnect()
        reconnectWithBackoff()
    }

    fun recordHeartbeatSuccess(rttMs: Long) {
        consecutiveFailures = 0
        onEvent?.invoke("Heartbeat ACK received. RTT: ${rttMs}ms")
    }

    // Exponential backoff with jitter
    private fun reconnectWithBackoff() {
        val exponent = if (consecutiveFailures > 5) 5 else consecutiveFailures
        val backoffBase = 1000L * (1L shl exponent) // 1s, 2s, 4s, 8s, 16s, 32s max
        val jitter = Random.nextLong(100, 500)
        val delay = backoffBase + jitter
        onEvent?.invoke("Scheduling reconnect in ${delay}ms (Consecutive failures: $consecutiveFailures)")

        // Reconnect simulation
        connect()
    }

    // Simulates elevating to Android Foreground Service to avoid OEM background freezes
    fun protect() {
        isProtected = true
        onEvent?.invoke("Foreground Service Started: Priority elevated (OEM freeze risk reduced).")
    }

    fun unprotect() {
        isProtected = false
        onEvent?.invoke("Foreground Service Stopped: App in standard background state.")
    }

    // BLE Companion Hook: Watch activity resets idle decay and temporarily protects the process
    fun onBleActivityReceived(signal: String) {
        lastBleActivityTimestamp = System.currentTimeMillis()
        onEvent?.invoke("BLE Signal from Smartwatch: '$signal'. Resetting keep-alive decay.")
        if (!isProtected) {
            protect()
        }
    }

    // Store intent in outbox and attempt immediate dispatch
    fun queueIntent(intent: TransactionIntent) {
        outbox.add(intent)
        onEvent?.invoke("Transaction queued in outbox: ${intent.action} (ID: ${intent.id.take(8)})")
        flushOutbox()
    }

    private fun flushOutbox() {
        if (!isConnected) {
            onEvent?.invoke("Outbox cannot flush: Network offline. Queued items pending: ${outbox.size}")
            return
        }

        while (outbox.isNotEmpty()) {
            val item = outbox.removeAt(0)
            onEvent?.invoke("TRANSACTION DISPATCHED -> ${item.action} [Payload: ${item.payload}, ID: ${item.id.take(8)}]")
        }
    }

    fun printMetrics() {
        println("================ METRICS ================")
        println("Learned NAT Timeout   : ${learnedTimeoutMs}ms")
        println("Next Keep-Alive Ping  : ${computeNextHeartbeatInterval()}ms")
        println("Consecutive Failures  : $consecutiveFailures")
        println("Foreground Protected  : $isProtected")
        println("Active Network Profile: $currentNetwork")
        println("Outbox Items Pending  : ${outbox.size}")
        println("=========================================\n")
    }
}

// 3. Complete End-to-End Test Harness
fun main() {
    println("=== CLOCKIT ADAPTIVE CONNECTION DEMO ===\n")

    val manager = AdaptiveConnectionManager()
    manager.onEvent = { log -> println("[RUNTIME] $log") }

    // Phase 1: Establish baseline on 4G
    manager.connect()
    manager.printMetrics()
    manager.recordHeartbeatSuccess(rttMs = 85)

    // Phase 2: Carrier silently drops after 32 seconds (NAT table flush)
    println("\n--- SIMULATION: Carrier NAT Table Drops at 32s ---")
    manager.recordSilentDrop(observedIdleTimeMs = 32_000L)
    manager.printMetrics()

    // Phase 3: Watch interaction arrives while network degrades to 2G
    println("\n--- SIMULATION: Network drops to Flaky 2G & Watch Double-Tap ---")
    manager.currentNetwork = NetworkQuality.HOSTILE_2G
    manager.disconnect()

    // Smartwatch sends BLE trigger
    manager.onBleActivityReceived("DOUBLE_TAP_INTENT")

    // User triggers transaction while offline
    val buyOrder = TransactionIntent(
        action = "EXECUTE_STOP_LOSS",
        payload = "{\"asset\":\"BTC-USD\",\"amount\":0.5}"
    )
    manager.queueIntent(buyOrder)
    manager.printMetrics()

    // Phase 4: Network recovers -> Outbox executes automatically
    println("\n--- SIMULATION: Network Connection Recovers ---")
    manager.connect()
    manager.printMetrics()

    manager.unprotect()
}