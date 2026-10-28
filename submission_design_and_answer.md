# Clockit Senior Software Engineer Assessment
**Candidate Submission: Real-time Event-to-Transaction Streaming Infrastructure**

---

## Part 1: System Design

### 1. High-Level Architecture Diagram

```
+-----------------------------------------------------------------------------------+
|                                 BACKEND CLOUD                                     |
|                                                                                   |
|  +---------------------+      +---------------------+      +-------------------+  |
|  | Event Ingestion Bus | ---> | Realtime WSS Gateway| <--- |  Transaction API  |  |
|  | (Kafka / RabbitMQ)  |      | (Stateful / Netty)  |      |   (Spring/Idemp)  |  |
|  +---------------------+      +----------▲----------+      +---------▲---------+  |
+------------------------------------------|---------------------------|------------+
                                     TLS / WSS                       HTTPS / TLS
                               (Adaptive Heartbeat)             (Outbox Relay Path)
                                           |                           |
+------------------------------------------|---------------------------|------------+
|                          ANDROID PHONE COMPANION RELAY                            |
|                                          |                           |            |
|  +---------------------------------------▼---------------------------▼---------+  |
|  |                        AdaptiveConnectionManager                           |  |
|  |   - Exponential Moving Average (EMA) NAT Learner                           |  |
|  |   - Quality-Aware Reconnect & Full-Jitter Backoff                          |  |
|  |   - Selective Foreground Service Controller (WakeLock Safeguard)           |  |
|  +---------------------------------------▲-------------------------------------+  |
|                                          |                                        |
|  +---------------------------------------▼-------------------------------------+  |
|  |                       Persistent Outbox Engine                              |  |
|  |   - SQLite / Room Storage (State: QUEUED -> IN_FLIGHT -> ACKED)             |  |
|  |   - Idempotent UUIDv7 generation per user action                            |  |
|  +---------------------------------------▲-------------------------------------+  |
|                                          |                                        |
|  +---------------------------------------▼-------------------------------------+  |
|  |                          BLE Central Manager                                |  |
|  |   - GATT Client (High MTU 247 bytes, Write with Response)                   |  |
|  |   - Android CompanionDeviceManager Whitelist                                |  |
|  +---------------------------------------▲-------------------------------------+  |
+------------------------------------------|----------------------------------------+
                                    BLE 5.x GATT
                              (Encrypted L2CAP Channel)
                                           |
+------------------------------------------|----------------------------------------+
|                                SMARTWATCH                                         |
|                                                                                   |
|  +---------------------------------------▼-------------------------------------+  |
|  |                       BLE Peripheral (GATT Server)                          |  |
|  +---------------------------------------▲-------------------------------------+  |
|  |                   UI / Haptic Notification Presenter                        |  |
|  +---------------------------------------▲-------------------------------------+  |
|  |           Double-Tap Intent Capture & Hardware Debounce Engine              |  |
|  +-----------------------------------------------------------------------------+  |
+-----------------------------------------------------------------------------------+
```

---

### 2. Detailed Design of Phone Connection Manager

#### A. Detecting & Learning NAT Idle Timeout
* **The Silent Drop Reality:** Cellular providers (MTN, Airtel, Safaricom) recycle NAT state tables aggressively (30s to 120s) without emitting TCP `RST` or `FIN` flags. To the phone OS, the socket file descriptor appears `ESTABLISHED`, but packets drop silently into a carrier black hole.
* **Probing & EMA Baseline:** The client tracks elapsed time since the last byte read on the socket. When an application-level ping goes unacknowledged past a tight pong deadline (e.g., 5 seconds), a silent drop is registered:
  $$\text{Timeout}_{\text{new}} = \alpha \cdot \text{Sample}_{\text{observed}} + (1 - \alpha) \cdot \text{Timeout}_{\text{current}}$$
  where $\alpha = 0.2$ gives resilience against temporary radio glitches while adapting downward within 2–3 failure cycles.

#### B. Heartbeat Strategy
* **Safety Margin & Jitter:** Heartbeats are scheduled at **75% of the learned timeout**:
  $$\text{Interval} = (\text{LearnedTimeout} \times 0.75) \pm \text{Jitter}(5\%)$$
* **Traffic Piggybacking:** If normal business events or acks traverse the wire, the heartbeat timer resets to zero, preventing redundant packet overhead.
* **Jitter:** Prevents synchronized thundering herds across cell towers when thousands of devices experience identical carrier drop times.

#### C. Reconnection Logic (Resume & Replay)
* **Exponential Backoff with Full Jitter:** Base delay of $1\text{s} \times 2^{\min(\text{retries}, 5)}$ combined with randomized jitter.
* **Session Resumption (`RESUME` Frame):** On reconnect, the phone emits a handshake containing `last_received_server_seq_id`. The backend streams missed events from an ephemeral Redis buffer, eliminating gaps without requiring a full re-authentication handshake.

#### D. Selective Foreground Service & Companion Device APIs
* Running an ongoing Foreground Service drains battery and triggers user uninstalls.
* **Selective Promotion:** The app normally runs as an idle background client. It temporarily promotes itself to an Android Foreground Service (with an ongoing notification) under two conditions:
  1. High-frequency market event windows (e.g., active trading session or imminent flash sale).
  2. Immediately upon receiving a BLE trigger from the watch, keeping the CPU awake via a short partial `WakeLock` (max 10s) until the Outbox dispatches the intent.
* **CompanionDeviceManager API:** Leverages Android's `CompanionDeviceManager` pairing to exempt the companion relay app from standard Doze restrictions without violating Google Play policies.

#### E. Interaction with the BLE Watch Link
* Operates as a BLE GATT Central with a negotiated ATT MTU of 247 bytes to avoid transport-layer fragmentation.
* Uses **Write with Response** for transaction intents to guarantee delivery from the watch before the watch UI transitions to a pending state.

---

### 3. End-to-End Flow Scenario

#### Scenario: Backend emits "Flash Sale: 12 units left" -> Watch Double-Tap -> Completed Transaction

```
BACKEND                    PHONE RELAY                   SMARTWATCH
   |                            |                             |
   |--- WSS: Event (Seq #42) -->|                             |
   |    [Payload + TTL: 30s]    |                             |
   |                            |--- BLE GATT Push ---------->|
   |                            |    (Wake display & vibrate) |
   |                            |                             | [User double-taps]
   |                            |<-- BLE Intent --------------|
   |                            |    (Action + UUIDv7)        |
   |                            |                             |
   |                      [Persist to Outbox]                 |
   |                      [Elevate to FG Service]             |
   |                            |                             |
   |       XX NETWORK DEAD XX   |                             |
   |                            |                             |
   |                      [Detect drop via Pong]              |
   |                      [Reconnect with Backoff]            |
   |                            |                             |
   |<- WSS Connect + Resume ----|                             |
   |<-- Drain Outbox: Intent ---|                             |
   |    (Idempotency Key: UUID) |                             |
   |                            |                             |
   |--- WSS: 200 OK (Confirmed)-|                             |
   |                            |--- BLE Haptic Confirmation->|
   |                            |    ("Order Placed!")        |
   |                      [Demote FG Service]                 |
```

#### Handling Edge Cases Mid-Flow:
* **Phone Process Killed by OEM Mid-Flow:** 
  The intent is committed to persistent storage (SQLite/Room) before network dispatch. Android's `WorkManager` (expedited job) or a broadcast receiver triggered by the BLE reconnect event revives the process and drains the pending Outbox entry.
* **Network Drops While Double-Tap is In-Flight:** 
  The transaction payload carries an immutable client-generated `UUIDv7` (idempotency key). When reconnected, the phone replays the intent. The backend checks Redis/SQL for this idempotency key; if already processed, it simply returns the cached success response rather than double-charging the user.

---

### 4. Top 5 African Mobile Failure Modes & Mitigations

| # | Failure Mode | Technical Mechanism | Mitigation Strategy |
|---|---|---|---|
| **1** | **OEM Background Freezing (Transsion HiOS/XOS, Xiaomi MIUI)** | Custom vendor battery engines (e.g., Transsion "Power Master") force-kill background processes regardless of standard Android battery whitelists. | Bind the app using Android's `CompanionDeviceManager` API (`REQUEST_COMPANION_RUN_IN_BACKGROUND`), request vendor auto-start permissions, and elevate selectively to an active Foreground Service during transaction windows. |
| **2** | **Aggressive Carrier NAT Expiration** | Telcos (MTN, Airtel, Safaricom) clear idle TCP table translations within 30s–60s without notifying endpoints. | Application-level ping/pong probes that dynamically calculate NAT timeouts using an Exponential Moving Average (EMA) and ping at a 75% safety margin + jitter. |
| **3** | **Radio Access Technology (RAT) Thrashing** | Frequent handoffs between 4G, 3G, and 2G/EDGE cause high packet loss, latency spikes (>3000ms), and silent TCP hangs. | Monitor network capabilities via Android `ConnectivityManager.NetworkCallback`. Pause outbound pings during link degradation and apply TCP socket timeouts (SO_TIMEOUT) of 5s to force-fail hung sockets. |
| **4** | **BLE Disconnect During Intent Dispatch** | User moves arm or walks away from phone, dropping BLE signal right after double-tapping. | The smartwatch maintains an onboard ring buffer of unacknowledged intents. If the BLE ACK is not received from the phone within 500ms, the watch retries upon link re-establishment. |
| **5** | **Radio Resource Control (RRC) Sleep Latency** | Mobile modems enter low-power radio states (PCH/IDLE) after 5–10s of inactivity; waking the radio adds a 2–3s round-trip delay. | Group heartbeats with outgoing telemetry; keep traffic profile lightweight to avoid keeping the radio in high-power state continuously, while using small payload frames (CBOR or Protobuf) to fit inside initial radio bursts. |

---

## Part 3: Debugging & Edge Cases

### 1. Root Causes for Production Logs
* **Heartbeats succeed 2–3 mins, then connection silently dies:**
  * **Cause:** Carrier NAT timeout is between 120 and 180 seconds. While the device was actively pinging or receiving traffic initially, an idle stretch allowed the carrier firewall/NAT table to evict the mapping. The local TCP stack never received a `FIN`/`RST`, keeping the socket in an orphaned `ESTABLISHED` state.
* **Events delayed 4–15 minutes after process death:**
  * **Cause:** The app was frozen/killed by the OS. Real-time WebSocket connection died with the process. The fallback notification used standard priority FCM/GCM, which Android bundles into standard OEM Doze maintenance windows (which trigger every 5 to 15 minutes).
* **Double-tap intents take >8 seconds to reach backend:**
  * **Cause:** Compounded latency: BLE GATT channel contention/re-connection (1–2s) + Cellular modem transition from RRC Idle to RRC Connected state (2s) + Fresh TCP/TLS handshake setup over high-latency 2G/3G networks (3–4s).
* **Transsion devices kill foreground service after 1 minute:**
  * **Cause:** Transsion's proprietary "Power Master" terminates foreground services if it detects socket polling with a wake lock or sustained background CPU usage, treating it as an abusive background process unless exempted in the device's hidden vendor settings.

### 2. Diagnostic Data to Add to Client Telemetry
* `carrier_mcc_mnc`: Mobile Country Code / Network Code to map specific telco NAT characteristics.
* `network_generation`: Active Radio Type (`EDGE`, `HSPAP`, `LTE`, `NR`).
* `nat_silence_duration_ms`: Duration of network inactivity preceding a drop.
* `ble_rssi_at_tap`: Bluetooth signal strength at the moment the intent was captured.
* `hop_latencies`:
  * $T_0$: Watch tap hardware interrupt
  * $T_1$: BLE arrival at phone
  * $T_2$: Storage in phone Outbox
  * $T_3$: WebSocket dispatch
  * $T_4$: Server acknowledgment received

### 3. Concrete Changes to Connection Manager & Watch Relay
1. **Heartbeat Ceiling:** Hard-cap maximum heartbeat interval to 45 seconds on detected mobile data networks, regardless of learned timeout.
2. **High-Priority FCM Payload:** Switch backend pushes to high-priority FCM data messages (`"priority": "high"`), which grant an immediate 10-second high-power execution slice to wake the companion relay app.
3. **BLE Connection Priority Boost:** When a critical event is pushed to the watch, instruct the Android BLE stack to switch to `CONNECTION_PRIORITY_HIGH` (reduces BLE connection interval from ~100ms down to 11.25–15ms).
4. **Pre-Warming the Socket:** When a critical alert is relayed to the watch, immediately initiate an upstream HTTP/WSS pre-warm packet to force the cellular radio out of RRC sleep before the user even finishes double-tapping.

---

## Part 4: Trade-offs & Senior Judgement

### 1. Persistent Foreground Service vs. High-Priority FCM + Push
* **Use Persistent Foreground Service when:** The user is in an active, time-bounded session requiring guaranteed sub-second latency (e.g., active trading window, an alert countdown timer, or live GPS session). 
* **Use High-Priority FCM + Watch Push when:** The app is in steady-state monitoring over hours or days. Permanent foreground notifications annoy users, invite uninstalls, and trigger vendor task killers. FCM data messages respect device health while offering acceptable wake-up paths for spontaneous alerts.

### 2. Balancing Battery Impact vs. Delivery Reliability
* **Adaptive Heartbeats over Fixed Polling:** Adjust ping cadence to carrier profiles rather than fixed low intervals.
* **Binary Serialization:** Replace JSON with Protocol Buffers or CBOR to minimize byte serialization time and modem transmission windows.
* **Batching Non-Critical Acks:** Group read receipts and metrics into outgoing heartbeat frames, waking the radio chip only when necessary.

### 3. Should the Smartwatch Maintain Its Own Upstream Connection?
* **No, in this environment it should not.**
* **Reasons:**
  1. **Hardware & Cost:** Cellular-enabled smartwatches (eSIM/LTE-M) have high battery drain and low penetration in African markets due to carrier eSIM limitations and hardware costs.
  2. **Battery Constraints:** A typical smartwatch battery (200–400 mAh) will deplete in 2–4 hours under active cellular network searching and TLS handshakes.
  3. **Reliability:** BLE 5.x to the phone offers micro-watt power consumption and offloads all heavy crypto, network negotiation, and retries to the phone's larger battery and superior antenna array.

### 4. Scaling to 500k Concurrent Users (2G/3G/4G + Aggressive OEMs)
* **Distributed Dynamic Connection Registry:** Segment clients at edge gateways (e.g., AWS ECS / Envoy / custom Go gateways) partitioned by geographic carrier ASN.
* **Server-Assisted NAT Hints:** The backend measures connection drops across millions of connections and pushes dynamic baseline hints to clients (e.g., "MTN Nigeria currently requires 35s pings; Safaricom Kenya supports 70s").
* **Strict Idempotency Layer:** Use distributed Redis clusters with sliding-window bloom filters and TTL-based idempotency keys to absorb duplicate transaction replays during flapping network conditions.
* **Message Compaction & Backpressure:** Drop obsolete price ticks on the server gateway if client TCP buffers become full, only transmitting the latest delta sequence.