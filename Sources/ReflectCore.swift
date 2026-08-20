import UIKit
import AdSupport

#if canImport(AppTrackingTransparency)
import AppTrackingTransparency
#endif

#if canImport(StoreKit)
import StoreKit
#endif

#if canImport(AdServices)
import AdServices
#endif

#if canImport(Network)
import Network
#endif

#if canImport(CoreTelephony)
import CoreTelephony
#endif

import CryptoKit
import Compression

/// Minimal FlutterStreamHandler that buffers one event until Dart subscribes —
/// used for the attribution channel (the plugin itself handles deep links).
public class ReflectCore: NSObject {

    private static let channelName = "com.reflect.sdk/channel"
    private static let deepLinkChannelName = "com.reflect.sdk/deep_links"
    // Bump in lockstep with pubspec.yaml. Wire form: "flutter-<version>".
    private static let sdkVersion = "flutter-1.7.2"
    // X-Reflect-Platform: the RUNTIME platform. Constant — this engine is the iOS
    // build, so every host on it (Unity, Flutter, RN, native) is "ios". Matches the
    // `platform IN ('android','ios')` vocabulary the schema uses everywhere else;
    // the web SDK sends its own "web".
    private static let platform = "ios"
    private static let sessionGapMs = 30 * 60 * 1000   // new session after 30 min in bg
    private static let subsessionFloorMs: Int64 = 1000 // sub-second fg flips aren't subsessions
    // Durable event queue (Adjust-style): persist before send, drain head-first,
    // delete only on 2xx/permanent-4xx, retry with backoff otherwise.
    private static let queueFileName = "reflect_queue.jsonl"
    private static let privacyTombstoneFileName = "reflect_privacy_suppression_v1.json"
    private static let maxQueue = 1000
    private static let batchSize = 50   // max events per HTTP request (Unity parity)
    // Hard ceiling the ingest endpoint enforces (server: workers/src/lib/validation.ts,
    // `LIMITS.eventsPerBatch`). A larger batch is rejected with a terminal 400
    // bad_batch_shape, which the send path maps to .drop — so an over-large configured
    // batch loses every event in it, then the next one, permanently and silently.
    // Clamp the caller's value instead of honouring it.
    private static let maxServerBatch = 200
    // The other half of the same contract: the ingest endpoint reads at most
    // `LIMITS.bodyBytes` WIRE bytes (post-gzip, since that is what we send) and 413s
    // anything larger — terminal, so the batch is .drop'd and lost. The event COUNT
    // ceiling above cannot prevent this on its own: 200 events with large props bags
    // can exceed it.
    //
    // Deliberately 1% under the server's 1,000,000 rather than equal to it: the body
    // we MEASURE and the body postBatch finally builds are not byte-identical
    // (sent_at_ms advances between them, droppedCount can move), so a batch measured
    // at exactly the ceiling could go out at ceiling+3 and 413 — the very outcome this
    // guard exists to prevent. The slack costs nothing; the events it displaces ride
    // the next batch.
    private static let maxWireBytes = 990_000
    private static let baseBackoffMs: Int64 = 1000
    private static let maxBackoffMs: Int64 = 3_600_000
    private static let maxAskInRepolls = 3   // A3: bound server-driven attribution re-polls per session
    // Raw install-referrer/AdServices values are memory-only and available just
    // long enough for the immediate online send.
    private static let transientAttributionTtlMs: Int64 = 30_000
    // A batch refused by iOS tracking-domain policy is waiting on the ATT
    // decision, not on the network. It still backs off — a host that never
    // presents the prompt would otherwise be polled forever — but against a
    // much lower ceiling than a server outage, because the wait is expected to
    // end in seconds and the retries are what catch an answer the transition
    // hook somehow misses. The decision itself (attTrackingDecisionResolved)
    // is the real wake-up. See AttTransportPolicy.
    private static let attBlockedMaxBackoffMs: Int64 = 300_000   // 5 min

    private enum SendResult { case success, retry, drop, cancelled, attBlocked }

    private var appKey = ""
    private var companyKey: String?
    private var baseUrl = "https://api.reflect.cloud"

    /// Automatic SKAdNetwork conversion-value state (revenue accumulator,
    /// schema cache, monotonic send gate). See SkanAutoConversion.swift.
    private let skanAuto = SkanAutoConversion()
    private var environment = "production"
    private var debug = false
    private var initialized = false
    private var installUuid = ""
    private var existingInstallUuid: String?   // legacy id to adopt on first migrated launch (Unity migration continuity)
    private var hostSdkVersion: String = ReflectCore.sdkVersion   // brand for sdk_version + X-Reflect-Sdk; host-supplied ("react-native-x"/"unity-x"), defaults to the Flutter const
    private var userId: String?
    private var pushToken: String?
    private var integrityToken: String?   // attestation — header on signed /event (Unity parity)
    private var externalDeviceId: String?
    // Configurable tuning knobs (Unity parity) — default to the constants.
    private var cfgBatchSize = ReflectCore.batchSize
    private var cfgMaxQueue = ReflectCore.maxQueue
    private var autoResolveDeferred = true
    private var autoSessionTracking = true   // false ⇒ host disables auto app_open/session tracking (Unity parity)
    private var lastCrashMs: Int64 = 0       // throttle: cap `_crash` events to 1/min (Unity parity)
    private var autoRegisterSkan = true
    private var autoRequestIosTracking = false   // true ⇒ auto-present ATT prompt at init (Unity parity)
    // Client-side dedup LRU (Unity parity) — insertion-order window of recently-seen
    // deduplication_ids; touched ONLY on the serial `queue`, so no lock needed.
    private var dedupMax = 10
    private var seenDedupIds = Set<String>()
    private var dedupOrder = [String]()
    private let eventStateLock = NSLock()
    private var consentState = "granted"
    private var requireConsent = false
    private var partnerSharing: [String: [String: Any]] = [:]
    private var thirdPartySharing: NSNumber?
    private var ffCoppa = false
    private var linkMeEnabled = false
    private var isForegroundState = true
    private var signingSecret: String?
    private var lastAttributionCheckMs: Int64 = 0
    private var requireAdConsentLatch = false
    // Session manager state.
    // Session manager (MONOTONIC clock via systemUptime — immune to wall-clock
    // jumps; all session state mutated on the serial `queue`).
    private var sessionStartElapsed: Int64 = 0   // monotonic ms of the current active stint; 0 = not timing
    private var lastBackgroundElapsed: Int64 = 0
    private var sessionCount: Int64 = 0
    private var sessionActiveMs: Int64 = 0        // accumulated active time this session (persisted)
    private var subsessionCount: Int64 = 0
    private var sessionOpen = false
    private var sessionId = ""                                  // per-session GUID, on EVERY event
    private var sessionThresholdMs = ReflectCore.sessionGapMs // configurable new-session gap
    private var heartbeatTimer: DispatchSourceTimer?
    // Serializes session persistence against privacy cleanup. A privacy block is
    // installed first; cleanup then waits for any winning mutation and erases it.
    private let sessionStateLock = NSRecursiveLock()
    private var trackingEnabled = true            // false after deleteUserData()/setEnabled(false)
    private var firstInstallMs: Int64 = 0
    // UIKit values snapshotted on the MAIN thread at init (UIKit accessors are
    // main-thread-only; buildDevice runs on a background queue).
    private var snapScreenW = 0
    private var snapScreenH = 0
    private var snapScreenDensityDpi = 0
    private var snapDeviceType = "phone"
    private var snapSystemVersion = ""
    private var snapIdfv: String?
    private var snapIdfa: String?
    private var cachedAttStatus: String?
    private var uikitSnapshotted = false
    private var userProperties: [String: Any]?
    private var advertisingConsent = true
    private weak var listener: ReflectListener?
    private var pendingDeferredDeepLink: Any?
    private var lastDeepLinkReported: String?
    private var lastDeepLink: String?   // GetLastDeeplink accessor (Unity parity)
    private var pendingAttribution: Any?
    private let queue = OperationQueue()
    private var globalProperties: [String: Any] = [:]
    private var partnerParameters: [String: String] = [:]   // forwarded to integration partners
    private var globalPropertySourceAtMs: [String: Int64] = [:]
    private var partnerParameterSourceAtMs: [String: Int64] = [:]

    // Free-form global/partner values remain useful within a running app, but
    // making them durable would let a suspended app retain arbitrary click
    // context on disk beyond 90 days. Source-age them in memory and purge every
    // historical UserDefaults representation.
    private func ensureParameterValuesAreEphemeral() {
        AttributionRetention.clearLegacyParameterPersistence()
    }

    private func restoreParams(
        nowMs: Int64 = Int64(Date().timeIntervalSince1970 * 1_000)
    ) {
        ensureParameterValuesAreEphemeral()
        // Preserve values set before initialize() and values held across a
        // same-process disable/enable, while enforcing their original clocks.
        pruneExpiredEphemeralParameters(nowMs: nowMs)
    }

    private func pruneExpiredEphemeralParameters(
        nowMs: Int64 = Int64(Date().timeIntervalSince1970 * 1_000)
    ) {
        var globalsChanged = false
        var partnersChanged = false
        globalLock.lock()
        for key in Array(globalProperties.keys) {
            guard let sourceAtMs = globalPropertySourceAtMs[key],
                  AttributionRetention.isSourceTimestampRetained(
                    sourceAtMs: sourceAtMs,
                    nowMs: nowMs
                  ) else {
                globalProperties.removeValue(forKey: key)
                globalPropertySourceAtMs.removeValue(forKey: key)
                globalsChanged = true
                continue
            }
        }
        for key in Array(partnerParameters.keys) {
            guard let sourceAtMs = partnerParameterSourceAtMs[key],
                  AttributionRetention.isSourceTimestampRetained(
                    sourceAtMs: sourceAtMs,
                    nowMs: nowMs
                  ) else {
                partnerParameters.removeValue(forKey: key)
                partnerParameterSourceAtMs.removeValue(forKey: key)
                partnersChanged = true
                continue
            }
        }
        globalLock.unlock()
        if globalsChanged || partnersChanged {
            ensureParameterValuesAreEphemeral()
        }
    }

    private func storedAttributionIfAllowed() -> String? {
        let defaults = UserDefaults.standard
        guard trackingEnabled,
              consentState != "denied",
              !defaults.bool(forKey: "reflect_suppressed") else { return nil }
        let stored = defaults.string(forKey: "reflect_attribution_json")
        let expiry = Int64(defaults.integer(forKey: "reflect_attr_click_context_expires_at_ms"))
        let scrubbed = AttributionRetention.scrubClickId(
            stored,
            expiresAtMs: expiry,
            failClosedWhenExpiryUnknown:
                defaults.object(forKey: "reflect_attr_click_context_expires_at_ms") == nil
        )
        let hasClickId = AttributionRetention.hasClickId(scrubbed)
        if scrubbed != stored ||
            (!hasClickId &&
             defaults.object(forKey: "reflect_attr_click_context_expires_at_ms") != nil) {
            if let scrubbed {
                defaults.set(scrubbed, forKey: "reflect_attribution_json")
            } else {
                defaults.removeObject(forKey: "reflect_attribution_json")
            }
            if !hasClickId {
                defaults.removeObject(forKey: "reflect_attr_click_context_expires_at_ms")
            }
        }
        return scrubbed
    }

    /// Apply identity/property collection only while the current privacy posture
    /// allows it. Once initialized, the mutation linearizes against block().
    @discardableResult
    private func mutateMeasurementState(_ action: () -> Void) ->
        (applied: Bool, permit: PrivacyTransportGate.Permit?) {
        guard trackingEnabled, consentState != "denied" else { return (false, nil) }
        let defaults = UserDefaults.standard
        guard !defaults.bool(forKey: "reflect_suppressed"),
              defaults.string(forKey: "reflect_consent_state") != "denied" else { return (false, nil) }
        if !initialized {
            action()
            return (true, nil)
        }
        guard let permit = transportGate.permit() else { return (false, nil) }
        var applied = false
        let valid = transportGate.runIfValid(permit) {
            guard trackingEnabled, consentState != "denied" else { return }
            action()
            applied = true
        }
        return (valid && applied, valid && applied ? permit : nil)
    }

    // X1 — durable attribution-critical signals: the deferred deep link + attribution
    // poll permanently lose their signal when offline in the first seconds after install.
    // Persist a "pending" marker cleared only on a real HTTP response; re-attempt on
    // connectivity-return + next launch until it lands.
    private func setPendingSignal(_ key: String, _ pending: Bool) {
        UserDefaults.standard.set(pending, forKey: key)
    }
    private func retryPendingSignals() {
        if UserDefaults.standard.bool(forKey: "reflect_pending_deferred_dl") {
            scheduleDeferredDeepLinkResolution()
        }
        if UserDefaults.standard.bool(forKey: "reflect_pending_attr") {
            scheduleAttributionCheck()
        }
    }
    private func scheduleDeferredDeepLinkResolution() {
        guard let permit = transportGate.permit() else { return }
        DispatchQueue.global(qos: .utility).async { [weak self] in
            self?.resolveDeferredDeepLink(permit: permit)
        }
    }
    private func scheduleAttributionCheck() {
        guard let permit = transportGate.permit() else { return }
        DispatchQueue.global(qos: .utility).async { [weak self] in
            self?.attributionCheck(permit: permit)
        }
    }

    // X2 — overwrite consent/sharing/ATT keys on a queued event with the CURRENT live
    // values just before send (Adjust updatePackagesTrackingI). An event frozen at
    // enqueue otherwise transmits a stale consent_state/third_party_sharing/att_status
    // (e.g. an install built before the ATT grant reports not_determined). Defensive:
    // on any parse hiccup, return the original (never drop over re-stamping).
    private func restampConsent(_ eventJson: String) -> String {
        guard let d = eventJson.data(using: .utf8),
              var o = (try? JSONSerialization.jsonObject(with: d)) as? [String: Any] else { return eventJson }
        o["consent_state"] = consentState
        o["third_party_sharing"] = thirdPartySharing?.boolValue ?? true
        if let att = attStatusString() { o["att_status"] = att }
        if let nd = try? JSONSerialization.data(withJSONObject: o),
           let ns = String(data: nd, encoding: .utf8) { return ns }
        return eventJson
    }
    private let globalLock = NSLock()

    // Durable event queue state.
    private var eventQueue: [String] = []
    private let queueLock = NSLock()
    private var transientAttributionPayloads:
        [String: (payload: String, expiresAtMs: Int64)] = [:]
    // Authoritative policy for every non-deletion request. It also owns
    // cancellation of concrete URLSessionTasks that won the start race.
    private let transportGate = PrivacyTransportGate()
    private lazy var pendingPrivacyDelete = PendingPrivacyDeleteStore(defaults: .standard)
    private lazy var privacyTombstone = PrivacySuppressionTombstone(url: privacyTombstoneFileURL())
    private var sending = false
    private var offlineMode = false   // setOfflineMode(true): keep tracking + queuing, but pause sending
    private var localOnly = false     // empty baseUrl ⇒ local DEBUG mode (Unity parity): collect, NEVER network
    private var flushIntervalMs: Int64 = 30_000   // periodic-flush backstop cadence (Unity FlushIntervalSeconds)
    private var flushTimer: DispatchSourceTimer?
    private let sendLock = NSLock()
    private var drainBackoffMs: Int64 = 0
    private var lastRetryAfterMs: Int64 = 0
    // Server-driven pacing (response directive `continue_in`): delay before the
    // NEXT batch after a success. Set in postBatch, consumed in drain.
    private var pendingContinueMs: Int64 = 0
    // Authoritative "don't send before" gate (monotonic ms). Both retry backoff and
    // continue_in pacing set it; drain() honors it before any send so a competing
    // scheduleDrain(0) can't bypass the pace/backoff. 0 = no gate; reset on reconnect.
    private var nextSendAllowedMs: Int64 = 0
    private var droppedCount: Int64 = 0   // cumulative queue-overflow drops (telemetry)
    private var headRetryCount = 0        // retries of the current head batch (telemetry)
    private var pathMonitorStarted = false

    public override init() {
        super.init()
        // Upgrade cleanup must not depend on initialize() being called or on a
        // permissive privacy posture.
        AttributionRetention.clearLegacyParameterPersistence()
        queue.maxConcurrentOperationCount = 1
        loadQueue()
    }

    /// Register the wrapper's listener for the deep-link + attribution streams.
    /// Events buffered before a listener attached are flushed now.
    public func setListener(_ l: ReflectListener?) {
        listener = l
        if let l = l {
            // Buffered values belong to the current privacy generation. A
            // deny/delete may win after async scheduling but before the main
            // queue runs, so delivery must validate the captured permit.
            let permit = transportGate.permit()
            if let value = pendingDeferredDeepLink {
                pendingDeferredDeepLink = nil
                if let permit = permit {
                    DispatchQueue.main.async { [weak self] in
                        guard let self = self, self.transportGate.isValid(permit),
                              self.listener === l else { return }
                        l.onDeepLink(value)
                    }
                }
            }
            if let value = pendingAttribution {
                pendingAttribution = nil
                if let permit = permit {
                    DispatchQueue.main.async { [weak self] in
                        guard let self = self, self.transportGate.isValid(permit),
                              self.listener === l else { return }
                        l.onAttribution(value)
                    }
                }
            }
        }
    }

    // MARK: - FlutterStreamHandler

    // MARK: - FlutterPlugin

    public func handle(method: String, args: [String: Any]?, result: @escaping ReflectResult) {
        switch method {
        case "initialize":
            handleInitialize(args: args, result: result)
        case "trackEvent":
            let name = args?["eventName"] as? String ?? ""
            let props = args?["properties"] as? String
            // Revenue/currency may ride an arbitrary event (Unity ReflectEventOptions
            // parity) → promote to the top-level envelope so it isn't lost.
            var top: [String: Any]? = nil
            if let rev = args?["revenue"] as? Double {
                top = ["revenue": rev]
                if let cur = args?["currency"] as? String { top?["currency"] = cur }
            }
            trackEventInternal(eventName: name, propertiesJson: props, referral: nil,
                               topLevel: top,
                               callbackId: args?["callbackId"] as? String,
                               callbackParamsJson: args?["callbackParams"] as? String,
                               partnerParamsJson: args?["partnerParams"] as? String,
                               deduplicationId: args?["deduplicationId"] as? String)
            result(nil)
        case "trackRevenue":
            handleTrackRevenue(args: args, result: result)
        case "trackPurchase":
            handleTrackPurchase(args: args, result: result)
        case "trackSubscription":
            handleTrackSubscription(args: args, result: result)
        case "trackAdRevenue":
            handleTrackAdRevenue(args: args, result: result)
        case "setUserId":
            let newId = args?["userId"] as? String
            var alias: [String: Any]?
            let mutation = mutateMeasurementState {
                if userId == nil, let nid = newId, !nid.isEmpty {
                    alias = ["user_id_new": nid, "previous_anonymous": installUuid]
                }
                userId = newId
            }
            if let alias = alias, let permit = mutation.permit {
                emitJsonEvent("_user_alias", alias, nil, acceptedPermit: permit)
            }
            result(nil)
        case "clearUserId":
            userId = nil
            result(nil)
        case "setUserProperties":
            mutateMeasurementState {
                if let json = args?["properties"] as? String,
                   let data = json.data(using: .utf8),
                   let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                    userProperties = dict
                }
            }
            result(nil)
        case "setGlobalProperty":
            mutateMeasurementState {
                if let key = args?["key"] as? String, let value = args?["value"] {
                    let nowMs = Int64(Date().timeIntervalSince1970 * 1_000)
                    let retained = AttributionRetention.sanitizeEphemeralParameter(
                        key: key,
                        value: value
                    )
                    globalLock.lock()
                    if let retained {
                        globalProperties[key] = retained
                        globalPropertySourceAtMs[key] = nowMs
                    } else {
                        globalProperties.removeValue(forKey: key)
                        globalPropertySourceAtMs.removeValue(forKey: key)
                    }
                    globalLock.unlock()
                    ensureParameterValuesAreEphemeral()
                }
            }
            result(nil)
        case "unsetGlobalProperty":
            if let key = args?["key"] as? String {
                globalLock.lock()
                globalProperties.removeValue(forKey: key)
                globalPropertySourceAtMs.removeValue(forKey: key)
                globalLock.unlock()
                ensureParameterValuesAreEphemeral()
            }
            result(nil)
        case "clearGlobalProperties":
            globalLock.lock()
            globalProperties.removeAll()
            globalPropertySourceAtMs.removeAll()
            globalLock.unlock()
            ensureParameterValuesAreEphemeral()
            result(nil)
        case "setConsent":
            let wasDenied = consentState == "denied"
            let granted = args?["granted"] as? Bool ?? true
            let defaults = UserDefaults.standard
            if granted {
                // A permissive transition requires initialized app/tenant state.
                // Never let a pre-init call erase a fail-closed tombstone based on
                // stale wrapper/native state.
                guard initialized else {
                    result(ReflectError(
                        code: "not_initialized",
                        message: "Consent cannot be granted before initialization.",
                        details: nil
                    ))
                    return
                }
                // Relaxation is two-phase: the ordinary store must commit first,
                // then the independent fail-closed tombstone may be removed. Any
                // failure restores denial before transport can reopen.
                defaults.set("granted", forKey: "reflect_consent_state")
                let defaultsPersisted = defaults.synchronize()
                let tombstoneCleared = defaultsPersisted
                    ? privacyTombstone.update(consentDenied: false)
                    : false
                guard defaultsPersisted && tombstoneCleared else {
                    defaults.set("denied", forKey: "reflect_consent_state")
                    _ = defaults.synchronize()
                    _ = privacyTombstone.update(consentDenied: true)
                    consentState = "denied"
                    advertisingConsent = false
                    transportGate.block()
                    clearEventQueue()
                    clearLocalIdentityForConsentDenial()
                    result(ReflectError(
                        code: "privacy_persistence_failed",
                        message: "Could not durably persist consent state.",
                        details: nil
                    ))
                    return
                }
                consentState = "granted"
                if ffCoppa { advertisingConsent = false }
                else if !requireAdConsentLatch { advertisingConsent = true }
                DispatchQueue.main.async { [weak self] in self?.refreshAttStatus(); self?.refreshIdfa() }
                if trackingEnabled && wasDenied { activateAfterPrivacyGate() }
                else { scheduleDrain(0) }
            } else {
                // Restriction takes effect in memory and destroys local data even
                // if one persistence backend fails. Either durable store is enough
                // to keep the next process fail-closed; callers receive an error
                // only when neither store could record the denial.
                consentState = "denied"
                advertisingConsent = false
                transportGate.block()
                let tombstonePersisted = privacyTombstone.update(consentDenied: true)
                defaults.set("denied", forKey: "reflect_consent_state")
                let defaultsPersisted = defaults.synchronize()
                clearEventQueue()
                clearLocalIdentityForConsentDenial()
                guard tombstonePersisted || defaultsPersisted else {
                    result(ReflectError(
                        code: "privacy_persistence_failed",
                        message: "Could not durably persist consent state.",
                        details: nil
                    ))
                    return
                }
            }
            result(nil)
        case "setExternalDeviceId":
            mutateMeasurementState { externalDeviceId = args?["externalDeviceId"] as? String }
            result(nil)
        case "setAudience":
            // Audience tags → _set_audience (Unity SetAudience); Flutter routes via trackEvent.
            if let tags = args?["tags"] as? [String] { emitJsonEvent("_set_audience", ["tags": tags], nil) }
            result(nil)
        case "setThirdPartySharing":
            if let enabled = args?["enabled"] as? Bool {
                UserDefaults.standard.set(enabled, forKey: "reflect_third_party_sharing")   // persist (Unity parity)
                guard UserDefaults.standard.synchronize() else {
                    if !enabled { thirdPartySharing = NSNumber(value: false) }
                    result(ReflectError(
                        code: "privacy_persistence_failed",
                        message: "Could not durably persist third-party-sharing state.",
                        details: nil
                    ))
                    return
                }
                thirdPartySharing = NSNumber(value: enabled)
                // Authoritative event (Unity parity) so the server records the change.
                emitJsonEvent("_third_party_sharing", ["enabled": enabled], nil)
            }
            result(nil)
        case "setAdvertisingConsent":
            let g = args?["granted"] as? Bool ?? true
            // COPPA / denied consent hard-block re-enabling ad tracking.
            let targetAdvertisingConsent: Bool
            if g && (ffCoppa || consentState == "denied") {
                log("setAdvertisingConsent(true) ignored — blocked by \(ffCoppa ? "COPPA" : "denied consent")")
                targetAdvertisingConsent = false
            } else { targetAdvertisingConsent = g }
            UserDefaults.standard.set(targetAdvertisingConsent, forKey: "reflect_ad_consent")
            guard UserDefaults.standard.synchronize() else {
                advertisingConsent = false
                result(ReflectError(
                    code: "privacy_persistence_failed",
                    message: "Could not durably persist advertising-consent state.",
                    details: nil
                ))
                return
            }
            advertisingConsent = targetAdvertisingConsent
            if advertisingConsent {
                DispatchQueue.main.async { [weak self] in self?.refreshAttStatus(); self?.refreshIdfa() }  // re-collect IDFA on grant (gap 5)
            }
            result(nil)
        case "setPartnerSharing":
            var sharingEvent: [String: Any]?
            let mutation = mutateMeasurementState {
                if let partner = args?["partner"] as? String, !partner.isEmpty,
                   let key = args?["key"] as? String, !key.isEmpty, let value = args?["value"] {
                    var m = partnerSharing[partner] ?? [:]
                    m[key] = value
                    partnerSharing[partner] = m
                    sharingEvent = ["partner_sharing": partnerSharing]
                }
            }
            if let sharingEvent = sharingEvent, let permit = mutation.permit {
                emitJsonEvent("_third_party_sharing", sharingEvent, nil, acceptedPermit: permit)
            }
            result(nil)
        case "registerPushToken":
            // The token rides the envelope (top-level push_token) on every
            // subsequent event; the server promotes it to a column.
            var pushEvent: [String: Any]?
            let mutation = mutateMeasurementState {
                pushToken = args?["token"] as? String
                // ALSO emit a _push_token event (Unity parity) so the server stores
                // the token immediately, not only when the next event carries it.
                if let tok = pushToken, !tok.isEmpty {
                    var props: [String: Any] = ["token": tok]
                    if let provider = args?["provider"] as? String, !provider.isEmpty { props["provider"] = provider }
                    pushEvent = props
                }
            }
            if let pushEvent = pushEvent, let permit = mutation.permit {
                emitJsonEvent("_push_token", pushEvent, nil, acceptedPermit: permit)
            }
            result(nil)
        case "setPushToken":
            // STICKY-ONLY (Unity parity): sets the envelope field, no _push_token event.
            mutateMeasurementState { pushToken = args?["token"] as? String }
            result(nil)
        case "setIntegrityToken":
            mutateMeasurementState { integrityToken = args?["token"] as? String }
            result(nil)
        case "verifyPurchase":
            let permit = transportGate.permit()
            DispatchQueue.global(qos: .utility).async { [weak self] in
                let r = self?.verifyPurchaseHttp(args, permit: permit) ?? ["status": "failed", "code": 0, "message": "request_failed"]
                DispatchQueue.main.async { result(r) }
            }
        case "requestIosTracking":
            handleRequestIosTracking(result: result)
        case "getDebugState":
            result(debugState())
        case "getInstallUuid":
            result(installUuid)
        case "getConsent":
            result(consentState)   // native source of truth (Unity GetConsent)
        case "getLastDeepLink":
            result(lastDeepLink)   // Unity GetLastDeeplink
        case "getAttribution":
            result(storedAttributionIfAllowed())
        case "getAttributionWithTimeout":
            // Force a FRESH /attribution/check; return as soon as it resolves or the
            // cached value after the timeout (Unity parity).
            let timeoutMs = (args?["timeoutMs"] as? Int) ?? 3000
            let responded = NSLock(); var done = false
            func reply(_ v: String?) {
                responded.lock(); let first = !done; done = true; responded.unlock()
                if first { DispatchQueue.main.async { result(v) } }
            }
            DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + .milliseconds(timeoutMs)) {
                reply(self.storedAttributionIfAllowed())
            }
            let permit = transportGate.permit()
            DispatchQueue.global(qos: .utility).async { [weak self] in
                if let permit = permit { self?.attributionCheck(forceFresh: true, permit: permit) }
                reply(self?.storedAttributionIfAllowed())
            }
        case "updateConversionValue":
            handleUpdateConversionValue(args: args, result: result)
        case "resolveDeepLink":
            let url = args?["url"] as? String ?? ""
            let permit = transportGate.permit()
            DispatchQueue.global(qos: .utility).async { [weak self] in
                let resolved = permit.flatMap { self?.resolveLink(url, permit: $0) }
                DispatchQueue.main.async {
                    result(permit.flatMap { self?.transportGate.isValid($0) == true ? resolved : nil } ?? nil)
                }
            }
        case "handleDeepLink":
            // App-driven deep-link injection (Unity HandleDeepLink): route a URL the
            // host captured itself through the core's deep-link path.
            if let s = args?["url"] as? String, let url = URL(string: s) { handleIncomingURL(url) }
            result(nil)
        case "getInitialDeepLink":
            handleGetInitialDeepLink(result: result)
        case "deleteUserData":
            handleDeleteUserData(result: result)
        case "setEnabled":
            if setTrackingEnabled(args?["enabled"] as? Bool ?? true) {
                result(nil)
            } else {
                result(ReflectError(
                    code: "privacy_persistence_failed",
                    message: "Could not durably persist tracking suppression.",
                    details: nil
                ))
            }
        case "isEnabled":
            result(trackingEnabled)
        case "setPartnerParameter":
            let key = args?["key"] as? String ?? ""
            mutateMeasurementState {
                if !key.isEmpty, let value = args?["value"] as? String {
                    let nowMs = Int64(Date().timeIntervalSince1970 * 1_000)
                    let retained = AttributionRetention.sanitizeEphemeralParameter(
                        key: key,
                        value: value
                    ) as? String
                    globalLock.lock()
                    if let retained {
                        partnerParameters[key] = retained
                        partnerParameterSourceAtMs[key] = nowMs
                    } else {
                        partnerParameters.removeValue(forKey: key)
                        partnerParameterSourceAtMs.removeValue(forKey: key)
                    }
                    globalLock.unlock()
                    ensureParameterValuesAreEphemeral()
                }
            }
            result(nil)
        case "unsetPartnerParameter":
            let key = args?["key"] as? String ?? ""
            globalLock.lock()
            partnerParameters.removeValue(forKey: key)
            partnerParameterSourceAtMs.removeValue(forKey: key)
            globalLock.unlock()
            ensureParameterValuesAreEphemeral()
            result(nil)
        case "clearPartnerParameters":
            globalLock.lock()
            partnerParameters.removeAll()
            partnerParameterSourceAtMs.removeAll()
            globalLock.unlock()
            ensureParameterValuesAreEphemeral()
            result(nil)
        case "flush":
            scheduleDrain(0)
            result(nil)
        case "setOfflineMode":
            offlineMode = args?["offline"] as? Bool ?? false
            if !offlineMode { scheduleDrain(0) }   // back online → flush soon
            result(nil)
        default:
            result(ReflectNotImplemented.instance)
        }
    }

    // MARK: - Method Handlers

    private func handleInitialize(args: [String: Any]?, result: ReflectResult) {
        if initialized { result(nil); return }

        appKey = args?["appKey"] as? String ?? ""
        companyKey = args?["companyKey"] as? String
        // Migration continuity (Unity): adopt a wrapper's legacy install_uuid rather
        // than minting a new one (a new id = a phantom reinstall for every upgrade).
        existingInstallUuid = (args?["existingInstallUuid"] as? String).flatMap { $0.isEmpty ? nil : $0 }
        // host-supplied SDK brand (RN/Unity); defaults to the Flutter const when absent.
        if let sv = args?["sdkVersion"] as? String, !sv.isEmpty { hostSdkVersion = sv }
        debug = args?["debug"] as? Bool ?? false
        if let env = args?["environment"] as? String, !env.isEmpty { environment = env }
        ffCoppa = args?["coppaCompliant"] as? Bool ?? false
        linkMeEnabled = args?["linkMeEnabled"] as? Bool ?? false
        if let t = args?["sessionThresholdSeconds"] as? Int, t > 0 { sessionThresholdMs = t * 1000 }
        // Tuning knobs (Unity parity) — omit → constant default preserved.
        if let b = args?["batchSize"] as? Int, b >= 1 { cfgBatchSize = min(b, ReflectCore.maxServerBatch) }
        if let q = args?["maxQueueSize"] as? Int, q > 0 { cfgMaxQueue = q }
        autoResolveDeferred = (args?["autoResolveDeferredDeepLink"] as? Bool) != false
        autoSessionTracking = (args?["autoSessionTracking"] as? Bool) != false
        if let f = args?["flushIntervalSeconds"] as? Int, f > 0 { flushIntervalMs = Int64(f) * 1000 }
        autoRegisterSkan = (args?["autoRegisterSkan"] as? Bool) != false
        autoRequestIosTracking = (args?["autoRequestIosTracking"] as? Bool) == true
        if let d = args?["eventDeduplicationIdsMaxSize"] as? Int, d >= 0 { dedupMax = d }
        signingSecret = args?["signingSecret"] as? String
        lastAttributionCheckMs = Int64(UserDefaults.standard.integer(forKey: "reflect_attr_watermark"))

        // Unity parity: an explicit EMPTY baseUrl ⇒ local DEBUG mode (collect locally,
        // NEVER hit the network — trial events never ship to prod). nil ⇒ keep default.
        if let url = args?["baseUrl"] as? String { baseUrl = url }
        localOnly = baseUrl.isEmpty

        let defaults = UserDefaults.standard
        if args?["requireAdvertisingConsent"] as? Bool == true {
            requireAdConsentLatch = true
        }
        requireConsent = args?["requireConsent"] as? Bool == true
        let durablePrivacy = privacyTombstone.read()
        // A denial in either store wins. Unity writes its mirror before an
        // opt-out dispatch and relaxes it only after native acknowledgement.
        let ic = args?["initialConsent"] as? String
        let storedConsent = defaults.string(forKey: "reflect_consent_state")
        consentState = InitialPrivacyPosture.resolveConsent(
            stored: storedConsent,
            initial: ic,
            requireConsent: requireConsent,
            durableConsentDenied: durablePrivacy.consentDenied
        )
        if ic == "denied", storedConsent != "denied" {
            defaults.set("denied", forKey: "reflect_consent_state")
        }
        func storedBool(_ key: String) -> Bool? {
            defaults.object(forKey: key) == nil ? nil : defaults.bool(forKey: key)
        }
        let storedSuppression = storedBool("reflect_suppressed")
        let storedAdConsent = storedBool("reflect_ad_consent")
        let storedThirdPartySharing = storedBool("reflect_third_party_sharing")
        let initialEnabled = args?["initialEnabled"] as? Bool
        let initialAdConsent = args?["initialAdvertisingConsent"] as? Bool
        let initialThirdPartySharing = args?["initialThirdPartySharing"] as? Bool
        let posture = InitialPrivacyPosture.resolve(
            storedSuppression: storedSuppression,
            storedAdvertisingConsent: storedAdConsent,
            storedThirdPartySharing: storedThirdPartySharing,
            initialEnabled: initialEnabled,
            initialAdvertisingConsent: initialAdConsent,
            initialThirdPartySharing: initialThirdPartySharing,
            advertisingHardBlocked: requireAdConsentLatch || consentState == "denied" || ffCoppa,
            coppa: ffCoppa,
            durableTrackingSuppressed: durablePrivacy.trackingSuppressed
        )
        trackingEnabled = posture.trackingEnabled
        advertisingConsent = posture.advertisingConsent
        thirdPartySharing = posture.thirdPartySharing.map { NSNumber(value: $0) }
        if initialEnabled == false || (storedSuppression == nil && initialEnabled != nil) {
            defaults.set(!trackingEnabled, forKey: "reflect_suppressed")
        }
        if initialAdConsent == false || (storedAdConsent == nil && initialAdConsent != nil) {
            defaults.set(advertisingConsent, forKey: "reflect_ad_consent")
        }
        if initialThirdPartySharing == false ||
           (storedThirdPartySharing == nil && initialThirdPartySharing != nil) {
            defaults.set(thirdPartySharing?.boolValue ?? false, forKey: "reflect_third_party_sharing")
        }
        // Purge legacy durable parameter stores before install/first-session
        // events, while preserving values set earlier in this process.
        restoreParams()

        // Snapshot main-thread-only UIKit values now (handleInitialize runs on
        // the platform/main thread), so buildDevice never touches UIKit off-thread.
        queue.maxConcurrentOperationCount = 1   // serial — session state has no torn reads
        snapshotUIKit()
        registerForegroundObservers()

        initialized = true

        // A privacy-delete retry is the sole request allowed while analytics
        // transport is suppressed. Start it before either early return.
        if !pendingPrivacyDelete.allIntents().isEmpty {
            DispatchQueue.global(qos: .utility).async { [weak self] in self?.retryPendingDelete() }
        }

        if !trackingEnabled {
            // A prior deleteUserData()/setEnabled(false) latched suppression — stay
            // fully silent (no identity, install, session, or events) until re-enabled.
            transportGate.block()
            clearEventQueue()
            if !pendingPrivacyDelete.allIntents().isEmpty {
                clearLocalIdentityForConsentDenial(includePrivacyChoices: true)
            } else if consentState == "denied" {
                clearLocalIdentityForConsentDenial()
            }
            log("Initialized in suppressed state — tracking off until re-enabled")
            result(nil)
            return
        }
        if consentState == "denied" {
            transportGate.block()
            clearEventQueue()
            clearLocalIdentityForConsentDenial()
            log("Initialized with consent denied — no identity or transport")
            result(nil)
            return
        }
        transportGate.allow()
        // Adopt a legacy install identity BEFORE minting one, so an upgrade from a
        // wrapper's old store keeps the same install_uuid (+ doesn't re-fire app_install
        // — a legacy install with a uuid already reported it).
        if UserDefaults.standard.string(forKey: "reflect_install_uuid") == nil, let eu = existingInstallUuid {
            UserDefaults.standard.set(eu, forKey: "reflect_install_uuid")
            UserDefaults.standard.set(true, forKey: "reflect_install_reported")
        }
        installUuid = getOrCreateInstallUuid()

        // first_install_time: persisted on the very first run (iOS has no native
        // install timestamp; the Unity bridge hardcodes 0 — we do better).
        firstInstallMs = Int64(defaults.double(forKey: "reflect_first_install_ms"))
        if firstInstallMs == 0 {
            firstInstallMs = nowMs()
            defaults.set(Double(firstInstallMs), forKey: "reflect_first_install_ms")
        }

        registerConnectivityDrain()
        restorePersistedBackoff()   // re-arm a server-outage backoff across restart
        scheduleDrain(0)   // flush any events persisted by a prior session
        // Periodic-flush backstop (Unity FlushIntervalSeconds); drain self-gates on
        // offline/localOnly/consent/backoff so this is a safe net, not a forced send.
        let ft = DispatchSource.makeTimerSource(queue: DispatchQueue.global(qos: .utility))
        ft.schedule(deadline: .now() + .milliseconds(Int(flushIntervalMs)), repeating: .milliseconds(Int(flushIntervalMs)))
        ft.setEventHandler { [weak self] in self?.scheduleDrain(0) }
        flushTimer = ft
        ft.resume()

        // Session bookkeeping runs on the serial `queue`. init runs foregrounded,
        // so recover any session a prior process left open, then open this launch's.
        // Gated on autoSessionTracking (Unity parity) so a host can take manual control.
        if autoSessionTracking {
            let sessionPermit = transportGate.permit()
            queue.addOperation { [weak self] in
                guard let self = self, let sessionPermit = sessionPermit else { return }
                self.sessionMutation(sessionPermit) {
                self.sessionCount = Int64(defaults.integer(forKey: "reflect_session_count"))
                self.sessionActiveMs = Int64(defaults.integer(forKey: "reflect_session_active_ms"))
                self.subsessionCount = Int64(defaults.integer(forKey: "reflect_subsession_count"))
                self.sessionOpen = defaults.bool(forKey: "reflect_session_open")
                self.sessionId = defaults.string(forKey: "reflect_session_id") ?? ""
                // Wall-clock gap since the last persisted activity — survives a process
                // kill (monotonic lastBackgroundElapsed does NOT). Drives the cross-kill
                // session threshold + last_interval_ms (Unity parity).
                let lastWall = Int64(defaults.integer(forKey: "reflect_last_activity_wall"))
                let rawGap: Int64 = lastWall > 0 ? self.nowMs() - lastWall : -1
                // Backward-clock ("time travel") guard (Adjust parity): a device whose
                // wall-clock rolled back since last activity is a fraud/accuracy signal.
                if lastWall > 0 && rawGap < 0 {
                    self.trackEventInternal(eventName: "_clock_skew", propertiesJson: "{\"skew_ms\":\(rawGap)}",
                                            referral: nil, acceptedPermit: sessionPermit)
                }
                let crossKillGap: Int64 = lastWall > 0 ? max(0, rawGap) : -1
                self.recoverInterruptedSession(crossKillGap, permit: sessionPermit)
                self.onForeground(crossKillGap, permit: sessionPermit)
                }
            }
        }

        // First launch → fire app_install (AdServices token). Every launch → app_open.
        let firstLaunch = !defaults.bool(forKey: "reflect_install_reported")
        if firstLaunch {
            defaults.set(true, forKey: "reflect_install_reported")
            trackEventInternal(eventName: "app_install", propertiesJson: nil, referral: adServicesReferral())
            // Once-per-install, immediately after app_install (Unity/Firebase parity).
            trackEventInternal(eventName: "app_first_open", propertiesJson: nil, referral: nil)
            if autoRegisterSkan { armSkan() }   // SKAdNetwork attribution timer at install (Unity parity)
            DispatchQueue.global(qos: .utility).async { [weak self] in self?.linkMeRecover() }
        }
        if autoSessionTracking { trackEventInternal(eventName: "app_open", propertiesJson: nil, referral: nil) }

        // COPPA: emit ONE authoritative "sharing off" signal so the server records the
        // child-directed posture explicitly (Adjust disableThirdPartySharingForCoppa).
        if ffCoppa && !defaults.bool(forKey: "reflect_coppa_tps_sent") {
            defaults.set(true, forKey: "reflect_coppa_tps_sent")
            emitJsonEvent("_third_party_sharing", ["enabled": false, "reason": "coppa"], nil)
        }

        if firstLaunch {
            if autoResolveDeferred {
                scheduleDeferredDeepLinkResolution()
            }
        } else if autoResolveDeferred && defaults.bool(forKey: "reflect_pending_deferred_dl") {
            // X1: a deferred deep link that was offline at first launch stays pending;
            // re-attempt it on any later launch (attribution already re-runs per session).
            scheduleDeferredDeepLinkResolution()
        }
        scheduleAttributionCheck()

        // Auto-ATT (Unity AutoRequestIosTracking): present the prompt at init when opted
        // in (defaults off — the host normally controls prompt timing).
        if autoRequestIosTracking {
            #if canImport(AppTrackingTransparency)
            if #available(iOS 14, *) {
                DispatchQueue.main.async {
                    ATTrackingManager.requestTrackingAuthorization { [weak self] _ in
                        DispatchQueue.main.async { self?.refreshAttStatus(); self?.refreshIdfa() }
                    }
                }
            }
            #endif
        }

        log("Initialized — appKey=\(appKey) installUuid=\(installUuid) firstLaunch=\(firstLaunch)")
        result(nil)
    }

    /// Apple AdServices attribution token (iOS 14.3+). The server exchanges it
    /// with Apple's API for campaign data — its deterministic iOS analog of the
    /// Android Play Install Referrer. Forwarded as referral.attribution_token.
    private func adServicesReferral() -> [String: Any]? {
        #if canImport(AdServices)
        if #available(iOS 14.3, *) {
            if let token = try? AAAttribution.attributionToken() {
                return ["source": "adservices", "attribution_token": token]
            }
        }
        #endif
        return nil
    }

    private func handleTrackRevenue(args: [String: Any]?, result: ReflectResult) {
        if !initialized { result(nil); return }
        let amount = args?["amount"] as? Double ?? 0.0
        let currency = args?["currency"] as? String ?? "USD"
        var top: [String: Any] = ["revenue": amount, "currency": currency]
        if let txn = args?["transactionId"] as? String { top["transaction_id"] = txn }
        if let product = args?["productId"] as? String { top["product_id"] = product }
        var props: [String: Any] = ["revenue_amount": amount, "revenue_currency": currency]
        if let type = args?["revenueType"] as? String { props["revenue_type"] = type }
        emitJsonEvent("revenue", props, top)
        recordSkanRevenue(amount)
        result(nil)
    }

    private func handleTrackPurchase(args: [String: Any]?, result: ReflectResult) {
        if !initialized { result(nil); return }
        emitJsonEvent("purchase", purchaseProps(args), revenueTopLevel(args), deduplicationId: purchaseDedup(args))
        recordSkanRevenue(args?["price"] as? Double ?? 0.0)
        result(nil)
    }

    private func handleTrackSubscription(args: [String: Any]?, result: ReflectResult) {
        if !initialized { result(nil); return }
        var top = revenueTopLevel(args)
        top["is_subscription"] = true
        var props = purchaseProps(args)
        props["is_trial"] = args?["isTrial"] as? Bool ?? false
        emitJsonEvent("subscribe", props, top, deduplicationId: purchaseDedup(args))
        recordSkanRevenue(args?["price"] as? Double ?? 0.0)
        result(nil)
    }

    /// Dedup key for purchases (Unity parity): explicit id, else Play purchase_token,
    /// else transaction_id. Drives both the client LRU and the wire deduplication_id.
    private func purchaseDedup(_ args: [String: Any]?) -> String? {
        if let d = args?["deduplicationId"] as? String, !d.isEmpty { return d }
        if let t = args?["purchaseToken"] as? String, !t.isEmpty { return t }
        if let x = args?["transactionId"] as? String, !x.isEmpty { return x }
        return nil
    }

    private func handleTrackAdRevenue(args: [String: Any]?, result: ReflectResult) {
        if !initialized { result(nil); return }
        let impressions = max(1, args?["impressions"] as? Int ?? 1)
        // Top-level revenue/currency + impressions_count on the ENVELOPE (Unity parity
        // → ad_revenue_events.impressions_count column).
        let top: [String: Any] = [
            "revenue": args?["revenue"] as? Double ?? 0.0,
            "currency": args?["currency"] as? String ?? "USD",
            "impressions_count": impressions,
        ]
        // Canonical server/Unity prop keys (lib/ad-revenue.ts reads exactly these).
        var props: [String: Any] = [
            "mediation_platform": args?["source"] as? String ?? "",                 // was "source"
            "revenue_precision": args?["precision"] as? String ?? "estimated",       // was "precision", default estimated
        ]
        if let n = args?["adNetwork"] as? String { props["ad_network"] = n }
        if let p = args?["adPlacement"] as? String { props["placement"] = p }        // was "ad_placement"
        if let u = args?["adUnit"] as? String { props["ad_unit_id"] = u }            // was "ad_unit"
        if let f = args?["adFormat"] as? String { props["ad_format"] = f }
        emitJsonEvent("ad_impression", props, top)                                   // was "_ad_impression"
        result(nil)
    }

    /// Promoted top-level revenue columns shared by purchase/subscribe.
    /// Feed post-install revenue into the automatic conversion value.
    ///
    /// Called from every revenue-bearing path (revenue / purchase / subscribe).
    /// Apple only accepts an INCREASING fine value inside a measurement window
    /// and each call can restart that window, so SkanAutoConversion suppresses
    /// anything that would not raise the value — this may legitimately send
    /// nothing.
    ///
    /// Fails silently and never throws: a conversion-value update must never
    /// break revenue tracking itself, which is the money-bearing path.
    private func recordSkanRevenue(_ amount: Double) {
        guard amount.isFinite, amount > 0 else { return }
        // Respect the same privacy posture as every other measurement write.
        guard trackingEnabled, consentState != "denied",
              !UserDefaults.standard.bool(forKey: "reflect_suppressed") else { return }

        let total = skanAuto.addRevenue(amount)
        refreshSkanSchemaIfStale()
        guard let update = skanAuto.nextUpdate(nowRevenue: total) else { return }

        handleUpdateConversionValue(
            args: [
                "fineValue": update.fineValue,
                "coarseValue": update.coarse.rawValue,
                "lockWindow": false,
            ],
            result: { [weak self] _ in
                // Only remember it once Apple's API has been invoked, so a
                // failed call is retried on the next purchase instead of being
                // recorded as sent and suppressed forever.
                self?.skanAuto.recordSent(fineValue: update.fineValue)
            })
    }

    /// Fetch the operator's conversion-value schema (24h TTL). Without a schema
    /// no value is ever sent — the SDK must not invent buckets the operator
    /// never defined.
    private func refreshSkanSchemaIfStale() {
        let nowMs = Date().timeIntervalSince1970 * 1000
        guard skanAuto.schemaIsStale(nowMs: nowMs) else { return }
        guard !appKey.isEmpty,
              let url = URL(string: "\(baseUrl)/skan/cv-schema?app_key=\(appKey)")
        else { return }

        var req = URLRequest(url: url)
        req.httpMethod = "GET"
        req.timeoutInterval = 10
        URLSession.shared.dataTask(with: req) { [weak self] data, response, _ in
            guard let self = self,
                  let http = response as? HTTPURLResponse, http.statusCode == 200,
                  let data = data,
                  let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
                  let mappings = obj["mappings"] ?? obj["schema_json"],
                  let json = Self.schemaJsonString(mappings)
            else { return }
            // storeSchema rejects anything that does not parse, so a bad
            // response cannot evict a good cached schema.
            _ = self.skanAuto.storeSchema(json, nowMs: Date().timeIntervalSince1970 * 1000)
        }.resume()
    }

    /// The endpoint may return the mappings as an array or as a JSON string;
    /// normalise both to the string SkanAutoConversion caches.
    private static func schemaJsonString(_ v: Any) -> String? {
        if let s = v as? String { return s }
        guard let d = try? JSONSerialization.data(withJSONObject: v) else { return nil }
        return String(data: d, encoding: .utf8)
    }

    private func revenueTopLevel(_ args: [String: Any]?) -> [String: Any] {
        var top: [String: Any] = [
            "revenue": args?["price"] as? Double ?? 0.0,
            "currency": args?["currency"] as? String ?? "USD",
        ]
        if let txn = args?["transactionId"] as? String { top["transaction_id"] = txn }
        if let product = args?["productId"] as? String { top["product_id"] = product }
        if let o = args?["orderId"] as? String { top["order_id"] = o }   // top-level → promoted column (Unity parity)
        // deduplication_id is now set via emitJsonEvent's deduplicationId param
        // (purchaseDedup → explicit ?? purchase_token ?? transaction_id), Unity parity.
        return top
    }

    /// Store receipt fields (kept in props; used for server-side verification).
    private func purchaseProps(_ args: [String: Any]?) -> [String: Any] {
        var props: [String: Any] = [
            "product_id": args?["productId"] as? String ?? "",
            "price": args?["price"] as? Double ?? 0.0,
            "currency": args?["currency"] as? String ?? "USD",
        ]
        if let receipt = args?["receiptData"] as? String { props["receipt_data"] = receipt }
        if let t = args?["purchaseToken"] as? String { props["purchase_token"] = t }
        if let s = args?["signature"] as? String { props["signature"] = s }
        if let r = args?["salesRegion"] as? String { props["sales_region"] = r }
        // Caller-supplied extras (e.g. verifyAndTrackPurchase's verification_status). Unity parity.
        if let extra = args?["extraProperties"] as? [String: Any] {
            for (k, v) in extra { props[k] = v }
        }
        return props
    }

    private func emitJsonEvent(_ name: String, _ props: [String: Any], _ topLevel: [String: Any]?,
                               deduplicationId: String? = nil,
                               acceptedPermit: PrivacyTransportGate.Permit? = nil) {
        if let data = try? JSONSerialization.data(withJSONObject: props),
           let json = String(data: data, encoding: .utf8) {
            trackEventInternal(eventName: name, propertiesJson: json, referral: nil,
                               topLevel: topLevel, deduplicationId: deduplicationId,
                               acceptedPermit: acceptedPermit)
        }
    }

    private func handleRequestIosTracking(result: @escaping ReflectResult) {
        #if canImport(AppTrackingTransparency)
        if #available(iOS 14, *) {
            ATTrackingManager.requestTrackingAuthorization { [weak self] status in
                // Re-collect IDFA + ATT status on grant (gap 5): the init snapshot was
                // taken before the user answered, so a mid-session grant must refresh it.
                DispatchQueue.main.async { self?.refreshAttStatus(); self?.refreshIdfa() }
                switch status {
                case .authorized:    result("authorized")
                case .denied:        result("denied")
                case .restricted:    result("restricted")
                case .notDetermined: result("not_determined")
                @unknown default:    result("not_determined")
                }
            }
            return
        }
        #endif
        result("unavailable")
    }

    /// Arm Apple's SKAdNetwork attribution timer at install by registering an
    /// initial conversion value of 0 (Unity parity). Called ONCE on first launch
    /// (re-arming later would reset a real CV). Silent — emits no `_skan_cv`.
    private func armSkan() {
        guard trackingEnabled, consentState != "denied",
              !UserDefaults.standard.bool(forKey: "reflect_suppressed"),
              let permit = transportGate.permit() else { return }
        _ = transportGate.runIfValid(permit) {
            guard trackingEnabled, consentState != "denied" else { return }
            if #available(iOS 16.1, *) {
                SKAdNetwork.updatePostbackConversionValue(0, coarseValue: .low, lockWindow: false) { _ in }
            } else if #available(iOS 15.4, *) {
                SKAdNetwork.updatePostbackConversionValue(0) { _ in }
            } else if #available(iOS 14.0, *) {
                SKAdNetwork.registerAppForAdNetworkAttribution()
            }
        }
    }

    private func handleUpdateConversionValue(args: [String: Any]?, result: @escaping ReflectResult) {
        let fineValue = args?["fineValue"] as? Int ?? 0
        let coarseValue = args?["coarseValue"] as? String
        let lockWindow = args?["lockWindow"] as? Bool ?? false
        // Guards (Unity parity) — the core validates so EVERY wrapper is protected,
        // not just the Flutter Dart layer.
        if !initialized {
            result(["success": false, "error": "not_initialized"]); return
        }
        if fineValue < 0 || fineValue > 63 {   // SKAdNetwork fine value range
            result(["success": false, "error": "fine_value_out_of_range"]); return
        }
        guard trackingEnabled, consentState != "denied",
              !UserDefaults.standard.bool(forKey: "reflect_suppressed"),
              let permit = transportGate.permit() else {
            result("{\"success\":false,\"error\":\"measurement_disabled\"}")
            return
        }

        if #available(iOS 16.1, *) {
            var coarse: SKAdNetwork.CoarseConversionValue = .low
            if coarseValue == "medium" { coarse = .medium }
            else if coarseValue == "high" { coarse = .high }
            var invoked = false
            let valid = transportGate.runIfValid(permit) {
                guard trackingEnabled, consentState != "denied" else { return }
                invoked = true
                SKAdNetwork.updatePostbackConversionValue(fineValue, coarseValue: coarse, lockWindow: lockWindow) { [weak self] error in
                    // Never re-enter the non-recursive privacy lock from a system
                    // callback that could theoretically be delivered synchronously.
                    DispatchQueue.main.async {
                        guard let self = self, self.transportGate.isValid(permit) else {
                            result("{\"success\":false,\"error\":\"privacy_state_changed\"}")
                            return
                        }
                        if let error = error {
                            result("{\"success\":false,\"error\":\"\(error.localizedDescription)\"}")
                        } else {
                            self.reportSkanCv(fineValue, coarseValue, lockWindow, "SKAdNetwork4", permit: permit)
                            result("{\"success\":true,\"method\":\"SKAdNetwork4\"}")
                        }
                    }
                }
            }
            if !valid || !invoked {
                result("{\"success\":false,\"error\":\"privacy_state_changed\"}")
            }
            return
        }

        if #available(iOS 15.4, *) {
            var invoked = false
            let valid = transportGate.runIfValid(permit) {
                guard trackingEnabled, consentState != "denied" else { return }
                invoked = true
                SKAdNetwork.updatePostbackConversionValue(fineValue) { [weak self] error in
                    DispatchQueue.main.async {
                        guard let self = self, self.transportGate.isValid(permit) else {
                            result("{\"success\":false,\"error\":\"privacy_state_changed\"}")
                            return
                        }
                        if let error = error {
                            result("{\"success\":false,\"error\":\"\(error.localizedDescription)\"}")
                        } else {
                            self.reportSkanCv(fineValue, coarseValue, lockWindow, "SKAdNetwork3", permit: permit)
                            result("{\"success\":true,\"method\":\"SKAdNetwork3\"}")
                        }
                    }
                }
            }
            if !valid || !invoked {
                result("{\"success\":false,\"error\":\"privacy_state_changed\"}")
            }
            return
        }

        if #available(iOS 14.0, *) {
            var invoked = false
            let valid = transportGate.runIfValid(permit) {
                guard trackingEnabled, consentState != "denied" else { return }
                invoked = true
                SKAdNetwork.registerAppForAdNetworkAttribution()
                SKAdNetwork.updateConversionValue(fineValue)
            }
            guard valid, invoked, transportGate.isValid(permit) else {
                result("{\"success\":false,\"error\":\"privacy_state_changed\"}")
                return
            }
            reportSkanCv(fineValue, coarseValue, lockWindow, "SKAdNetwork2", permit: permit)
            result("{\"success\":true,\"method\":\"SKAdNetwork2\"}")
            return
        }

        result("{\"success\":false,\"error\":\"skan_not_available\"}")
    }

    /// Report a successful SKAN conversion-value update to the Reflect server as a
    /// `_skan_cv` event (rides the normal signed /event path). Lets the server do
    /// first-party CV tracking + reconcile with Apple's eventual SKAN postback —
    /// previously the CV update was applied locally and never reached the server.
    private func reportSkanCv(
        _ fineValue: Int,
        _ coarseValue: String?,
        _ lockWindow: Bool,
        _ method: String,
        permit: PrivacyTransportGate.Permit
    ) {
        var props: [String: Any] = [
            "conversion_value": fineValue,
            "lock_window": lockWindow,
            "skan_version": method,
        ]
        if let c = coarseValue, !c.isEmpty { props["coarse_value"] = c }
        emitJsonEvent("_skan_cv", props, nil, acceptedPermit: permit)
    }

    // MARK: - Inbound deep links (direct custom-scheme + Universal Links)
    // Previously DEAD on iOS: getInitialDeepLink read a UserDefaults key nothing
    // wrote, and there were no openURL / continueUserActivity handlers. Now wired
    // via addApplicationDelegate so warm opens hit the onDeepLink stream and cold
    // launches are captured for getInitialDeepLink.

    /// Push an incoming URL to the host: warm launch → onDeepLink stream; also
    /// persisted so a cold-launch getInitialDeepLink returns it.
    public func handleIncomingURL(_ url: URL) {
        handleIncomingURL(url, acceptedPermit: nil)
    }

    /// Route privacy-sensitive LinkMe input only in the generation that read it.
    private func handleIncomingURL(_ url: URL, acceptedPermit: PrivacyTransportGate.Permit?) {
        var params: [String: String] = [:]
        if let comps = URLComponents(url: url, resolvingAgainstBaseURL: false), let q = comps.queryItems {
            for item in q { params[item.name] = item.value ?? "" }
        }
        var map: [String: Any] = ["url": url.absoluteString, "path": url.path, "isDeferred": false, "params": params]
        if let c = params["click_id"] { map["clickId"] = c }
        if let c = params["campaign"] { map["campaign"] = c }
        if let p = params["partner"]  { map["partner"]  = p }
        var reportProps: [String: Any]?
        let mutation: (applied: Bool, permit: PrivacyTransportGate.Permit?)
        if let acceptedPermit = acceptedPermit {
            var applied = false
            let valid = transportGate.runIfValid(acceptedPermit) {
                guard trackingEnabled, consentState != "denied" else { return }
                let retainedURL = AttributionRetention.urlWithoutQueryOrFragment(url.absoluteString)
                UserDefaults.standard.set(retainedURL, forKey: "reflect_launch_url")
                lastDeepLink = retainedURL
                if retainedURL != lastDeepLinkReported {
                    lastDeepLinkReported = retainedURL
                    reportProps = deepLinkOpenedProperties(url.absoluteString, params, "direct")
                }
                applied = true
            }
            mutation = (valid && applied, valid && applied ? acceptedPermit : nil)
        } else {
            mutation = mutateMeasurementState {
                let retainedURL = AttributionRetention.urlWithoutQueryOrFragment(url.absoluteString)
                UserDefaults.standard.set(retainedURL, forKey: "reflect_launch_url")
                lastDeepLink = retainedURL   // GetLastDeeplink accessor (Unity parity)
                if retainedURL != lastDeepLinkReported {
                    lastDeepLinkReported = retainedURL
                    reportProps = deepLinkOpenedProperties(url.absoluteString, params, "direct")
                }
            }
        }
        guard mutation.applied else { return }
        emitDeepLink(map, acceptedPermit: mutation.permit)
        if let reportProps = reportProps {
            emitJsonEvent("deep_link_opened", reportProps, nil, acceptedPermit: mutation.permit)
        }
    }

    private func deepLinkOpenedProperties(_ url: String, _ params: [String: String], _ source: String) -> [String: Any] {
        var props: [String: Any] = [
            "url": AttributionRetention.urlWithoutQueryOrFragment(url),
            "source": source,
        ]
        if let p = URL(string: url)?.path, !p.isEmpty { props["path"] = p }
        let hasTracking =
            AttributionRetention.hasUniqueClickContext(params) ||
            params["campaign"]?.isEmpty == false ||
            params["partner"]?.isEmpty == false
        if hasTracking { props["is_reattribution"] = true }
        return props
    }

    /// Emit a `deep_link_opened` event for server-side reattribution / deep-link
    /// conversion (mirrors Unity). is_reattribution when the link carries tracking
    /// params. Deduped per URL so it fires at most once.
    private func reportDeepLinkOpened(_ url: String, _ params: [String: String], _ source: String = "direct") {
        guard let permit = transportGate.permit() else { return }
        var props: [String: Any]?
        guard transportGate.runIfValid(permit, {
            let reportKey = AttributionRetention.urlWithoutQueryOrFragment(url)
            guard reportKey != lastDeepLinkReported else { return }
            lastDeepLinkReported = reportKey
            props = deepLinkOpenedProperties(url, params, source)
        }) else { return }
        if let props = props {
            emitJsonEvent("deep_link_opened", props, nil, acceptedPermit: permit)
        }
    }

    /// Resolve/unshorten a tracking URL via /deeplink/resolve (client parity with
    /// Unity's ResolveDeepLink; server url-resolve handling is a shared gap).
    private func resolveLink(_ url: String, permit: PrivacyTransportGate.Permit) -> String? {
        guard trackingEnabled, consentState != "denied", transportGate.isValid(permit) else { return nil }
        let bodyDict: [String: Any] = ["app_key": appKey, "install_uuid": installUuid, "url": url]
        guard let bodyData = try? JSONSerialization.data(withJSONObject: bodyDict),
              let bodyStr = String(data: bodyData, encoding: .utf8) else { return nil }
        let sig = (signingSecret?.isEmpty == false) ? hmacHex(bodyStr, signingSecret!) : nil
        guard let resp = httpJson("\(baseUrl)/deeplink/resolve", "POST", bodyStr, sig, permit: permit),
              let data = resp.data(using: .utf8),
              let obj = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] else { return nil }
        return (obj["resolved_url"] as? String) ?? (obj["deep_link_path"] as? String)
    }

    /// LinkMe (opt-in): if the pasteboard holds an http(s) URL on first launch,
    /// route it as a deferred deep link (improves iOS deferred match). Unity parity.
    private func linkMeRecover() {
        guard let permit = transportGate.permit() else { return }
        // UIKit pasteboard access belongs on the main thread. The read itself is
        // linearized with deny/delete, and routing keeps the same generation so
        // a quick deny/re-enable cannot resurrect text captured before the boundary.
        DispatchQueue.main.async { [weak self] in
            guard let self = self else { return }
            var recoveredURL: URL?
            let readAllowed = self.transportGate.runIfValid(permit) {
                guard self.trackingEnabled, self.consentState != "denied", self.linkMeEnabled,
                      let text = UIPasteboard.general.string,
                      text.hasPrefix("http://") || text.hasPrefix("https://") else { return }
                recoveredURL = URL(string: text)
            }
            guard readAllowed, let url = recoveredURL else { return }
            self.handleIncomingURL(url, acceptedPermit: permit)
        }
    }

    private func handleGetInitialDeepLink(result: ReflectResult) {
        let defaults = UserDefaults.standard
        guard let storedURL = defaults.string(forKey: "reflect_launch_url") else {
            result(nil)
            return
        }
        // Upgrade legacy cold-launch state fail closed before exposing it.
        let urlString = AttributionRetention.urlWithoutQueryOrFragment(storedURL)
        if urlString != storedURL {
            defaults.set(urlString, forKey: "reflect_launch_url")
        }
        guard
              let url = URL(string: urlString) else {
            result(nil)
            return
        }
        var dl: [String: Any] = ["url": url.absoluteString, "path": url.path, "isDeferred": false]
        var params: [String: String] = [:]
        if let comps = URLComponents(url: url, resolvingAgainstBaseURL: false),
           let queryItems = comps.queryItems {
            for item in queryItems { params[item.name] = item.value ?? "" }
        }
        dl["params"] = params
        // Insert only when present — boxing a nil Optional into [String: Any]
        // makes the dict invalid JSON, so JSONSerialization would throw and the
        // whole deep link would be silently dropped. Dart reads these as String?.
        if let c = params["click_id"] { dl["clickId"] = c }
        if let c = params["campaign"] { dl["campaign"] = c }
        if let p = params["partner"]  { dl["partner"] = p }
        reportDeepLinkOpened(url.absoluteString, params)
        defaults.removeObject(forKey: "reflect_launch_url")   // consume once
        if let data = try? JSONSerialization.data(withJSONObject: dl),
           let json = String(data: data, encoding: .utf8) {
            result(json)
        } else { result(nil) }
    }

    private func clearEventQueue() {
        queueLock.lock()
        eventQueue.removeAll()
        transientAttributionPayloads.removeAll()
        let url = queueFileURL()
        if let url = url {
            try? FileManager.default.removeItem(at: url)
            try? FileManager.default.removeItem(at: url.appendingPathExtension("tmp"))
        }
        queueLock.unlock()
        headRetryCount = 0
        drainBackoffMs = 0
        lastRetryAfterMs = 0
        pendingContinueMs = 0
        nextSendAllowedMs = 0
        clearPersistedBackoff()
    }

    /// Consent withdrawal rotates every local measurement identifier. Consent,
    /// suppression, and an outstanding explicit delete marker remain authoritative.
    private func clearLocalIdentityForConsentDenial(includePrivacyChoices: Bool = false) {
        sessionStateLock.lock(); defer { sessionStateLock.unlock() }
        PrivacyPersistence.clearIdentityAndEvents(
            defaults: .standard,
            queueURL: queueFileURL(),
            includePrivacyChoices: includePrivacyChoices,
            preservePendingDelete: true
        )
        installUuid = ""
        userId = nil
        userProperties = nil
        pushToken = nil
        integrityToken = nil
        externalDeviceId = nil
        pendingAttribution = nil
        pendingDeferredDeepLink = nil
        lastDeepLink = nil
        lastDeepLinkReported = nil
        lastAttributionCheckMs = 0
        eventStateLock.lock()
        lastCrashMs = 0
        seenDedupIds.removeAll()
        dedupOrder.removeAll()
        eventStateLock.unlock()
        snapIdfa = nil
        snapIdfv = nil
        cachedAttStatus = nil
        firstInstallMs = 0
        uikitSnapshotted = false
        sessionOpen = false
        sessionId = ""
        sessionCount = 0
        subsessionCount = 0
        sessionActiveMs = 0
        sessionStartElapsed = 0
        lastBackgroundElapsed = 0
        droppedCount = 0
        stopHeartbeat()
        globalLock.lock()
        globalProperties.removeAll()
        globalPropertySourceAtMs.removeAll()
        partnerParameters.removeAll()
        partnerParameterSourceAtMs.removeAll()
        partnerSharing.removeAll()
        globalLock.unlock()
        if includePrivacyChoices {
            advertisingConsent = false
            thirdPartySharing = nil
        }
    }

    /// Start a new measurement lifetime after consent/enable re-opens transport.
    private func activateAfterPrivacyGate() {
        guard initialized, trackingEnabled, consentState != "denied" else { return }
        if installUuid.isEmpty { installUuid = getOrCreateInstallUuid() }
        if Thread.isMainThread { snapshotUIKit() }
        else { DispatchQueue.main.sync { snapshotUIKit() } }
        if firstInstallMs == 0 {
            firstInstallMs = nowMs()
            UserDefaults.standard.set(Double(firstInstallMs), forKey: "reflect_first_install_ms")
        }
        transportGate.allow()
        registerConnectivityDrain()
        scheduleDrain(0)
        flushTimer?.cancel()
        let ft = DispatchSource.makeTimerSource(queue: DispatchQueue.global(qos: .utility))
        ft.schedule(deadline: .now() + .milliseconds(Int(flushIntervalMs)),
                    repeating: .milliseconds(Int(flushIntervalMs)))
        ft.setEventHandler { [weak self] in self?.scheduleDrain(0) }
        flushTimer = ft
        ft.resume()

        if autoSessionTracking, let permit = transportGate.permit() {
            queue.addOperation { [weak self] in self?.startSession(permit: permit) }
        }
        let defaults = UserDefaults.standard
        let firstLaunch = !defaults.bool(forKey: "reflect_install_reported")
        if firstLaunch {
            defaults.set(true, forKey: "reflect_install_reported")
            trackEventInternal(eventName: "app_install", propertiesJson: nil, referral: adServicesReferral())
            trackEventInternal(eventName: "app_first_open", propertiesJson: nil, referral: nil)
            if autoRegisterSkan { armSkan() }
            if autoResolveDeferred {
                scheduleDeferredDeepLinkResolution()
            }
        }
        if autoSessionTracking { trackEventInternal(eventName: "app_open", propertiesJson: nil, referral: nil) }
        scheduleAttributionCheck()
    }

    private func handleDeleteUserData(result: @escaping ReflectResult) {
        // Linearization point: no analytics request can register after block(),
        // and every request registered before it receives cancel().
        let target = currentPrivacyDeleteTarget()
        let currentUuid = PrivacyDeletePolicy.identifierForJournal(
            initialized: initialized,
            inMemory: installUuid,
            persisted: UserDefaults.standard.string(forKey: "reflect_install_uuid"),
            target: target
        ) ?? pendingPrivacyDelete.allIntents().first(where: { $0.target == target })?.identifier
        let uid = userId
        trackingEnabled = false
        transportGate.block()
        let tombstonePersisted = privacyTombstone.update(trackingSuppressed: true)
        UserDefaults.standard.set(true, forKey: "reflect_suppressed")
        let defaultsPersisted = UserDefaults.standard.synchronize()
        guard let currentUuid = currentUuid else {
            // Do not erase the only identifier when no authenticated target can
            // be journaled. The request remains locally fail-closed and reports
            // failure instead of a false-positive successful deletion.
            clearEventQueue()
            result(ReflectError(
                code: "privacy_delete_unavailable",
                message: initialized
                    ? "Privacy deletion requires a persisted install identifier and complete app configuration."
                    : "Privacy deletion cannot be journaled before initialization.",
                details: ["suppressionPersisted": tombstonePersisted || defaultsPersisted]
            ))
            return
        }
        // Crash-safe order: suppression and all unconfirmed remote identifiers
        // reach durable defaults before any local identifier is erased.
        let journal = pendingPrivacyDelete.journalSuppression(
            currentUuid,
            target: target
        )
        guard journal.durable else {
            result(ReflectError(
                code: "privacy_persistence_failed",
                message: "Could not durably journal privacy deletion; identity was retained and transport remains blocked.",
                details: nil
            ))
            return
        }
        let currentIntent = journal.intent
        clearEventQueue()
        clearLocalIdentityForConsentDenial(includePrivacyChoices: true)

        // The dedicated privacy route bypasses analytics transport. A true callback
        // covers every previously journaled lifetime, not only the newest UUID.
        drainPendingDeletes(
            pendingPrivacyDelete.allIntents(),
            currentIntent: currentIntent,
            currentUserId: uid
        ) { complete in
            DispatchQueue.main.async { result(complete) }
        }
    }

    /// POST a /privacy/delete for one install_uuid. Authentication is mandatory,
    /// and success requires the server's explicit {ok:true,queued:true} receipt.
    private func currentPrivacyDeleteTarget() -> PrivacyDeleteTarget {
        PrivacyDeleteTarget(baseUrl: baseUrl, appKey: appKey, companyKey: companyKey)
    }

    private func sendPrivacyDelete(
        _ intent: PendingPrivacyDeleteIntent,
        _ uid: String?,
        _ completion: @escaping (Bool) -> Void
    ) {
        let currentTarget = currentPrivacyDeleteTarget()
        guard PrivacyDeletePolicy.canDispatch(intent, using: currentTarget),
              let target = intent.target else {
            log("privacy/delete deferred — pending intent belongs to another or legacy-unscoped config")
            completion(false)
            return
        }
        guard let secret = PrivacyDeletePolicy.signingSecret(signingSecret) else {
            log("privacy/delete deferred — signing secret unavailable")
            completion(false)
            return
        }
        var body: [String: Any] = ["app_key": target.appKey, "install_uuid": intent.identifier]
        if let u = uid { body["user_id"] = u }
        guard let bodyData = try? JSONSerialization.data(withJSONObject: body),
              let bodyStr = String(data: bodyData, encoding: .utf8),
              let url = URL(string: "\(target.baseUrl)/privacy/delete") else { completion(false); return }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue(hostSdkVersion, forHTTPHeaderField: "X-Reflect-Sdk")
        request.setValue(target.appKey, forHTTPHeaderField: "X-Reflect-App-Key")
        if let ck = target.companyKey, !ck.isEmpty { request.setValue(ck, forHTTPHeaderField: "X-Reflect-Company-Key") }
        request.setValue(hmacHex(bodyStr, secret), forHTTPHeaderField: "X-Reflect-Signature")
        request.setValue("1", forHTTPHeaderField: "X-Reflect-Signature-Version")
        request.httpBody = bodyData
        request.timeoutInterval = 10
        URLSession.shared.dataTask(with: request) { data, response, _ in
            let code = (response as? HTTPURLResponse)?.statusCode ?? 0
            completion(PrivacyDeletePolicy.isAccepted(statusCode: code, data: data))
        }.resume()
    }

    /// On launch, retry an unconfirmed /privacy/delete (GDPR durability, Unity parity).
    private func retryPendingDelete() {
        drainPendingDeletes(pendingPrivacyDelete.allIntents(), currentIntent: nil, currentUserId: nil) { _ in }
    }

    private func drainPendingDeletes(
        _ pending: [PendingPrivacyDeleteIntent],
        currentIntent: PendingPrivacyDeleteIntent?,
        currentUserId: String?,
        allAccepted: Bool = true,
        completion: @escaping (Bool) -> Void
    ) {
        guard let first = pending.first else {
            completion(allAccepted && pendingPrivacyDelete.allIntents().isEmpty)
            return
        }
        sendPrivacyDelete(first, first == currentIntent ? currentUserId : nil) { ok in
            if ok { self.pendingPrivacyDelete.removeIfMatches(first) }
            self.drainPendingDeletes(
                Array(pending.dropFirst()),
                currentIntent: currentIntent,
                currentUserId: currentUserId,
                allAccepted: allAccepted && ok,
                completion: completion
            )
        }
    }

    // MARK: - Event Tracking

    private func trackEventInternal(eventName: String, propertiesJson: String?, referral: [String: Any]?,
                                    topLevel: [String: Any]? = nil, callbackId: String? = nil,
                                    callbackParamsJson: String? = nil, partnerParamsJson: String? = nil,
                                    deduplicationId: String? = nil,
                                    acceptedPermit: PrivacyTransportGate.Permit? = nil) {
        if !initialized || !trackingEnabled { return }   // forget-me / disable latch
        // Bind queued build work to the generation in which the API call was
        // accepted. A deny/delete followed by a quick reopen invalidates it.
        guard let permit = acceptedPermit ?? transportGate.permit(),
              transportGate.isValid(permit) else { return }
        queue.addOperation { [weak self] in
            guard let self = self else { return }
            // Work accepted just before a privacy transition may still be waiting
            // on the serial queue. Re-check policy at execution time.
            guard self.trackingEnabled, self.consentState != "denied",
                  self.transportGate.isValid(permit) else { return }
            self.pruneExpiredEphemeralParameters()
            self.eventStateLock.lock()
            guard self.transportGate.isValid(permit) else {
                self.eventStateLock.unlock()
                return
            }
            if let d = deduplicationId, !d.isEmpty, self.isDuplicateEvent(d) {
                self.eventStateLock.unlock()
                self.log("Dropped duplicate '\(eventName)' (dedup_id=\(d))")
                return
            }
            if eventName == "_crash" {
                let now = self.nowMs()
                if now - self.lastCrashMs < 60_000 {
                    self.eventStateLock.unlock()
                    return
                }
                self.lastCrashMs = now
            }
            self.eventStateLock.unlock()
            var payload: [String: Any] = [
                "app_key": self.appKey,
                "event_name": eventName,
                "event_id": UUID().uuidString.replacingOccurrences(of: "-", with: ""),
                "event_ts_ms": Int64(Date().timeIntervalSince1970 * 1000),
                "install_uuid": self.installUuid,
                "sdk_version": hostSdkVersion,
                "platform": "ios",
                "environment": self.environment,
                "is_foreground": self.isForegroundState,
                "consent_state": self.consentState
            ]
            if self.ffCoppa { payload["ff_coppa"] = true }
            // Session context on EVERY event (Unity parity), not just session_start/end.
            if !self.sessionId.isEmpty { payload["session_id"] = self.sessionId }
            if self.sessionCount > 0 {
                payload["session_count"] = self.sessionCount
                payload["subsession_count"] = self.subsessionCount
            }
            payload["third_party_sharing"] = self.thirdPartySharing?.boolValue ?? true   // always present, default true (Unity parity)
            if let att = self.attStatusString() { payload["att_status"] = att }
            if let v = self.appVersionName() { payload["app_version"] = v }
            if let userId = self.userId { payload["user_id"] = userId }
            if let token = self.pushToken { payload["push_token"] = token }
            if let ext = self.externalDeviceId { payload["external_device_id"] = ext }
            // Promoted top-level fields (revenue/currency/transaction_id/...).
            if let topLevel = topLevel { for (k, v) in topLevel { payload[k] = v } }

            payload["device"] = self.buildDevice()
            if let referral = referral { payload["referral"] = referral }

            var merged: [String: Any] = [:]
            self.globalLock.lock()
            for (k, v) in self.globalProperties { merged[k] = v }
            self.globalLock.unlock()
            if let json = propertiesJson,
               let data = json.data(using: .utf8),
               let props = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                for (k, v) in props { merged[k] = v }
            }
            if !merged.isEmpty { payload["properties"] = merged }
            if let userProps = self.userProperties { payload["user_properties"] = userProps }

            // Per-event options (Unity ReflectEventOptions parity).
            if let cid = callbackId { payload["callback_id"] = cid }
            if let dedup = deduplicationId { payload["deduplication_id"] = dedup }
            if let cpj = callbackParamsJson, let d = cpj.data(using: .utf8),
               let cp = try? JSONSerialization.jsonObject(with: d) as? [String: Any], !cp.isEmpty {
                payload["callback_params"] = cp
            }
            var partner: [String: Any] = [:]
            self.globalLock.lock()
            for (k, v) in self.partnerParameters { partner[k] = v }
            self.globalLock.unlock()
            if let ppj = partnerParamsJson, let d = ppj.data(using: .utf8),
               let pp = try? JSONSerialization.jsonObject(with: d) as? [String: Any] {
                for (k, v) in pp { partner[k] = v }
            }
            if !partner.isEmpty { payload["partner_params"] = partner }

            if let data = try? JSONSerialization.data(withJSONObject: payload),
               let json = String(data: data, encoding: .utf8) {
                self.enqueue(json, permit: permit)
            }
        }
    }

    // MARK: - Durable event queue (persist → drain → retry)

    private func privacyTombstoneFileURL() -> URL? {
        let fm = FileManager.default
        guard let dir = try? fm.url(for: .applicationSupportDirectory, in: .userDomainMask,
                                    appropriateFor: nil, create: true) else { return nil }
        return dir.appendingPathComponent(ReflectCore.privacyTombstoneFileName)
    }

    private func queueFileURL() -> URL? {
        let fm = FileManager.default
        guard let dir = try? fm.url(for: .applicationSupportDirectory, in: .userDomainMask,
                                    appropriateFor: nil, create: true) else { return nil }
        return dir.appendingPathComponent(ReflectCore.queueFileName)
    }

    private func loadQueue() {
        guard let url = queueFileURL(),
              let text = try? String(contentsOf: url, encoding: .utf8) else { return }
        queueLock.lock()
        for line in text.split(separator: "\n", omittingEmptySubsequences: true) {
            eventQueue.append(String(line))
        }
        scrubQueueLocked()
        queueLock.unlock()
    }

    /// Caller MUST hold queueLock.
    private func persistQueueLocked() {
        guard let url = queueFileURL() else { return }
        let text = eventQueue.joined(separator: "\n")
        try? text.write(to: url, atomically: true, encoding: .utf8)
    }

    /// Caller MUST hold queueLock. Removes expired click context from legacy
    /// queue rows and drops malformed rows fail closed.
    private func scrubQueueLocked(
        nowMs: Int64 = Int64(Date().timeIntervalSince1970 * 1000)
    ) {
        let scrubbed = eventQueue.compactMap {
            AttributionRetention.scrubQueuedEvent($0, nowMs: nowMs)
        }
        if scrubbed != eventQueue {
            eventQueue = scrubbed
            persistQueueLocked()
        }
    }

    /// Diagnostics snapshot for Reflect.debugSnapshot() / the debug overlay.
    /// PII-safe: identifiers reported as presence booleans, never raw values.
    private func debugState() -> [String: Any] {
        queueLock.lock(); let qsize = eventQueue.count; queueLock.unlock()
        return [
            "sdkVersion": hostSdkVersion,
            "platform": "ios",
            "baseUrl": baseUrl,
            "initialized": initialized,
            "trackingEnabled": trackingEnabled,
            "offlineMode": offlineMode,
            "consentState": consentState,
            "advertisingConsent": advertisingConsent,
            "coppa": ffCoppa,
            "queueSize": qsize,
            "droppedCount": droppedCount,
            "headRetryCount": headRetryCount,
            "backoffMs": drainBackoffMs,
            "sessionCount": sessionCount,
            "subsessionCount": subsessionCount,
            "sessionId": sessionId,
            "sessionOpen": sessionOpen,
            "dedupMax": dedupMax,
            "dedupWindow": seenDedupIds.count,
            "batchSize": cfgBatchSize,
            "maxQueue": cfgMaxQueue,
            "userIdPresent": userId != nil,
            "externalDeviceIdPresent": externalDeviceId != nil,
            "pushTokenPresent": !(pushToken?.isEmpty ?? true),
            "integrityTokenPresent": !(integrityToken?.isEmpty ?? true),
        ]
    }

    /// Bounded insertion-order dedup window (Unity parity). True if dedupId was seen
    /// recently. Caller guarantees serial-`queue` access → no lock.
    private func isDuplicateEvent(_ dedupId: String) -> Bool {
        if dedupMax <= 0 { return false }
        if seenDedupIds.contains(dedupId) { return true }
        seenDedupIds.insert(dedupId)
        dedupOrder.append(dedupId)
        while dedupOrder.count > dedupMax { seenDedupIds.remove(dedupOrder.removeFirst()) }
        return false
    }

    private func enqueue(_ payload: String, permit: PrivacyTransportGate.Permit) {
        guard trackingEnabled, consentState != "denied", transportGate.isValid(permit) else { return }
        guard let retentionSafePayload = AttributionRetention.scrubQueuedEvent(payload) else { return }
        var transientEventId: String?
        queueLock.lock()
        guard trackingEnabled, consentState != "denied", transportGate.isValid(permit) else {
            queueLock.unlock()
            return
        }
        scrubQueueLocked()
        // Overflow → drop the NEWEST (this event) instead of the oldest, so the
        // attribution-critical install/first-session events at the head survive a
        // sustained backlog (Unity parity).
        if eventQueue.count >= cfgMaxQueue {
            droppedCount += 1
            queueLock.unlock()
            return
        }
        eventQueue.append(retentionSafePayload)
        if AttributionRetention.hasTransientAttributionContext(payload),
           let eventId = extractEventId(retentionSafePayload) {
            transientEventId = eventId
            transientAttributionPayloads[eventId] = (
                payload: payload,
                expiresAtMs: nowMs() + ReflectCore.transientAttributionTtlMs
            )
        }
        persistQueueLocked()
        queueLock.unlock()
        if let transientEventId {
            DispatchQueue.global(qos: .utility).asyncAfter(
                deadline: .now() + .milliseconds(Int(ReflectCore.transientAttributionTtlMs))
            ) { [weak self] in
                guard let self else { return }
                self.queueLock.lock()
                if let current = self.transientAttributionPayloads[transientEventId],
                   current.expiresAtMs <= self.nowMs() {
                    self.transientAttributionPayloads.removeValue(forKey: transientEventId)
                }
                self.queueLock.unlock()
            }
        }
        scheduleDrain(0)
    }

    private func scheduleDrain(_ delayMs: Int64) {
        let deadline = DispatchTime.now() + .milliseconds(Int(delayMs))
        DispatchQueue.global(qos: .utility).asyncAfter(deadline: deadline) { [weak self] in
            self?.drain()
        }
    }

    private func beginSending() -> Bool {
        sendLock.lock(); defer { sendLock.unlock() }
        if sending { return false }
        sending = true
        return true
    }
    private func endSending() { sendLock.lock(); sending = false; sendLock.unlock() }

    // Runs on a background dispatch queue (via scheduleDrain), decoupled from the
    // event-build OperationQueue. beginSending() keeps exactly one in flight.
    private func drain() {
        queueLock.lock(); scrubQueueLocked(); queueLock.unlock()
        if !initialized || !trackingEnabled { return }
        if offlineMode || localOnly { return }   // offline / debug-mode → keep queued, don't send
        if consentState == "denied" { return }
        guard let permit = transportGate.permit() else { return }
        if !beginSending() { return }
        var releaseGuard = true
        defer {
            if releaseGuard { endSending() }
            // If this drain was cancelled and consent was re-opened before its
            // completion arrived, make sure the fresh generation is not stranded.
            if !transportGate.isValid(permit), transportGate.permit() != nil {
                queueLock.lock(); let hasEvents = !eventQueue.isEmpty; queueLock.unlock()
                if hasEvents { scheduleDrain(0) }
            }
        }
        while true {
            if !trackingEnabled || consentState == "denied" || offlineMode || !transportGate.isValid(permit) { break }
            // A long multi-batch drain can cross the retention boundary.
            // Re-scrub the durable head immediately before every snapshot.
            queueLock.lock(); scrubQueueLocked(); queueLock.unlock()
            // Honor the send gate (retry backoff / continue_in pace) authoritatively,
            // so a competing scheduleDrain(0) can't bypass it.
            let now = monotonicMs()
            if nextSendAllowedMs > now {
                endSending(); releaseGuard = false; scheduleDrain(nextSendAllowedMs - now); return
            }
            // Take up to batchSize events from the head into ONE request (Unity
            // parity — was one event per HTTP call).
            queueLock.lock()
            let n = min(cfgBatchSize, eventQueue.count)
            var rawBatch = Array(eventQueue.prefix(n))
            let immediateBatch = rawBatch.map { transientPayloadForImmediateSendLocked($0) }
            let qsize = eventQueue.count
            queueLock.unlock()
            // X2: re-stamp the LIVE consent/sharing/ATT onto each queued event just before
            // send (Adjust updatePackagesTrackingI) — a frozen-at-enqueue event otherwise
            // transmits a stale decision (e.g. an install built pre-ATT-grant).
            var batchEvents = immediateBatch.map { restampConsent($0) }
            if batchEvents.isEmpty { break }
            // Second half of the server's batch contract: the COUNT ceiling is clamped
            // at config time, but bytes are only knowable here, after re-stamping.
            // Trim rawBatch too — the success path below removes exactly
            // rawBatch.count events, so shrinking only the payload would delete events
            // that were never sent. The untrimmed tail stays queued for the next pass.
            let fitting = wireFittingCount(batchEvents, headRetryCount, qsize)
            if fitting < batchEvents.count {
                log("Batch trimmed to wire limit [n=\(batchEvents.count) → \(fitting)]")
                rawBatch = Array(rawBatch.prefix(fitting))
                batchEvents = Array(batchEvents.prefix(fitting))
            }
            switch postBatch(batchEvents, headRetryCount, qsize, permit) {
            case .success, .drop:
                if !transportGate.isValid(permit) { break }
                queueLock.lock()
                let sameHead = eventQueue.count >= rawBatch.count &&
                    Array(eventQueue.prefix(rawBatch.count)) == rawBatch &&
                    transportGate.isValid(permit)
                if sameHead {
                    for persisted in rawBatch {
                        if let eventId = extractEventId(persisted) {
                            transientAttributionPayloads.removeValue(forKey: eventId)
                        }
                    }
                    eventQueue.removeFirst(rawBatch.count)
                    persistQueueLocked()
                }
                queueLock.unlock()
                if !sameHead { break }
                var pace: Int64 = 0
                guard transportGate.runIfValid(permit, {
                    drainBackoffMs = 0
                    headRetryCount = 0
                    nextSendAllowedMs = 0
                    clearPersistedBackoff()
                    pace = pendingContinueMs
                    pendingContinueMs = 0
                    if pace > 0 { nextSendAllowedMs = monotonicMs() + pace }
                }) else { break }
                if pace > 0 {
                    endSending(); releaseGuard = false; scheduleDrain(pace); return
                }
            case .retry:
                var retryDelay: Int64 = 0
                guard transportGate.runIfValid(permit, {
                    headRetryCount += 1
                    drainBackoffMs = nextBackoff(drainBackoffMs, lastRetryAfterMs)
                    nextSendAllowedMs = monotonicMs() + drainBackoffMs
                    persistBackoff(drainBackoffMs)
                    retryDelay = drainBackoffMs
                }) else { break }
                endSending()            // release before scheduling the retry
                releaseGuard = false
                scheduleDrain(retryDelay)
                return
            case .attBlocked:
                // Refused by tracking-domain policy: the events are intact and
                // the gate opens on the user's ATT answer, not with time. Back
                // off against the low ATT ceiling rather than the server-outage
                // one, and — critically — persist NOTHING, so a relaunch that
                // already carries the answer sends immediately instead of
                // serving out a deadline earned behind the gate.
                var parkDelay: Int64 = 0
                guard transportGate.runIfValid(permit, {
                    headRetryCount += 1
                    drainBackoffMs = min(nextBackoff(drainBackoffMs, 0),
                                         ReflectCore.attBlockedMaxBackoffMs)
                    clearPersistedBackoff()
                    parkDelay = drainBackoffMs
                    nextSendAllowedMs = monotonicMs() + parkDelay
                }) else { break }
                endSending()
                releaseGuard = false
                scheduleDrain(parkDelay)
                return
            case .cancelled:
                break
            }
        }
    }

    /// Persist the current backoff as a WALL-CLOCK deadline so a server-outage
    /// backoff still gates sending after an app relaunch (Unity parity).
    private func persistBackoff(_ delayMs: Int64) {
        let d = UserDefaults.standard
        d.set(Int(delayMs), forKey: "reflect_backoff_ms")
        d.set(Int(nowMs() + delayMs), forKey: "reflect_backoff_deadline")
    }
    private func clearPersistedBackoff() {
        let d = UserDefaults.standard
        d.removeObject(forKey: "reflect_backoff_ms")
        d.removeObject(forKey: "reflect_backoff_deadline")
    }
    /// Restore a persisted backoff on init: if its wall-clock deadline is still in
    /// the future, re-arm the monotonic send gate for the remaining time.
    private func restorePersistedBackoff() {
        let d = UserDefaults.standard
        let deadline = Int64(d.integer(forKey: "reflect_backoff_deadline"))
        if deadline <= 0 { return }
        let remaining = deadline - nowMs()
        if remaining <= 0 { clearPersistedBackoff(); return }
        drainBackoffMs = Int64(d.integer(forKey: "reflect_backoff_ms"))
        nextSendAllowedMs = monotonicMs() + min(remaining, ReflectCore.maxBackoffMs)
    }

    private func nextBackoff(_ current: Int64, _ retryAfter: Int64) -> Int64 {
        if retryAfter > 0 { return min(retryAfter, ReflectCore.maxBackoffMs) }
        let base = current <= 0 ? ReflectCore.baseBackoffMs : min(current * 2, ReflectCore.maxBackoffMs)
        let jitter = Int64(Double(base) * (0.5 + Double.random(in: 0...0.5)))
        return max(jitter, ReflectCore.baseBackoffMs)
    }

    /// POST one event and classify: success=2xx (delete), drop=permanent 4xx
    /// (malformed → delete), retry=429/408/5xx/network/timeout (keep + backoff,
    /// honoring Retry-After). Blocks the calling background op via a semaphore.
    private func postBatch(
        _ events: [String],
        _ retryCount: Int,
        _ queueSize: Int,
        _ permit: PrivacyTransportGate.Permit
    ) -> SendResult {
        guard transportGate.isValid(permit) else { return .cancelled }
        lastRetryAfterMs = 0
        pendingContinueMs = 0
        let bid = batchId(events)
        let bodyStr = buildBatchBody(events, retryCount, queueSize, bid)
        let secret = (signingSecret?.isEmpty == false) ? signingSecret : nil
        let signed = secret != nil
        guard let url = URL(string: "\(baseUrl)\(signed ? "/event" : "/event/batch")"),
              let rawBody = bodyStr.data(using: .utf8) else { return .drop }
        // gzip batches of ≥10 events (Unity/Android parity). Sign over the WIRE bytes
        // (the server verifies the sig over the compressed bytes, THEN decompresses).
        let gz = events.count >= 10 ? gzipBody(rawBody) : nil
        let wireBody = gz ?? rawBody
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        // The RUNTIME platform, not the SDK flavour — this engine only ever runs on
        // iOS, so it is a constant and hosts cannot override it. (It read "flutter"
        // until 2026-08: a framework name in a platform field, which mis-reported
        // every Unity/RN/native host as Flutter.) The flavour + version travel
        // separately in X-Reflect-Sdk.
        request.setValue(ReflectCore.platform, forHTTPHeaderField: "X-Reflect-Platform")
        request.setValue(hostSdkVersion, forHTTPHeaderField: "X-Reflect-Sdk")
        if gz != nil { request.setValue("gzip", forHTTPHeaderField: "Content-Encoding") }
        // SIGNED path (matches the Unity SDK): HMAC-SHA256 the WIRE body, POST to /event.
        if signed, let secret = secret {
            request.setValue(appKey, forHTTPHeaderField: "X-Reflect-App-Key")
            if let ck = companyKey, !ck.isEmpty { request.setValue(ck, forHTTPHeaderField: "X-Reflect-Company-Key") }
            request.setValue(hmacHexData(wireBody, secret), forHTTPHeaderField: "X-Reflect-Signature")
            request.setValue("1", forHTTPHeaderField: "X-Reflect-Signature-Version")
            // Attestation token — header only, NOT in the signed bytes (Unity parity).
            if let t = integrityToken, !t.isEmpty {
                request.setValue(t, forHTTPHeaderField: "X-Reflect-Integrity-Token")
            }
        }
        request.httpBody = wireBody
        request.timeoutInterval = 15
        var outcome: SendResult = .retry
        var responseRetryIn: Int64 = 0
        var responseContinueIn: Int64 = 0
        var responseCode = 0
        // Snapshot the main-thread-owned ATT value here (still on the drain
        // queue) rather than reading it from the URLSession callback.
        let attSnapshot = cachedAttStatus
        let sem = DispatchSemaphore(value: 0)
        let task = URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            defer { sem.signal() }
            if let error = error {
                // An ingest host declared in NSPrivacyTrackingDomains is refused
                // by iOS as "offline" until ATT is answered. That is a policy
                // gate, not flakiness — classify it so drain() waits for the
                // answer instead of burning persisted exponential backoff.
                outcome = AttTransportPolicy.classify(
                    errorDomain: (error as NSError).domain,
                    errorCode: (error as NSError).code,
                    attStatus: attSnapshot
                ) == .attBlocked ? .attBlocked : .retry
                return
            }
            guard let http = response as? HTTPURLResponse else { outcome = .retry; return }
            let code = http.statusCode
            responseCode = code
            switch code {
            case 200..<300: outcome = .success
            case 408, 429, 500..<600: outcome = .retry
            case 400..<500: outcome = .drop
            default: outcome = .retry
            }
            // Parse server pacing directives from the body (response-driven retry).
            // Guarded — a hostile/empty body must never throw; fall back to backoff.
            let directives = self?.parseDirectives(data) ?? (retryIn: 0, continueIn: 0)
            // Honor Retry-After on ANY retryable response (Unity parity), not just 429/503.
            let hdrRetry = (outcome == .retry)
                ? (self?.parseRetryAfter(http.value(forHTTPHeaderField: "Retry-After")) ?? 0) : 0
            responseRetryIn = directives.retryIn > 0 ? directives.retryIn : hdrRetry
            responseContinueIn = (200..<300).contains(code) ? directives.continueIn : 0
        }
        guard transportGate.register(task, permit: permit, cancel: { task.cancel() }) else {
            task.cancel()
            return .cancelled
        }
        task.resume()
        sem.wait()
        transportGate.unregister(task)
        guard transportGate.runIfValid(permit, {
            lastRetryAfterMs = responseRetryIn
            pendingContinueMs = responseContinueIn
            // Name the ATT gate explicitly. It reports as a bare "offline"
            // URLError, so without this the log reads as a generic network
            // failure and sends the next person debugging it after the radio.
            let status = outcome == .attBlocked
                ? "BLOCKED (host in NSPrivacyTrackingDomains, ATT unanswered — parked for the prompt)"
                : "→ \(responseCode)"
            log("Batch sent \(status) \(signed ? "(signed /event)" : "(/event/batch)") [n=\(events.count) batch=\(bid) retry=\(retryCount) q=\(queueSize)]"
                + (responseRetryIn > 0 ? " retry_in=\(responseRetryIn)ms" : "")
                + (responseContinueIn > 0 ? " continue_in=\(responseContinueIn)ms" : ""))
        }) else { return .cancelled }
        return outcome
    }

    /// Wrap one event in the batch envelope (mirrors Unity/Android): sent_at_ms
    /// re-stamped per attempt, sdk_telemetry, stable batch_id. app_key rides the
    /// envelope for the unsigned /event/batch path (header on the signed /event).
    private func buildBatchBody(_ events: [String], _ retryCount: Int, _ queueSize: Int, _ bid: String) -> String {
        return "{\"app_key\":\"\(appKey)\",\"events\":[\(events.joined(separator: ","))],\"sent_at_ms\":\(nowMs())," +
               "\"sdk_telemetry\":{\"retry_count\":\(retryCount),\"queue_size\":\(queueSize),\"dropped\":\(droppedCount)}," +
               "\"batch_id\":\"\(bid)\"}"
    }

    /// Wire size of the batch exactly as `postBatch` would put it on the socket —
    /// gzipped above the threshold, because the server's ceiling applies to the
    /// compressed bytes it actually reads.
    private func wireByteLength(_ events: [String], _ retryCount: Int, _ queueSize: Int) -> Int {
        let raw = Data(buildBatchBody(events, retryCount, queueSize, batchId(events)).utf8)
        if events.count >= 10, let gz = gzipBody(raw) { return gz.count }
        return raw.count
    }

    /// How many events from the head of `events` fit under `maxWireBytes`.
    ///
    /// Without this, an over-size batch is a permanent silent outage exactly like the
    /// over-size COUNT was: the server 413s it, a terminal 4xx maps to `.drop`, the
    /// events are deleted, and the next flush rebuilds another too-large batch.
    /// Halving rather than stepping down one at a time bounds this to ~8 rebuilds
    /// even from a full 200-event batch.
    ///
    /// Returns at least 1. A single event that cannot fit alone is genuinely
    /// undeliverable, and returning 0 would park it at the head of the queue for
    /// ever — strictly worse than today. Sending it costs one wasted request, earns
    /// the same 413/.drop it already gets, and lets the queue drain; the server-side
    /// ingest counter is what makes that visible.
    private func wireFittingCount(_ events: [String], _ retryCount: Int, _ queueSize: Int) -> Int {
        // Fast path, and the one every healthy batch takes: if the UNCOMPRESSED body
        // already fits then the wire body must too — gzip only shrinks at these
        // sizes, and below the threshold the wire body IS the raw body. Skips
        // compressing twice per send.
        let rawSize = Data(buildBatchBody(events, retryCount, queueSize, batchId(events)).utf8).count
        if rawSize <= ReflectCore.maxWireBytes { return events.count }

        // Raw overflows, so compression decides. Re-measure for real: a batch that
        // gzips 5-10x still sends in full, and only a genuinely huge one halves.
        var n = events.count
        while n > 1 {
            if wireByteLength(Array(events.prefix(n)), retryCount, queueSize) <= ReflectCore.maxWireBytes {
                return n
            }
            n /= 2
        }
        return 1
    }

    /// SHA-256 over the comma-joined, sorted event_ids; first 16 bytes hex (128-bit)
    /// — a content fingerprint stable across retries (matches Unity's BatchId).
    private func batchId(_ events: [String]) -> String {
        let ids = events.compactMap { extractEventId($0) }.sorted()
        let digest = SHA256.hash(data: Data(ids.joined(separator: ",").utf8))
        return digest.prefix(16).map { String(format: "%02x", $0) }.joined()
    }

    private func extractEventId(_ eventJson: String) -> String? {
        guard let r = eventJson.range(of: "\"event_id\":\"") else { return nil }
        let rest = eventJson[r.upperBound...]
        guard let end = rest.firstIndex(of: "\"") else { return nil }
        return String(rest[..<end])
    }

    /// Caller holds queueLock. The durable row is always coarse; a raw
    /// install-referrer override exists only for the first 30 seconds of the
    /// current process so the normal online install keeps deterministic credit.
    private func transientPayloadForImmediateSendLocked(
        _ persistedPayload: String
    ) -> String {
        guard let eventId = extractEventId(persistedPayload),
              let current = transientAttributionPayloads[eventId] else {
            return persistedPayload
        }
        if current.expiresAtMs <= nowMs() {
            transientAttributionPayloads.removeValue(forKey: eventId)
            return persistedPayload
        }
        return current.payload
    }

    private func monotonicMs() -> Int64 { return Int64(ProcessInfo.processInfo.systemUptime * 1000) }

    private func parseRetryAfter(_ header: String?) -> Int64 {
        guard let h = header?.trimmingCharacters(in: .whitespaces), let secs = Int64(h) else { return 0 }
        return min(secs * 1000, ReflectCore.maxBackoffMs)
    }

    /// Parse server pacing directives from a response body. Returns (0, 0) on an
    /// empty/garbage body or absent `directives` → local backoff fallback. Every
    /// value clamped to [0, 1h] so a poisoned directive can't wedge the queue.
    private func parseDirectives(_ data: Data?) -> (retryIn: Int64, continueIn: Int64) {
        guard let data = data, !data.isEmpty,
              let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let d = obj["directives"] as? [String: Any] else { return (0, 0) }
        func clamp(_ key: String) -> Int64 {
            guard let n = d[key] as? NSNumber else { return 0 }
            let v = n.int64Value
            return v <= 0 ? 0 : min(v, ReflectCore.maxBackoffMs)
        }
        return (clamp("retry_in"), clamp("continue_in"))
    }

    private func registerConnectivityDrain() {
        #if canImport(Network)
        if pathMonitorStarted { return }
        pathMonitorStarted = true
        let monitor = NWPathMonitor()
        monitor.pathUpdateHandler = { [weak self] path in
            if path.status == .satisfied {
                self?.drainBackoffMs = 0
                self?.nextSendAllowedMs = 0   // reconnect preempts any server pacing/backoff
                self?.scheduleDrain(0)   // connectivity returned — drain now
                self?.retryPendingSignals()   // X1: re-attempt a deferred DL / attribution the offline install missed
            }
        }
        monitor.start(queue: DispatchQueue.global(qos: .utility))
        #endif
    }

    // MARK: - Server-resolved data: deferred deep link + attribution (mirrors Unity)

    /// Resolve a deferred deep link via POST /deeplink/resolve. Emits the
    /// resolved link on the onDeepLink stream with isDeferred = true.
    private func resolveDeferredDeepLink(permit: PrivacyTransportGate.Permit) {
        if !trackingEnabled || consentState == "denied" || localOnly || !transportGate.isValid(permit) { return }
        guard transportGate.runIfValid(permit, {
            setPendingSignal("reflect_pending_deferred_dl", true)
        }) else { return }
        let bodyDict: [String: Any] = ["app_key": appKey, "install_uuid": installUuid]
        guard let bodyData = try? JSONSerialization.data(withJSONObject: bodyDict),
              let body = String(data: bodyData, encoding: .utf8) else { return }
        let sig = (signingSecret?.isEmpty == false) ? hmacHex(body, signingSecret!) : nil
        guard let resp = httpJson("\(baseUrl)/deeplink/resolve", "POST", body, sig, permit: permit) else { return }
        guard let data = resp.data(using: .utf8),
              let obj = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] else { return }
        var callbackMap: [String: Any]?
        var reportProps: [String: Any]?
        guard transportGate.runIfValid(permit, {
            setPendingSignal("reflect_pending_deferred_dl", false)
            guard let path = obj["deep_link_path"] as? String, !path.isEmpty else { return }
            var params: [String: String] = [:]
            if let p = obj["deep_link_params"] as? [String: Any] {
                for (k, v) in p { params[k] = "\(v)" }
            }
            var map: [String: Any] = ["url": path, "path": path, "isDeferred": true, "params": params]
            if let c = params["click_id"] { map["clickId"] = c }
            if let c = params["campaign"] { map["campaign"] = c }
            if let pr = params["partner"] { map["partner"] = pr }
            let retainedPath = AttributionRetention.urlWithoutQueryOrFragment(path)
            lastDeepLink = retainedPath
            callbackMap = map
            if retainedPath != lastDeepLinkReported {
                lastDeepLinkReported = retainedPath
                reportProps = deepLinkOpenedProperties(path, params, "deferred")
            }
        }) else { return }
        if let callbackMap = callbackMap { emitDeepLink(callbackMap, acceptedPermit: permit) }
        if let reportProps = reportProps {
            emitJsonEvent("deep_link_opened", reportProps, nil, acceptedPermit: permit)
        }
    }

    /// Poll GET /attribution/check (HMAC-signed query) once per session. On a
    /// newer attribution row, persist it (so getAttribution works) + the
    /// watermark, and push onAttributionChanged. Needs signingSecret.
    private func attributionCheck(
        forceFresh: Bool = false,
        attempt: Int = 0,
        askAttempt: Int = 0,
        permit: PrivacyTransportGate.Permit
    ) {
        if !trackingEnabled || consentState == "denied" || localOnly || !transportGate.isValid(permit) { return }
        guard let secret = signingSecret, !secret.isEmpty else { return }
        if attempt == 0 && !transportGate.runIfValid(permit, {
            setPendingSignal("reflect_pending_attr", true)
        }) { return }
        let encoded = installUuid.addingPercentEncoding(withAllowedCharacters: .alphanumerics) ?? installUuid
        let defaults = UserDefaults.standard
        // SDKs released before the retention contract cached clickId without
        // its expiry. Fetch that row once with since=0 so an upgraded client
        // can enforce the boundary while offline.
        let needsExpiryBootstrap =
            defaults.object(forKey: "reflect_attr_click_context_expires_at_ms") == nil &&
            defaults.string(forKey: "reflect_attribution_json") != nil
        let since = (forceFresh || needsExpiryBootstrap) ? 0 : lastAttributionCheckMs
        let query = "install_uuid=\(encoded)&since=\(since)"
        guard let resp = httpJson("\(baseUrl)/attribution/check?\(query)", "GET", nil,
                                  hmacHex(query, secret), permit: permit) else {
            // Transient failure → retry {2s,5s} up to 3 attempts (Unity parity), so an
            // attribution that resolves while we were offline at install isn't missed.
            if attempt < 2 {
                let delayMs = attempt == 0 ? 2000 : 5000
                DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + .milliseconds(delayMs)) { [weak self] in
                    self?.attributionCheck(forceFresh: forceFresh, attempt: attempt + 1,
                                           askAttempt: askAttempt, permit: permit)
                }
            }
            return   // network failure → stay pending (X1); retried on connectivity/next launch
        }
        guard let data = resp.data(using: .utf8),
              let obj = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] else { return }
        guard (obj["changed"] as? Bool) == true, let d = obj["data"] as? [String: Any] else {
            // A3: honor a server-driven `ask_in` — attribution not resolved yet; re-poll
            // that far out, bounded so an organic install doesn't poll forever.
            _ = transportGate.runIfValid(permit) {
                setPendingSignal("reflect_pending_attr", false)
                if let dir = obj["directives"] as? [String: Any] {
                    let askIn = (dir["ask_in"] as? Int) ?? Int((dir["ask_in"] as? Double) ?? 0)
                    if askIn > 0, askIn <= Int(ReflectCore.maxBackoffMs), askAttempt < ReflectCore.maxAskInRepolls {
                        DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + .milliseconds(askIn)) { [weak self] in
                            self?.attributionCheck(forceFresh: true, attempt: 0,
                                                   askAttempt: askAttempt + 1, permit: permit)
                        }
                    }
                }
            }
            return
        }
        var callbackMap: [String: Any]?
        guard transportGate.runIfValid(permit, {
            setPendingSignal("reflect_pending_attr", false)
            let attributedAt = (d["attributed_at_ms"] as? Int64)
                ?? Int64((d["attributed_at_ms"] as? Double) ?? 0)
            let revisionAt = (d["attribution_revision_ms"] as? Int64)
                ?? Int64((d["attribution_revision_ms"] as? Double) ?? Double(attributedAt))
            if revisionAt > lastAttributionCheckMs {
                lastAttributionCheckMs = revisionAt
            }
            let clickContextExpiresAt = max(
                0,
                (d["click_context_expires_at_ms"] as? Int64)
                    ?? Int64((d["click_context_expires_at_ms"] as? Double) ?? 0)
            )
            var map: [String: Any] = [:]
            if let t = d["attribution_type"] as? String { map["type"] = t }
            if let p = d["partner_slug"] as? String { map["partner"] = p }
            if let c = d["campaign_name"] as? String { map["campaign"] = c }
            if clickContextExpiresAt > 0,
               let ci = d["click_id"] as? String,
               !AttributionRetention.isExpired(expiresAtMs: clickContextExpiresAt) {
                map["clickId"] = ci
            }
            // clickId is callback-only. Persist only coarse attribution fields:
            // an exact id/expiry cannot be physically removed while iOS keeps an
            // app suspended or the user never launches it again.
            var persistentMap = map
            persistentMap.removeValue(forKey: "clickId")
            if let pdata = try? JSONSerialization.data(withJSONObject: persistentMap, options: .sortedKeys),
               let pstr = String(data: pdata, encoding: .utf8) {
                defaults.set(Int(lastAttributionCheckMs), forKey: "reflect_attr_watermark")
                defaults.removeObject(forKey: "reflect_attr_click_context_expires_at_ms")
                defaults.set(pstr, forKey: "reflect_attribution_json")
            }
            // changed=true already denotes a newer server revision. Deliver its
            // volatile clickId even if persisted coarse fields are identical.
            callbackMap = map
        }) else { return }
        if let callbackMap = callbackMap { emitAttribution(callbackMap, acceptedPermit: permit) }
    }

    private func emitDeepLink(_ map: [String: Any], acceptedPermit: PrivacyTransportGate.Permit? = nil) {
        let wasInitialized = initialized
        let permit = acceptedPermit ?? (wasInitialized ? transportGate.permit() : nil)
        DispatchQueue.main.async {
            if wasInitialized {
                guard let permit = permit, self.transportGate.isValid(permit) else { return }
            } else {
                guard self.mutateMeasurementState({}).applied else { return }
            }
            if let l = self.listener {
                l.onDeepLink(map)
            } else {
                self.pendingDeferredDeepLink = self.retainedDeepLinkCallback(map)
            }
        }
    }

    private func retainedDeepLinkCallback(_ map: [String: Any]) -> [String: Any] {
        var retained = map
        if let url = retained["url"] as? String {
            retained["url"] = AttributionRetention.urlWithoutQueryOrFragment(url)
        }
        if let path = retained["path"] as? String {
            retained["path"] = AttributionRetention.urlWithoutQueryOrFragment(path)
        }
        for key in [
            "clickId", "click_id", "ext_click_id", "clickid",
            "sub1", "sub2", "sub3", "sub4", "sub5",
        ] {
            retained.removeValue(forKey: key)
        }
        if let params = retained["params"] as? [String: Any] {
            retained["params"] = params.filter { $0.key == "campaign" || $0.key == "partner" }
        } else if retained["params"] != nil {
            retained["params"] = [String: Any]()
        }
        return retained
    }

    private func retainedAttributionCallback(_ map: [String: Any]) -> [String: Any] {
        var retained = map
        for key in [
            "clickId", "click_id", "ext_click_id", "clickid",
            "sub1", "sub2", "sub3", "sub4", "sub5",
        ] {
            retained.removeValue(forKey: key)
        }
        return retained
    }

    private func emitAttribution(_ map: [String: Any], acceptedPermit: PrivacyTransportGate.Permit? = nil) {
        guard let permit = acceptedPermit ?? transportGate.permit() else { return }
        DispatchQueue.main.async {
            guard self.transportGate.isValid(permit) else { return }
            if let l = self.listener {
                l.onAttribution(map)
            } else {
                self.pendingAttribution = self.retainedAttributionCallback(map)
            }
        }
    }

    /// GET/POST JSON, blocking via semaphore on the calling background queue.
    /// Returns the response body on 2xx, else nil.
    private func httpJson(
        _ urlStr: String,
        _ method: String,
        _ body: String?,
        _ signature: String?,
        permit: PrivacyTransportGate.Permit
    ) -> String? {
        if offlineMode || localOnly { return nil }
        guard transportGate.isValid(permit) else { return nil }
        guard let url = URL(string: urlStr) else { return nil }
        var req = URLRequest(url: url)
        req.httpMethod = method
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.setValue(hostSdkVersion, forHTTPHeaderField: "X-Reflect-Sdk")
        if !appKey.isEmpty { req.setValue(appKey, forHTTPHeaderField: "X-Reflect-App-Key") }
        if let ck = companyKey, !ck.isEmpty { req.setValue(ck, forHTTPHeaderField: "X-Reflect-Company-Key") }
        if let s = signature { req.setValue(s, forHTTPHeaderField: "X-Reflect-Signature") }
        if let b = body { req.httpBody = b.data(using: .utf8) }
        req.timeoutInterval = 15
        var out: String?
        let sem = DispatchSemaphore(value: 0)
        let task = URLSession.shared.dataTask(with: req) { data, response, _ in
            defer { sem.signal() }
            guard let http = response as? HTTPURLResponse,
                  (200..<300).contains(http.statusCode), let data = data else { return }
            out = String(data: data, encoding: .utf8)
        }
        guard transportGate.register(task, permit: permit, cancel: { task.cancel() }) else {
            task.cancel()
            return nil
        }
        task.resume()
        sem.wait()
        transportGate.unregister(task)
        return transportGate.isValid(permit) ? out : nil
    }

    private func hmacHex(_ data: String, _ secret: String) -> String {
        let key = SymmetricKey(data: Data(secret.utf8))
        let mac = HMAC<SHA256>.authenticationCode(for: Data(data.utf8), using: key)
        return mac.map { String(format: "%02x", $0) }.joined()
    }

    /// HMAC-SHA256 over RAW BYTES (used to sign the GZIPPED wire bytes — the server
    /// verifies the sig over the wire bytes, THEN decompresses).
    private func hmacHexData(_ data: Data, _ secret: String) -> String {
        let key = SymmetricKey(data: Data(secret.utf8))
        let mac = HMAC<SHA256>.authenticationCode(for: data, using: key)
        return mac.map { String(format: "%02x", $0) }.joined()
    }

    /// gzip a payload (Unity/Android parity for batches ≥ GZIP_THRESHOLD events).
    /// Apple's Compression COMPRESSION_ZLIB is RAW DEFLATE (RFC 1951), so we wrap it
    /// in gzip framing (10-byte header + deflate + CRC32 + ISIZE) → a valid
    /// Content-Encoding: gzip the server's DecompressionStream("gzip") accepts.
    private func gzipBody(_ data: Data) -> Data? {
        if data.isEmpty { return nil }
        let cap = data.count + 128
        var dst = Data(count: cap)
        let n: Int = dst.withUnsafeMutableBytes { d in
            data.withUnsafeBytes { s in
                compression_encode_buffer(d.bindMemory(to: UInt8.self).baseAddress!, cap,
                                          s.bindMemory(to: UInt8.self).baseAddress!, data.count,
                                          nil, COMPRESSION_ZLIB)
            }
        }
        if n == 0 { return nil }   // incompressible / didn't fit → caller sends uncompressed
        var out = Data([0x1f, 0x8b, 0x08, 0x00, 0, 0, 0, 0, 0x00, 0xff])   // gzip header
        out.append(dst.prefix(n))
        var crc = crc32(data).littleEndian
        withUnsafeBytes(of: &crc) { out.append(contentsOf: $0) }
        var isize = UInt32(truncatingIfNeeded: data.count).littleEndian
        withUnsafeBytes(of: &isize) { out.append(contentsOf: $0) }
        return out
    }

    private func crc32(_ data: Data) -> UInt32 {
        var crc: UInt32 = 0xffffffff
        for b in data {
            crc ^= UInt32(b)
            for _ in 0..<8 { crc = (crc & 1) != 0 ? (crc >> 1) ^ 0xEDB88320 : crc >> 1 }
        }
        return ~crc
    }

    /// JSON-escape into a QUOTED literal; nil → empty-string "" (Unity
    /// EscapeJsonString parity — never JSON null). For hand-built bodies whose
    /// exact bytes are HMAC-signed, so key ORDER + escaping must match Unity.
    private func jq(_ s: String?) -> String {
        let v = s ?? ""
        var out = "\""
        for c in v {
            switch c {
            case "\\": out += "\\\\"
            case "\"": out += "\\\""
            case "\n": out += "\\n"
            case "\r": out += "\\r"
            case "\t": out += "\\t"
            default:   out.append(c)
            }
        }
        return out + "\""
    }

    /// Verify a purchase receipt server-side (Unity HttpDispatcher.VerifyPurchase
    /// parity). Body is a HAND-BUILT string in Unity's exact key order, signed over
    /// the raw bytes. Returns {status, code, message}.
    private func verifyPurchaseHttp(
        _ args: [String: Any]?,
        permit: PrivacyTransportGate.Permit?
    ) -> [String: Any] {
        let productId     = args?["productId"] as? String ?? ""
        let transactionId = args?["transactionId"] as? String ?? ""
        let purchaseToken = args?["purchaseToken"] as? String ?? ""
        let receiptData   = args?["receiptData"] as? String ?? ""
        if baseUrl.isEmpty { return ["status": "unknown", "code": 0, "message": "debug_mode"] }
        guard let permit = permit, trackingEnabled, consentState != "denied",
              transportGate.isValid(permit) else {
            return ["status": "failed", "code": 0, "message": "privacy_blocked"]
        }
        let body = "{"
            + "\"app_key\":"        + jq(appKey)        + ","
            + "\"install_uuid\":"   + jq(installUuid)   + ","
            + "\"product_id\":"     + jq(productId)     + ","
            + "\"transaction_id\":" + jq(transactionId) + ","
            + "\"purchase_token\":" + jq(purchaseToken) + ","
            + "\"receipt_data\":"   + jq(receiptData)
            + "}"
        let secret = (signingSecret?.isEmpty == false) ? signingSecret : nil
        let sig = secret.map { hmacHex(body, $0) }
        guard let resp = httpJson("\(baseUrl)/purchase/verify", "POST", body, sig, permit: permit) else {
            return ["status": "failed", "code": 0, "message": "request_failed"]
        }
        guard let data = resp.data(using: .utf8),
              let obj = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] else {
            return ["status": "failed", "code": 0, "message": "bad_response"]
        }
        return [
            "status": (obj["status"] as? String) ?? "unknown",
            "code": (obj["code"] as? NSNumber)?.intValue ?? 0,
            "message": (obj["message"] as? String) ?? "",
        ]
    }

    // MARK: - Device snapshot

    private func buildDevice() -> [String: Any] {
        // hardwareModel()/uname + Locale/TimeZone/Bundle are thread-safe; the
        // UIKit-derived values come from the main-thread snapshot (snapshotUIKit).
        var device: [String: Any] = [
            "os": "ios",
            "os_version": snapSystemVersion,
            "device_model": hardwareModel(),
            "device_manufacturer": "Apple",
            "device_brand": "Apple",
            "device_type": snapDeviceType,
            "locale": Locale.current.identifier,
            "language": Locale.current.languageCode ?? "",
            "timezone": TimeZone.current.identifier,
            "tz_offset_min": TimeZone.current.secondsFromGMT() / 60,
            "is_emulator": isSimulator(),
            "cpu_arch": cpuArch(),
            "screen_width": snapScreenW,
            "screen_height": snapScreenH,
            "screen_density": snapScreenDensityDpi,   // DPI (scale*160), matches the rest of the fleet
            "total_ram_mb": Int(ProcessInfo.processInfo.physicalMemory / (1024 * 1024)),
            "connection_type": connectionType(),      // were all missing on Flutter-iOS
            "vpn_detected": vpnDetected(),
            "is_rooted": isJailbrokenCached,
            // Schema parity with Android (Unity emits these on iOS too): api_level = iOS
            // major version; mock_location_enabled is always false (no iOS mock-location API).
            "api_level": Int(snapSystemVersion.split(separator: ".").first.map(String.init) ?? "0") ?? 0,
            "mock_location_enabled": false,
            "first_install_time": firstInstallMs,     // persisted on first run (Unity-iOS hardcodes 0)
            "last_update_time": bundleModifiedMs(),
        ]
        if let region = Locale.current.regionCode { device["country"] = region }
        #if canImport(CoreTelephony)
        // mcc/mnc where still exposed (deprecated iOS 16+, returns 65535 then).
        if let carrier = CTTelephonyNetworkInfo().subscriberCellularProvider {
            if let name = carrier.carrierName, !name.isEmpty { device["carrier"] = name }
            if let mcc = carrier.mobileCountryCode, mcc != "65535" { device["carrier_mcc"] = mcc }   // Unity wire key (was "mcc")
            if let mnc = carrier.mobileNetworkCode, mnc != "65535" { device["carrier_mnc"] = mnc }   // Unity wire key (was "mnc")
        }
        #endif

        if let info = Bundle.main.infoDictionary {
            if let v = info["CFBundleShortVersionString"] as? String { device["app_version"] = v }
            if let b = info["CFBundleVersion"] as? String { device["app_version_code"] = Int(b) ?? b }
        }
        if let bundleId = Bundle.main.bundleIdentifier { device["app_bundle_id"] = bundleId }
        device["install_source"] = "app_store"

        // IDFA from the main-thread snapshot (refreshIdfa); never read ATT /
        // ASIdentifierManager off-thread here.
        if advertisingConsent, consentState != "denied", let idfa = snapIdfa {
            device["idfa"] = idfa
        }
        // lat_enabled — limited-ad-tracking (Unity-iOS parity). Limited == no usable
        // IDFA AND the user has actually made an ATT decision (idfa nil while
        // not_determined just means "not asked yet", not "limited").
        if consentState != "denied" {
            let att = cachedAttStatus ?? "not_determined"
            let hasIdfa = (snapIdfa != nil && snapIdfa != "00000000-0000-0000-0000-000000000000")
            device["lat_enabled"] = (!hasIdfa && att != "not_determined")
        }
        // idfv is a quasi-identifier — suppress on consent denial.
        if consentState != "denied", let idfv = snapIdfv {
            device["idfv"] = idfv
        }
        return device
    }

    /// Hardware identifier, e.g. "iPhone15,2" — richer than UIDevice.model
    /// ("iPhone"), matching what Adjust/MMPs collect for device targeting.
    private func hardwareModel() -> String {
        var systemInfo = utsname()
        uname(&systemInfo)
        let mirror = Mirror(reflecting: systemInfo.machine)
        let id = mirror.children.reduce("") { acc, el in
            guard let value = el.value as? Int8, value != 0 else { return acc }
            return acc + String(UnicodeScalar(UInt8(value)))
        }
        return id.isEmpty ? UIDevice.current.model : id
    }

    private func isSimulator() -> Bool {
        #if targetEnvironment(simulator)
        return true
        #else
        return false
        #endif
    }

    private func cpuArch() -> String {
        #if arch(arm64)
        return "arm64"
        #elseif arch(x86_64)
        return "x86_64"
        #else
        return "unknown"
        #endif
    }

    /// Jailbreak detection (ported from the Unity ReflectBridge IsJailbroken).
    /// Only jailbreak-SPECIFIC artifacts count as evidence. The previous
    /// generation also probed `/bin/bash`, `/usr/sbin/sshd`, `/etc/apt` and
    /// readability of the mobile user's `.GlobalPreferences.plist`; on recent
    /// stock iOS those fire universally (production data 2026-08: 100% of real
    /// iPhones reported `is_rooted=true`, stamping `fraud_flag=device_rooted`
    /// on every iOS attribution), so ambiguous system paths are no longer
    /// treated as evidence.
    /// Computed once per process: the answer cannot change mid-process, and
    /// re-running the write probe would cost one denied sandbox syscall per
    /// tracked event. Lazy init is safe here — buildDevice() runs on the
    /// SDK's serial queue, so first access is single-threaded.
    private lazy var isJailbrokenCached: Bool = computeIsJailbroken()

    private func computeIsJailbroken() -> Bool {
        #if targetEnvironment(simulator)
        return false
        #else
        let fm = FileManager.default
        // Rootful artifacts, plus the rootless bootstrap dir `/var/jb`:
        // modern (Dopamine/palera1n-class) jailbreaks relocate there because
        // the iOS 15+ root volume is a sealed SSV they cannot remount.
        for p in ["/Applications/Cydia.app", "/Applications/Sileo.app",
                  "/Applications/Zebra.app",
                  "/Library/MobileSubstrate/MobileSubstrate.dylib",
                  "/private/var/lib/apt/", "/var/jb"] {
            if fm.fileExists(atPath: p) { return true }
        }
        // Sandbox-escape write probe — honest scope: on iOS 15+ the sealed
        // root volume means this succeeds only on legacy ROOTFUL jailbreaks;
        // rootless ones are covered by the /var/jb artifact above. A stock
        // device fails here with a single denied syscall.
        let probe = "/private/reflect_jb_\(UUID().uuidString).txt"
        do {
            try "1".write(toFile: probe, atomically: false, encoding: .utf8)
            try? fm.removeItem(atPath: probe)
            return true
        } catch {
            return false
        }
        #endif
    }

    /// Active-interface connectivity (en* = wifi/wired, pdp_ip* = cellular).
    private func connectionType() -> String {
        var wifi = false, cell = false
        var ifaddr: UnsafeMutablePointer<ifaddrs>?
        if getifaddrs(&ifaddr) == 0 {
            var ptr = ifaddr
            while let p = ptr {
                let flags = p.pointee.ifa_flags
                let up = (flags & UInt32(IFF_UP)) != 0 && (flags & UInt32(IFF_RUNNING)) != 0
                let loopback = (flags & UInt32(IFF_LOOPBACK)) != 0
                if up, !loopback, let a = p.pointee.ifa_addr {
                    let fam = a.pointee.sa_family
                    if fam == UInt8(AF_INET) || fam == UInt8(AF_INET6) {
                        let name = String(cString: p.pointee.ifa_name)
                        if name.hasPrefix("en") { wifi = true }
                        else if name.hasPrefix("pdp_ip") { cell = true }
                    }
                }
                ptr = p.pointee.ifa_next
            }
            freeifaddrs(ifaddr)
        }
        if wifi { return "wifi" }
        if cell { return "cellular" }
        return "none"
    }

    /// VPN via tunnelling interfaces (ppp/ipsec/tap/tun; utun* excluded as it's used
    /// by non-VPN system services). Ported from the Unity ReflectBridge.
    private func vpnDetected() -> Bool {
        var vpn = false
        var ifaddr: UnsafeMutablePointer<ifaddrs>?
        if getifaddrs(&ifaddr) == 0 {
            var ptr = ifaddr
            while let p = ptr {
                let flags = p.pointee.ifa_flags
                if (flags & UInt32(IFF_UP)) != 0, (flags & UInt32(IFF_RUNNING)) != 0 {
                    let name = String(cString: p.pointee.ifa_name)
                    if name.hasPrefix("ppp") || name.hasPrefix("ipsec") || name.hasPrefix("tap") || name.hasPrefix("tun") {
                        vpn = true; break
                    }
                }
                ptr = p.pointee.ifa_next
            }
            freeifaddrs(ifaddr)
        }
        return vpn
    }

    /// Bundle modification time as a last_update_time proxy (changes on app update).
    private func bundleModifiedMs() -> Int64 {
        let path = Bundle.main.executablePath ?? Bundle.main.bundlePath
        if let attrs = try? FileManager.default.attributesOfItem(atPath: path),
           let mod = attrs[.modificationDate] as? Date {
            return Int64(mod.timeIntervalSince1970 * 1000)
        }
        return firstInstallMs
    }

    // MARK: - Helpers

    private func appVersionName() -> String? {
        return Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String
    }

    /// ATT status as a wire string — returns the value cached on the main thread
    /// (snapshotUIKit + didBecomeActive), never reading ATT off-thread.
    private func attStatusString() -> String? { return cachedAttStatus }

    /// Read the (main-thread-only) UIKit/ATT values once and cache them so the
    /// background buildDevice never touches UIKit off-thread (UB / stale 0s).
    private func snapshotUIKit() {
        let bounds = UIScreen.main.bounds.size
        let scale = UIScreen.main.scale
        snapScreenW = Int(bounds.width * scale)
        snapScreenH = Int(bounds.height * scale)
        snapScreenDensityDpi = Int(scale * 160)
        snapDeviceType = UIDevice.current.userInterfaceIdiom == .pad ? "tablet" : "phone"
        snapSystemVersion = UIDevice.current.systemVersion
        snapIdfv = (trackingEnabled && consentState != "denied")
            ? UIDevice.current.identifierForVendor?.uuidString : nil
        uikitSnapshotted = true
        refreshAttStatus()
        refreshIdfa()
    }

    /// IDFA read on the main thread (ATT/ASIdentifierManager are read here, never
    /// in the background buildDevice). Refreshed on each activation since ATT auth
    /// can change after the prompt. buildDevice still gates on advertisingConsent.
    private func refreshIdfa() {
        guard trackingEnabled, consentState != "denied", advertisingConsent, !ffCoppa else {
            snapIdfa = nil
            return
        }
        if #available(iOS 14, *) {
            #if canImport(AppTrackingTransparency)
            snapIdfa = (ATTrackingManager.trackingAuthorizationStatus == .authorized)
                ? ASIdentifierManager.shared().advertisingIdentifier.uuidString : nil
            #endif
        } else {
            snapIdfa = ASIdentifierManager.shared().isAdvertisingTrackingEnabled
                ? ASIdentifierManager.shared().advertisingIdentifier.uuidString : nil
        }
    }

    private func refreshAttStatus() {
        #if canImport(AppTrackingTransparency)
        if #available(iOS 14, *) {
            let previous = cachedAttStatus
            switch ATTrackingManager.trackingAuthorizationStatus {
            case .authorized:    cachedAttStatus = "authorized"
            case .denied:        cachedAttStatus = "denied"
            case .restricted:    cachedAttStatus = "restricted"
            case .notDetermined: cachedAttStatus = "not_determined"
            @unknown default:    cachedAttStatus = "not_determined"
            }
            // The ATT answer is the only event that lifts tracking-domain
            // blocking, and it produces no NWPath transition — so unless it
            // reopens transport here, a first-install batch that was refused
            // behind the gate stays queued until the user relaunches. Every
            // caller of refreshAttStatus (the prompt completion, auto-ATT at
            // init, didBecomeActive, consent/ad-consent grants) routes through
            // this one hook.
            if AttTransportPolicy.isPromptResolution(previous: previous, current: cachedAttStatus) {
                attTrackingDecisionResolved()
            }
        }
        #endif
    }

    /// Reopen transport the moment the ATT prompt is answered.
    ///
    /// Clears only the gate that the blocked attempts installed. A grant lifts
    /// the tracking-domain refusal outright; a denial does not, but it does end
    /// the wait, so the queue returns to ordinary backoff instead of parking on
    /// a decision that has already been made.
    private func attTrackingDecisionResolved() {
        guard initialized, trackingEnabled, consentState != "denied" else { return }
        drainBackoffMs = 0
        headRetryCount = 0
        nextSendAllowedMs = 0
        clearPersistedBackoff()
        scheduleDrain(0)
    }

    private func nowMs() -> Int64 { return Int64(Date().timeIntervalSince1970 * 1000) }

    // MARK: - Session manager (feeds aggregates_sessions)

    // All of these run on the serial `queue`. A session spans brief fg/bg flips
    // (subsessions); it ends only after a > sessionGapMs background or is recovered
    // on the next launch if the process died mid-session.

    @discardableResult
    private func sessionMutation(_ permit: PrivacyTransportGate.Permit, _ action: () -> Void) -> Bool {
        sessionStateLock.lock(); defer { sessionStateLock.unlock() }
        guard trackingEnabled, consentState != "denied", transportGate.isValid(permit) else { return false }
        action()
        return true
    }

    private func startSession(intervalMs: Int64 = -1, permit: PrivacyTransportGate.Permit) {
        sessionMutation(permit) {
        sessionCount += 1
        sessionActiveMs = 0
        subsessionCount = 1          // the opening foreground is subsession 1 (Unity parity)
        sessionOpen = true
        sessionId = UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased()
        sessionStartElapsed = monotonicMs()
        let d = UserDefaults.standard
        d.set(Int(sessionCount), forKey: "reflect_session_count")
        d.set(0, forKey: "reflect_session_active_ms")
        d.set(1, forKey: "reflect_subsession_count")
        d.set(true, forKey: "reflect_session_open")
        d.set(sessionId, forKey: "reflect_session_id")
        var props: [String: Any] = ["session_count": sessionCount, "subsession_count": subsessionCount]
        if intervalMs >= 0 { props["last_interval_ms"] = intervalMs }   // gap since last activity (Unity parity)
        emitJsonEvent("session_start", props, nil, acceptedPermit: permit)
        if sessionCount > 1 {
            DispatchQueue.global(qos: .utility).async { [weak self] in self?.attributionCheck(permit: permit) }
        }
        }
    }

    private func bankActive(_ permit: PrivacyTransportGate.Permit) {
        sessionMutation(permit) {
            if sessionStartElapsed > 0 {
                sessionActiveMs += max(0, monotonicMs() - sessionStartElapsed)
                sessionStartElapsed = 0
                UserDefaults.standard.set(Int(sessionActiveMs), forKey: "reflect_session_active_ms")
            }
        }
    }

    /// Foreground heartbeat (Unity parity): bank+persist the active stint WITHOUT
    /// stopping the timer, so a crash mid-foreground loses ≤30s of session length.
    private func heartbeatBank(_ permit: PrivacyTransportGate.Permit) {
        sessionMutation(permit) {
            if sessionStartElapsed > 0 {
                let now = monotonicMs()
                sessionActiveMs += max(0, now - sessionStartElapsed)
                sessionStartElapsed = now
                UserDefaults.standard.set(Int(sessionActiveMs), forKey: "reflect_session_active_ms")
                UserDefaults.standard.set(Int(nowMs()), forKey: "reflect_last_activity_wall")
            }
        }
    }
    private func startHeartbeat(_ permit: PrivacyTransportGate.Permit) {
        sessionMutation(permit) {
            stopHeartbeat()
            let t = DispatchSource.makeTimerSource(queue: .main)
            t.schedule(deadline: .now() + 30, repeating: 30)
            t.setEventHandler { [weak self] in self?.heartbeatBank(permit) }
            heartbeatTimer = t
            t.resume()
        }
    }
    private func stopHeartbeat() { heartbeatTimer?.cancel(); heartbeatTimer = nil }

    private func emitSessionEnd(_ permit: PrivacyTransportGate.Permit) {
        sessionMutation(permit) {
            bankActive(permit)
            emitJsonEvent("session_end",
                ["session_length_ms": sessionActiveMs, "session_count": sessionCount, "subsession_count": subsessionCount],
                nil, acceptedPermit: permit)
            sessionOpen = false
            sessionActiveMs = 0
            let d = UserDefaults.standard
            d.set(false, forKey: "reflect_session_open")
            d.set(0, forKey: "reflect_session_active_ms")
        }
    }

    /// If a prior process died with a session open, emit its banked length now.
    private func recoverInterruptedSession(_ crossKillGapMs: Int64 = -1,
                                           permit: PrivacyTransportGate.Permit) {
        // End the interrupted session ONLY if the cross-kill gap exceeded the threshold
        // (or is unknown). A within-threshold relaunch keeps it OPEN so onForeground
        // continues it as a subsession — no phantom extra session (Unity parity).
        if sessionOpen && (crossKillGapMs < 0 || crossKillGapMs > Int64(sessionThresholdMs)) {
            emitSessionEnd(permit)
        }
    }

    private func onForeground(_ crossKillGapMs: Int64 = -1, permit: PrivacyTransportGate.Permit) {
        sessionMutation(permit) {
        let now = monotonicMs()
        // gap=0 when never backgrounded this process (the launch foreground is a
        // continuation of the just-started session, not a 30-min-gap new one).
        let hasPrior = lastBackgroundElapsed > 0
        // Cold launch after a kill → use the persisted WALL-CLOCK gap (monotonic reset).
        let gap: Int64 = crossKillGapMs >= 0 ? crossKillGapMs : (hasPrior ? now - lastBackgroundElapsed : 0)
        if !sessionOpen {
            startSession(intervalMs: (crossKillGapMs >= 0 || hasPrior) ? gap : -1, permit: permit)
        } else if gap > Int64(sessionThresholdMs) {
            emitSessionEnd(permit); startSession(intervalMs: gap, permit: permit)
        } else if gap > ReflectCore.subsessionFloorMs {
            subsessionCount += 1
            UserDefaults.standard.set(Int(subsessionCount), forKey: "reflect_subsession_count")
            sessionStartElapsed = now
        } else {
            sessionStartElapsed = now
        }
        startHeartbeat(permit)   // periodically bank+persist foreground time (crash granularity)
        UserDefaults.standard.set(Int(nowMs()), forKey: "reflect_last_activity_wall")   // refresh cross-kill anchor
        }
    }

    private func onBackground(_ permit: PrivacyTransportGate.Permit) {
        sessionMutation(permit) {
            stopHeartbeat()
            bankActive(permit)
            lastBackgroundElapsed = monotonicMs()
            UserDefaults.standard.set(Int(nowMs()), forKey: "reflect_last_activity_wall")
        }
    }

    /// Enable/disable measurement. Disable is synchronous and authoritative:
    /// there is no final analytics event after the caller asks transport to stop.
    private func setTrackingEnabled(_ enabled: Bool) -> Bool {
        // Reopening before initialization could clear a durable suppression latch
        // without a validated app configuration or initialized transport.
        if enabled && !initialized { return false }
        if enabled == trackingEnabled {
            if enabled { return true }
            transportGate.block()
            let tombstonePersisted = privacyTombstone.update(trackingSuppressed: true)
            UserDefaults.standard.set(true, forKey: "reflect_suppressed")
            let defaultsPersisted = UserDefaults.standard.synchronize()
            clearEventQueue()
            sessionStateLock.lock()
            sessionOpen = false
            sessionStartElapsed = 0
            stopHeartbeat()
            UserDefaults.standard.set(false, forKey: "reflect_session_open")
            sessionStateLock.unlock()
            return tombstonePersisted || defaultsPersisted
        }
        if enabled {
            UserDefaults.standard.removeObject(forKey: "reflect_suppressed")
            guard UserDefaults.standard.synchronize() else { return false }
            guard privacyTombstone.update(trackingSuppressed: false) else {
                UserDefaults.standard.set(true, forKey: "reflect_suppressed")
                _ = UserDefaults.standard.synchronize()
                _ = privacyTombstone.update(trackingSuppressed: true)
                return false
            }
            trackingEnabled = true
            activateAfterPrivacyGate()
        } else {
            trackingEnabled = false
            transportGate.block()
            let tombstonePersisted = privacyTombstone.update(trackingSuppressed: true)
            UserDefaults.standard.set(true, forKey: "reflect_suppressed")
            let defaultsPersisted = UserDefaults.standard.synchronize()
            clearEventQueue()
            sessionStateLock.lock()
            sessionOpen = false
            sessionStartElapsed = 0
            stopHeartbeat()
            UserDefaults.standard.set(false, forKey: "reflect_session_open")
            sessionStateLock.unlock()
            guard tombstonePersisted || defaultsPersisted else { return false }
        }
        return true
    }

    /// Foreground/background observers drive both is_foreground and the session
    /// manager. ATT status is refreshed on each activation (it changes after the
    /// ATT prompt). Observers registered once at init on the main thread.
    private func registerForegroundObservers() {
        let nc = NotificationCenter.default
        nc.addObserver(forName: UIApplication.didBecomeActiveNotification, object: nil, queue: .main) { [weak self] _ in
            guard let self = self else { return }
            self.isForegroundState = true
            self.refreshAttStatus()
            self.refreshIdfa()
            guard let permit = self.transportGate.permit() else { return }
            self.queue.addOperation { self.onForeground(permit: permit) }   // session state on the serial queue
        }
        nc.addObserver(forName: UIApplication.willResignActiveNotification, object: nil, queue: .main) { [weak self] _ in
            self?.isForegroundState = false
        }
        nc.addObserver(forName: UIApplication.didEnterBackgroundNotification, object: nil, queue: .main) { [weak self] _ in
            guard let self = self else { return }
            self.isForegroundState = false
            guard let permit = self.transportGate.permit() else { return }
            self.queue.addOperation { self.onBackground(permit) }
        }
    }

    private func getOrCreateInstallUuid() -> String {
        let defaults = UserDefaults.standard
        if let uuid = defaults.string(forKey: "reflect_install_uuid") { return uuid }
        let uuid = UUID().uuidString.replacingOccurrences(of: "-", with: "")
        defaults.set(uuid, forKey: "reflect_install_uuid")
        return uuid
    }

    private func log(_ msg: String) {
        if debug { print("[Reflect] \(msg)") }
    }
}
