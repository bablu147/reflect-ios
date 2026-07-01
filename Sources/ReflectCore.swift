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
    private static let sdkVersion = "flutter-1.7.0"
    private static let sessionGapMs = 30 * 60 * 1000   // new session after 30 min in bg
    private static let subsessionFloorMs: Int64 = 1000 // sub-second fg flips aren't subsessions
    // Durable event queue (Adjust-style): persist before send, drain head-first,
    // delete only on 2xx/permanent-4xx, retry with backoff otherwise.
    private static let queueFileName = "reflect_queue.jsonl"
    private static let maxQueue = 1000
    private static let batchSize = 50   // max events per HTTP request (Unity parity)
    private static let baseBackoffMs: Int64 = 1000
    private static let maxBackoffMs: Int64 = 3_600_000
    private static let maxAskInRepolls = 3   // A3: bound server-driven attribution re-polls per session

    private enum SendResult { case success, retry, drop }

    private var appKey = ""
    private var companyKey: String?
    private var baseUrl = "https://api.reflect.cloud"
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

    // Persist global properties + partner parameters (Adjust parity): otherwise a dev who
    // sets them once at launch loses them on cold start → partner-forwarded params
    // silently vanish on the install/first-session events that matter most for attribution.
    private func persistGlobalProps() {
        globalLock.lock(); let snap = globalProperties; globalLock.unlock()
        if JSONSerialization.isValidJSONObject(snap),
           let d = try? JSONSerialization.data(withJSONObject: snap),
           let s = String(data: d, encoding: .utf8) {
            UserDefaults.standard.set(s, forKey: "reflect_global_props")
        }
    }
    private func persistPartnerParams() {
        globalLock.lock(); let snap = partnerParameters; globalLock.unlock()
        if let d = try? JSONSerialization.data(withJSONObject: snap),
           let s = String(data: d, encoding: .utf8) {
            UserDefaults.standard.set(s, forKey: "reflect_partner_params")
        }
    }
    private func restoreParams() {
        if let s = UserDefaults.standard.string(forKey: "reflect_global_props"), let d = s.data(using: .utf8),
           let o = (try? JSONSerialization.jsonObject(with: d)) as? [String: Any] {
            globalLock.lock(); globalProperties = o; globalLock.unlock()
        }
        if let s = UserDefaults.standard.string(forKey: "reflect_partner_params"), let d = s.data(using: .utf8),
           let o = (try? JSONSerialization.jsonObject(with: d)) as? [String: String] {
            globalLock.lock(); partnerParameters = o; globalLock.unlock()
        }
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
            DispatchQueue.global(qos: .utility).async { [weak self] in self?.resolveDeferredDeepLink() }
        }
        if UserDefaults.standard.bool(forKey: "reflect_pending_attr") {
            DispatchQueue.global(qos: .utility).async { [weak self] in self?.attributionCheck() }
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
        queue.maxConcurrentOperationCount = 1
        loadQueue()
    }

    /// Register the wrapper's listener for the deep-link + attribution streams.
    /// Events buffered before a listener attached are flushed now.
    public func setListener(_ l: ReflectListener?) {
        listener = l
        if let l = l {
            if let p = pendingDeferredDeepLink { pendingDeferredDeepLink = nil; DispatchQueue.main.async { l.onDeepLink(p) } }
            if let p = pendingAttribution { pendingAttribution = nil; DispatchQueue.main.async { l.onAttribution(p) } }
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
            // Anonymous → known: emit a _user_alias stitch event (Unity parity).
            if userId == nil, let nid = newId, !nid.isEmpty {
                emitJsonEvent("_user_alias", ["user_id_new": nid, "previous_anonymous": installUuid], nil)
            }
            userId = newId
            result(nil)
        case "clearUserId":
            userId = nil
            result(nil)
        case "setUserProperties":
            if let json = args?["properties"] as? String,
               let data = json.data(using: .utf8),
               let dict = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
                userProperties = dict
            }
            result(nil)
        case "setGlobalProperty":
            if let key = args?["key"] as? String, let value = args?["value"] {
                globalLock.lock()
                globalProperties[key] = value
                globalLock.unlock()
                persistGlobalProps()
            }
            result(nil)
        case "unsetGlobalProperty":
            if let key = args?["key"] as? String {
                globalLock.lock()
                globalProperties.removeValue(forKey: key)
                globalLock.unlock()
                persistGlobalProps()
            }
            result(nil)
        case "clearGlobalProperties":
            globalLock.lock()
            globalProperties.removeAll()
            globalLock.unlock()
            persistGlobalProps()
            result(nil)
        case "setConsent":
            let granted = args?["granted"] as? Bool ?? true
            consentState = granted ? "granted" : "denied"
            if ffCoppa { advertisingConsent = false }              // COPPA always wins
            else if !granted { advertisingConsent = false }
            else if !requireAdConsentLatch { advertisingConsent = true }
            UserDefaults.standard.set(consentState, forKey: "reflect_consent_state")
            if granted {
                DispatchQueue.main.async { [weak self] in self?.refreshAttStatus(); self?.refreshIdfa() }  // re-collect IDFA on grant (gap 5)
                scheduleDrain(0)   // consent regained → flush held events
            }
            result(nil)
        case "setExternalDeviceId":
            externalDeviceId = args?["externalDeviceId"] as? String
            result(nil)
        case "setAudience":
            // Audience tags → _set_audience (Unity SetAudience); Flutter routes via trackEvent.
            if let tags = args?["tags"] as? [String] { emitJsonEvent("_set_audience", ["tags": tags], nil) }
            result(nil)
        case "setThirdPartySharing":
            if let enabled = args?["enabled"] as? Bool {
                thirdPartySharing = NSNumber(value: enabled)
                UserDefaults.standard.set(enabled, forKey: "reflect_third_party_sharing")   // persist (Unity parity)
                // Authoritative event (Unity parity) so the server records the change.
                emitJsonEvent("_third_party_sharing", ["enabled": enabled], nil)
            }
            result(nil)
        case "setAdvertisingConsent":
            let g = args?["granted"] as? Bool ?? true
            // COPPA / denied consent hard-block re-enabling ad tracking.
            if g && (ffCoppa || consentState == "denied") {
                log("setAdvertisingConsent(true) ignored — blocked by \(ffCoppa ? "COPPA" : "denied consent")")
                advertisingConsent = false
            } else { advertisingConsent = g }
            if advertisingConsent {
                DispatchQueue.main.async { [weak self] in self?.refreshAttStatus(); self?.refreshIdfa() }  // re-collect IDFA on grant (gap 5)
            }
            UserDefaults.standard.set(advertisingConsent, forKey: "reflect_ad_consent")   // persist (Unity parity)
            result(nil)
        case "setPartnerSharing":
            if let partner = args?["partner"] as? String, !partner.isEmpty,
               let key = args?["key"] as? String, !key.isEmpty, let value = args?["value"] {
                var m = partnerSharing[partner] ?? [:]
                m[key] = value
                partnerSharing[partner] = m
                emitJsonEvent("_third_party_sharing", ["partner_sharing": partnerSharing], nil)
            }
            result(nil)
        case "registerPushToken":
            // The token rides the envelope (top-level push_token) on every
            // subsequent event; the server promotes it to a column.
            pushToken = args?["token"] as? String
            // ALSO emit a _push_token event (Unity parity) so the server stores
            // the token immediately, not only when the next event carries it.
            if let tok = pushToken, !tok.isEmpty {
                var props: [String: Any] = ["token": tok]
                if let provider = args?["provider"] as? String, !provider.isEmpty { props["provider"] = provider }
                emitJsonEvent("_push_token", props, nil)
            }
            result(nil)
        case "setPushToken":
            // STICKY-ONLY (Unity parity): sets the envelope field, no _push_token event.
            pushToken = args?["token"] as? String
            result(nil)
        case "setIntegrityToken":
            integrityToken = args?["token"] as? String
            result(nil)
        case "verifyPurchase":
            DispatchQueue.global(qos: .utility).async { [weak self] in
                let r = self?.verifyPurchaseHttp(args) ?? ["status": "failed", "code": 0, "message": "request_failed"]
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
            let attr = UserDefaults.standard.string(forKey: "reflect_attribution_json")
            result(attr)
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
                reply(UserDefaults.standard.string(forKey: "reflect_attribution_json"))
            }
            DispatchQueue.global(qos: .utility).async { [weak self] in
                self?.attributionCheck(forceFresh: true)
                reply(UserDefaults.standard.string(forKey: "reflect_attribution_json"))
            }
        case "updateConversionValue":
            handleUpdateConversionValue(args: args, result: result)
        case "resolveDeepLink":
            let url = args?["url"] as? String ?? ""
            DispatchQueue.global(qos: .utility).async { [weak self] in
                let resolved = self?.resolveLink(url)
                DispatchQueue.main.async { result(resolved) }
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
            setTrackingEnabled(args?["enabled"] as? Bool ?? true)
            result(nil)
        case "isEnabled":
            result(trackingEnabled)
        case "setPartnerParameter":
            let key = args?["key"] as? String ?? ""
            if !key.isEmpty, let value = args?["value"] as? String {
                globalLock.lock(); partnerParameters[key] = value; globalLock.unlock()
                persistPartnerParams()
            }
            result(nil)
        case "unsetPartnerParameter":
            let key = args?["key"] as? String ?? ""
            globalLock.lock(); partnerParameters.removeValue(forKey: key); globalLock.unlock()
            persistPartnerParams()
            result(nil)
        case "clearPartnerParameters":
            globalLock.lock(); partnerParameters.removeAll(); globalLock.unlock()
            persistPartnerParams()
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
        if let b = args?["batchSize"] as? Int, b >= 1, b <= 1000 { cfgBatchSize = b }
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

        if args?["requireAdvertisingConsent"] as? Bool == true {
            requireAdConsentLatch = true
            advertisingConsent = false
        }
        requireConsent = args?["requireConsent"] as? Bool == true
        // Consent posture (Unity parity): a PERSISTED state wins; else explicit
        // initialConsent; else fail-closed ("denied") when requireConsent. Applied
        // BEFORE install/open fire so a CMP-gated app never leaks ids on event 1.
        let ic = args?["initialConsent"] as? String
        if UserDefaults.standard.object(forKey: "reflect_consent_state") != nil {
            consentState = UserDefaults.standard.string(forKey: "reflect_consent_state") ?? "granted"
        } else if ic == "denied" || ic == "granted" {
            consentState = ic!
        } else if requireConsent {
            consentState = "denied"
        }
        if consentState == "denied" { advertisingConsent = false }
        if ffCoppa { advertisingConsent = false }   // COPPA hard-gate
        // Restore persisted opt-outs across relaunch (Unity parity): only AND-IN a stored
        // ad-consent=false (never force it back true — COPPA/denied/requireAdConsent win).
        if UserDefaults.standard.object(forKey: "reflect_ad_consent") != nil,
           !UserDefaults.standard.bool(forKey: "reflect_ad_consent") { advertisingConsent = false }
        if UserDefaults.standard.object(forKey: "reflect_third_party_sharing") != nil {
            thirdPartySharing = NSNumber(value: UserDefaults.standard.bool(forKey: "reflect_third_party_sharing"))
        }
        // COPPA hard-gate (Adjust parity): a child-directed app must NEVER share with
        // partners — force off regardless of any stored/default. (Was a defect: COPPA
        // apps still transmitted third_party_sharing=true.)
        if ffCoppa { thirdPartySharing = NSNumber(value: false) }
        restoreParams()   // reload persisted global/partner params BEFORE the install/first-session events emit

        // Snapshot main-thread-only UIKit values now (handleInitialize runs on
        // the platform/main thread), so buildDevice never touches UIKit off-thread.
        queue.maxConcurrentOperationCount = 1   // serial — session state has no torn reads
        snapshotUIKit()
        registerForegroundObservers()

        let defaults = UserDefaults.standard
        trackingEnabled = !defaults.bool(forKey: "reflect_suppressed")
        initialized = true

        if !trackingEnabled {
            // A prior deleteUserData()/setEnabled(false) latched suppression — stay
            // fully silent (no identity, install, session, or events) until re-enabled.
            log("Initialized in suppressed state — tracking off until re-enabled")
            result(nil)
            return
        }
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
            queue.addOperation { [weak self] in
                guard let self = self else { return }
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
                    self.trackEventInternal(eventName: "_clock_skew", propertiesJson: "{\"skew_ms\":\(rawGap)}", referral: nil)
                }
                let crossKillGap: Int64 = lastWall > 0 ? max(0, rawGap) : -1
                self.recoverInterruptedSession(crossKillGap)
                self.onForeground(crossKillGap)
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
                DispatchQueue.global(qos: .utility).async { [weak self] in self?.resolveDeferredDeepLink() }
            }
        } else if autoResolveDeferred && defaults.bool(forKey: "reflect_pending_deferred_dl") {
            // X1: a deferred deep link that was offline at first launch stays pending;
            // re-attempt it on any later launch (attribution already re-runs per session).
            DispatchQueue.global(qos: .utility).async { [weak self] in self?.resolveDeferredDeepLink() }
        }
        DispatchQueue.global(qos: .utility).async { [weak self] in self?.attributionCheck() }
        DispatchQueue.global(qos: .utility).async { [weak self] in self?.retryPendingDelete() }  // GDPR durability

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
        result(nil)
    }

    private func handleTrackPurchase(args: [String: Any]?, result: ReflectResult) {
        if !initialized { result(nil); return }
        emitJsonEvent("purchase", purchaseProps(args), revenueTopLevel(args), deduplicationId: purchaseDedup(args))
        result(nil)
    }

    private func handleTrackSubscription(args: [String: Any]?, result: ReflectResult) {
        if !initialized { result(nil); return }
        var top = revenueTopLevel(args)
        top["is_subscription"] = true
        var props = purchaseProps(args)
        props["is_trial"] = args?["isTrial"] as? Bool ?? false
        emitJsonEvent("subscribe", props, top, deduplicationId: purchaseDedup(args))
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
                               deduplicationId: String? = nil) {
        if let data = try? JSONSerialization.data(withJSONObject: props),
           let json = String(data: data, encoding: .utf8) {
            trackEventInternal(eventName: name, propertiesJson: json, referral: nil,
                               topLevel: topLevel, deduplicationId: deduplicationId)
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
        if #available(iOS 16.1, *) {
            SKAdNetwork.updatePostbackConversionValue(0, coarseValue: .low, lockWindow: false) { _ in }
        } else if #available(iOS 15.4, *) {
            SKAdNetwork.updatePostbackConversionValue(0) { _ in }
        } else if #available(iOS 14.0, *) {
            SKAdNetwork.registerAppForAdNetworkAttribution()
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

        if #available(iOS 16.1, *) {
            var coarse: SKAdNetwork.CoarseConversionValue = .low
            if coarseValue == "medium" { coarse = .medium }
            else if coarseValue == "high" { coarse = .high }
            SKAdNetwork.updatePostbackConversionValue(fineValue, coarseValue: coarse, lockWindow: lockWindow) { [weak self] error in
                if let error = error {
                    result("{\"success\":false,\"error\":\"\(error.localizedDescription)\"}")
                } else {
                    self?.reportSkanCv(fineValue, coarseValue, lockWindow, "SKAdNetwork4")
                    result("{\"success\":true,\"method\":\"SKAdNetwork4\"}")
                }
            }
            return
        }

        if #available(iOS 15.4, *) {
            SKAdNetwork.updatePostbackConversionValue(fineValue) { [weak self] error in
                if let error = error {
                    result("{\"success\":false,\"error\":\"\(error.localizedDescription)\"}")
                } else {
                    self?.reportSkanCv(fineValue, coarseValue, lockWindow, "SKAdNetwork3")
                    result("{\"success\":true,\"method\":\"SKAdNetwork3\"}")
                }
            }
            return
        }

        if #available(iOS 14.0, *) {
            SKAdNetwork.registerAppForAdNetworkAttribution()
            SKAdNetwork.updateConversionValue(fineValue)
            reportSkanCv(fineValue, coarseValue, lockWindow, "SKAdNetwork2")
            result("{\"success\":true,\"method\":\"SKAdNetwork2\"}")
            return
        }

        result("{\"success\":false,\"error\":\"skan_not_available\"}")
    }

    /// Report a successful SKAN conversion-value update to the Reflect server as a
    /// `_skan_cv` event (rides the normal signed /event path). Lets the server do
    /// first-party CV tracking + reconcile with Apple's eventual SKAN postback —
    /// previously the CV update was applied locally and never reached the server.
    private func reportSkanCv(_ fineValue: Int, _ coarseValue: String?, _ lockWindow: Bool, _ method: String) {
        var props: [String: Any] = [
            "conversion_value": fineValue,
            "lock_window": lockWindow,
            "skan_version": method,
        ]
        if let c = coarseValue, !c.isEmpty { props["coarse_value"] = c }
        emitJsonEvent("_skan_cv", props, nil)
    }

    // MARK: - Inbound deep links (direct custom-scheme + Universal Links)
    // Previously DEAD on iOS: getInitialDeepLink read a UserDefaults key nothing
    // wrote, and there were no openURL / continueUserActivity handlers. Now wired
    // via addApplicationDelegate so warm opens hit the onDeepLink stream and cold
    // launches are captured for getInitialDeepLink.

    /// Push an incoming URL to the host: warm launch → onDeepLink stream; also
    /// persisted so a cold-launch getInitialDeepLink returns it.
    public func handleIncomingURL(_ url: URL) {
        UserDefaults.standard.set(url.absoluteString, forKey: "reflect_launch_url")
        var params: [String: String] = [:]
        if let comps = URLComponents(url: url, resolvingAgainstBaseURL: false), let q = comps.queryItems {
            for item in q { params[item.name] = item.value ?? "" }
        }
        var map: [String: Any] = ["url": url.absoluteString, "path": url.path, "isDeferred": false, "params": params]
        if let c = params["click_id"] { map["clickId"] = c }
        if let c = params["campaign"] { map["campaign"] = c }
        if let p = params["partner"]  { map["partner"]  = p }
        lastDeepLink = url.absoluteString   // GetLastDeeplink accessor (Unity parity)
        emitDeepLink(map)
        reportDeepLinkOpened(url.absoluteString, params)
    }

    /// Emit a `deep_link_opened` event for server-side reattribution / deep-link
    /// conversion (mirrors Unity). is_reattribution when the link carries tracking
    /// params. Deduped per URL so it fires at most once.
    private func reportDeepLinkOpened(_ url: String, _ params: [String: String], _ source: String = "direct") {
        if url == lastDeepLinkReported { return }
        lastDeepLinkReported = url
        var props: [String: Any] = ["url": url, "source": source]   // direct / deferred (Unity parity)
        if let p = URL(string: url)?.path, !p.isEmpty { props["path"] = p }
        var hasTracking = false
        for (k, v) in params where !v.isEmpty {
            // Forward ALL query params as dl_<key> (key capped 30 chars), Unity parity.
            let key = k.count > 30 ? String(k.prefix(30)) : k
            props["dl_" + key] = v
            if k == "click_id" || k == "campaign" || k == "partner" { hasTracking = true }
        }
        if let c = params["click_id"] { props["click_id"] = c }
        if let c = params["campaign"] { props["campaign"] = c }
        if let p = params["partner"]  { props["partner"]  = p }
        if hasTracking { props["is_reattribution"] = true }
        emitJsonEvent("deep_link_opened", props, nil)
    }

    /// Resolve/unshorten a tracking URL via /deeplink/resolve (client parity with
    /// Unity's ResolveDeepLink; server url-resolve handling is a shared gap).
    private func resolveLink(_ url: String) -> String? {
        let bodyDict: [String: Any] = ["app_key": appKey, "install_uuid": installUuid, "url": url]
        guard let bodyData = try? JSONSerialization.data(withJSONObject: bodyDict),
              let bodyStr = String(data: bodyData, encoding: .utf8) else { return nil }
        let sig = (signingSecret?.isEmpty == false) ? hmacHex(bodyStr, signingSecret!) : nil
        guard let resp = httpJson("\(baseUrl)/deeplink/resolve", "POST", bodyStr, sig),
              let data = resp.data(using: .utf8),
              let obj = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] else { return nil }
        return (obj["resolved_url"] as? String) ?? (obj["deep_link_path"] as? String)
    }

    /// LinkMe (opt-in): if the pasteboard holds an http(s) URL on first launch,
    /// route it as a deferred deep link (improves iOS deferred match). Unity parity.
    private func linkMeRecover() {
        if consentState == "denied" { return }   // GDPR: no clipboard read / network under denied consent
        if !linkMeEnabled { return }
        guard let text = UIPasteboard.general.string,
              text.hasPrefix("http://") || text.hasPrefix("https://"),
              let url = URL(string: text) else { return }
        DispatchQueue.main.async { [weak self] in self?.handleIncomingURL(url) }
    }

    private func handleGetInitialDeepLink(result: ReflectResult) {
        guard let urlString = UserDefaults.standard.string(forKey: "reflect_launch_url"),
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
        UserDefaults.standard.removeObject(forKey: "reflect_launch_url")   // consume once
        if let data = try? JSONSerialization.data(withJSONObject: dl),
           let json = String(data: data, encoding: .utf8) {
            result(json)
        } else { result(nil) }
    }

    private func handleDeleteUserData(result: @escaping ReflectResult) {
        queue.addOperation { [weak self] in
            guard let self = self else { result(false); return }
            let uuidToDelete = self.installUuid
            let uid = self.userId
            // Forget-me LATCH first + persist the pending-delete uuid (survives, so an
            // unconfirmed delete is retried on next launch — Unity parity).
            let d = UserDefaults.standard
            d.removeObject(forKey: "reflect_install_uuid")
            d.removeObject(forKey: "reflect_attribution_json")
            d.removeObject(forKey: "reflect_session_open")
            d.removeObject(forKey: "reflect_global_props")
            d.removeObject(forKey: "reflect_partner_params")
            d.set(true, forKey: "reflect_suppressed")
            d.set(uuidToDelete, forKey: "reflect_pending_delete")
            self.trackingEnabled = false
            self.installUuid = ""
            self.sessionOpen = false
            self.userId = nil
            self.userProperties = nil
            self.pushToken = nil
            self.externalDeviceId = nil
            self.globalLock.lock()
            self.globalProperties.removeAll()
            self.partnerParameters.removeAll()
            self.globalLock.unlock()
            // Send the SIGNED delete; clear the pending marker only on success.
            self.sendPrivacyDelete(uuidToDelete, uid) { ok in
                if ok { UserDefaults.standard.removeObject(forKey: "reflect_pending_delete") }
                DispatchQueue.main.async { result(ok) }
            }
        }
    }

    /// POST a /privacy/delete for one install_uuid, HMAC-signed (+ company/app-key
    /// headers) when a signing secret is configured (Unity parity). 2xx → true.
    private func sendPrivacyDelete(_ uuid: String, _ uid: String?, _ completion: @escaping (Bool) -> Void) {
        var body: [String: Any] = ["app_key": appKey, "install_uuid": uuid]
        if let u = uid { body["user_id"] = u }
        guard let bodyData = try? JSONSerialization.data(withJSONObject: body),
              let bodyStr = String(data: bodyData, encoding: .utf8),
              let url = URL(string: "\(baseUrl)/privacy/delete") else { completion(false); return }
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue(hostSdkVersion, forHTTPHeaderField: "X-Reflect-Sdk")
        if let secret = signingSecret, !secret.isEmpty {
            request.setValue(appKey, forHTTPHeaderField: "X-Reflect-App-Key")
            if let ck = companyKey, !ck.isEmpty { request.setValue(ck, forHTTPHeaderField: "X-Reflect-Company-Key") }
            request.setValue(hmacHex(bodyStr, secret), forHTTPHeaderField: "X-Reflect-Signature")
            request.setValue("1", forHTTPHeaderField: "X-Reflect-Signature-Version")
        }
        request.httpBody = bodyData
        request.timeoutInterval = 10
        URLSession.shared.dataTask(with: request) { _, response, _ in
            let code = (response as? HTTPURLResponse)?.statusCode ?? 0
            completion(code >= 200 && code < 300)
        }.resume()
    }

    /// On launch, retry an unconfirmed /privacy/delete (GDPR durability, Unity parity).
    private func retryPendingDelete() {
        guard let pending = UserDefaults.standard.string(forKey: "reflect_pending_delete"),
              !pending.isEmpty else { return }
        sendPrivacyDelete(pending, nil) { ok in
            if ok { UserDefaults.standard.removeObject(forKey: "reflect_pending_delete") }
        }
    }

    // MARK: - Event Tracking

    private func trackEventInternal(eventName: String, propertiesJson: String?, referral: [String: Any]?,
                                    topLevel: [String: Any]? = nil, callbackId: String? = nil,
                                    callbackParamsJson: String? = nil, partnerParamsJson: String? = nil,
                                    deduplicationId: String? = nil) {
        if !initialized || !trackingEnabled { return }   // forget-me / disable latch
        queue.addOperation { [weak self] in
            guard let self = self else { return }
            // Client-side de-dup (Unity parity): drop an event whose deduplication_id
            // was seen recently. Runs on the serial queue → no lock. No id ⇒ always pass.
            if let d = deduplicationId, !d.isEmpty, self.isDuplicateEvent(d) {
                self.log("Dropped duplicate '\(eventName)' (dedup_id=\(d))")
                return
            }
            // Crash-report throttle (Unity parity): cap `_crash` to 1/min.
            if eventName == "_crash" {
                let now = self.nowMs()
                if now - self.lastCrashMs < 60_000 { return }
                self.lastCrashMs = now
            }
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
                self.enqueue(json)
            }
        }
    }

    // MARK: - Durable event queue (persist → drain → retry)

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
        queueLock.unlock()
    }

    /// Caller MUST hold queueLock.
    private func persistQueueLocked() {
        guard let url = queueFileURL() else { return }
        let text = eventQueue.joined(separator: "\n")
        try? text.write(to: url, atomically: true, encoding: .utf8)
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

    private func enqueue(_ payload: String) {
        queueLock.lock()
        // Overflow → drop the NEWEST (this event) instead of the oldest, so the
        // attribution-critical install/first-session events at the head survive a
        // sustained backlog (Unity parity).
        if eventQueue.count >= cfgMaxQueue {
            droppedCount += 1
            queueLock.unlock()
            return
        }
        eventQueue.append(payload)
        persistQueueLocked()
        queueLock.unlock()
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
        if !initialized { return }
        if offlineMode || localOnly { return }   // offline / debug-mode → keep queued, don't send
        if consentState == "denied" { return }   // consent denied → HOLD on-device, never transmit (GDPR)
        if !beginSending() { return }
        var releaseGuard = true
        defer { if releaseGuard { endSending() } }
        while true {
            if offlineMode { break }   // toggled offline mid-drain → stop sending
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
            let rawBatch = Array(eventQueue.prefix(n))
            let qsize = eventQueue.count
            queueLock.unlock()
            // X2: re-stamp the LIVE consent/sharing/ATT onto each queued event just before
            // send (Adjust updatePackagesTrackingI) — a frozen-at-enqueue event otherwise
            // transmits a stale decision (e.g. an install built pre-ATT-grant).
            let batchEvents = rawBatch.map { restampConsent($0) }
            if batchEvents.isEmpty { break }
            switch postBatch(batchEvents, headRetryCount, qsize) {
            case .success, .drop:
                queueLock.lock()
                eventQueue.removeFirst(min(batchEvents.count, eventQueue.count))
                persistQueueLocked()
                queueLock.unlock()
                drainBackoffMs = 0
                headRetryCount = 0
                nextSendAllowedMs = 0
                clearPersistedBackoff()
                // Server-requested pacing: gate the NEXT batch by continue_in.
                let pace = pendingContinueMs
                pendingContinueMs = 0
                if pace > 0 {
                    nextSendAllowedMs = monotonicMs() + pace
                    endSending(); releaseGuard = false; scheduleDrain(pace); return
                }
            case .retry:
                headRetryCount += 1
                drainBackoffMs = nextBackoff(drainBackoffMs, lastRetryAfterMs)
                nextSendAllowedMs = monotonicMs() + drainBackoffMs
                persistBackoff(drainBackoffMs)   // survive app restart
                endSending()            // release before scheduling the retry
                releaseGuard = false
                scheduleDrain(drainBackoffMs)
                return
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
    private func postBatch(_ events: [String], _ retryCount: Int, _ queueSize: Int) -> SendResult {
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
        request.setValue("flutter", forHTTPHeaderField: "X-Reflect-Platform")
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
        let sem = DispatchSemaphore(value: 0)
        let task = URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            defer { sem.signal() }
            if error != nil { outcome = .retry; return }
            guard let http = response as? HTTPURLResponse else { outcome = .retry; return }
            let code = http.statusCode
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
            self?.lastRetryAfterMs  = directives.retryIn > 0 ? directives.retryIn : hdrRetry
            self?.pendingContinueMs = (200..<300).contains(code) ? directives.continueIn : 0
            let ri = self?.lastRetryAfterMs ?? 0, ci = self?.pendingContinueMs ?? 0
            self?.log("Batch sent → \(code) \(signed ? "(signed /event)" : "(/event/batch)") [n=\(events.count) batch=\(bid) retry=\(retryCount) q=\(queueSize)]"
                + (ri > 0 ? " retry_in=\(ri)ms" : "") + (ci > 0 ? " continue_in=\(ci)ms" : ""))
        }
        task.resume()
        sem.wait()
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
    private func resolveDeferredDeepLink() {
        if consentState == "denied" || localOnly { return }   // GDPR / debug-mode: no /deeplink/resolve
        setPendingSignal("reflect_pending_deferred_dl", true)   // X1: stay pending until a real response lands
        let bodyDict: [String: Any] = ["app_key": appKey, "install_uuid": installUuid]
        guard let bodyData = try? JSONSerialization.data(withJSONObject: bodyDict),
              let body = String(data: bodyData, encoding: .utf8) else { return }
        let sig = (signingSecret?.isEmpty == false) ? hmacHex(body, signingSecret!) : nil
        guard let resp = httpJson("\(baseUrl)/deeplink/resolve", "POST", body, sig) else { return }  // offline → stay pending
        setPendingSignal("reflect_pending_deferred_dl", false)   // got a response (link or not) → resolved
        guard let data = resp.data(using: .utf8),
              let obj = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any],
              let path = obj["deep_link_path"] as? String, !path.isEmpty else { return }

        var params: [String: String] = [:]
        if let p = obj["deep_link_params"] as? [String: Any] {
            for (k, v) in p { params[k] = "\(v)" }
        }
        var map: [String: Any] = ["url": path, "path": path, "isDeferred": true, "params": params]
        if let c = params["click_id"] { map["clickId"] = c }
        if let c = params["campaign"] { map["campaign"] = c }
        if let pr = params["partner"] { map["partner"] = pr }
        lastDeepLink = path   // GetLastDeeplink (Unity parity)
        emitDeepLink(map)
        // Fire deep_link_opened on the DEFERRED branch too (Unity parity).
        reportDeepLinkOpened(path, params, "deferred")
    }

    /// Poll GET /attribution/check (HMAC-signed query) once per session. On a
    /// newer attribution row, persist it (so getAttribution works) + the
    /// watermark, and push onAttributionChanged. Needs signingSecret.
    private func attributionCheck(forceFresh: Bool = false, attempt: Int = 0, askAttempt: Int = 0) {
        if consentState == "denied" || localOnly { return }   // GDPR / debug-mode: no /attribution/check
        guard let secret = signingSecret, !secret.isEmpty else { return }
        if attempt == 0 { setPendingSignal("reflect_pending_attr", true) }   // X1: durable retry across process death
        let encoded = installUuid.addingPercentEncoding(withAllowedCharacters: .alphanumerics) ?? installUuid
        let since = forceFresh ? 0 : lastAttributionCheckMs
        let query = "install_uuid=\(encoded)&since=\(since)"
        guard let resp = httpJson("\(baseUrl)/attribution/check?\(query)", "GET", nil, hmacHex(query, secret)) else {
            // Transient failure → retry {2s,5s} up to 3 attempts (Unity parity), so an
            // attribution that resolves while we were offline at install isn't missed.
            if attempt < 2 {
                let delayMs = attempt == 0 ? 2000 : 5000
                DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + .milliseconds(delayMs)) { [weak self] in
                    self?.attributionCheck(forceFresh: forceFresh, attempt: attempt + 1, askAttempt: askAttempt)
                }
            }
            return   // network failure → stay pending (X1); retried on connectivity/next launch
        }
        setPendingSignal("reflect_pending_attr", false)   // X1: got a response → resolved
        guard let data = resp.data(using: .utf8),
              let obj = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] else { return }
        guard (obj["changed"] as? Bool) == true, let d = obj["data"] as? [String: Any] else {
            // A3: honor a server-driven `ask_in` — attribution not resolved yet; re-poll
            // that far out, bounded so an organic install doesn't poll forever.
            if let dir = obj["directives"] as? [String: Any] {
                let askIn = (dir["ask_in"] as? Int) ?? Int((dir["ask_in"] as? Double) ?? 0)
                if askIn > 0, askIn <= Int(ReflectCore.maxBackoffMs), askAttempt < ReflectCore.maxAskInRepolls {
                    DispatchQueue.global(qos: .utility).asyncAfter(deadline: .now() + .milliseconds(askIn)) { [weak self] in
                        self?.attributionCheck(forceFresh: true, attempt: 0, askAttempt: askAttempt + 1)
                    }
                }
            }
            return
        }

        let attributedAt = (d["attributed_at_ms"] as? Int64) ?? Int64((d["attributed_at_ms"] as? Double) ?? 0)
        if attributedAt > lastAttributionCheckMs {
            lastAttributionCheckMs = attributedAt
            UserDefaults.standard.set(Int(attributedAt), forKey: "reflect_attr_watermark")
        }

        // Map server fields → AttributionData keys (type/partner/campaign/clickId).
        var map: [String: Any] = [:]
        if let t = d["attribution_type"] as? String { map["type"] = t }
        if let p = d["partner_slug"] as? String { map["partner"] = p }
        if let c = d["campaign_name"] as? String { map["campaign"] = c }
        if let ci = d["click_id"] as? String { map["clickId"] = ci }

        // Value-level change detection (Adjust `equals` dedup): only surface the callback
        // when a field actually changed — a server re-stamp with identical tracker fields
        // must NOT fire a spurious onAttribution.
        var changed = true
        if let pdata = try? JSONSerialization.data(withJSONObject: map, options: .sortedKeys),
           let pstr = String(data: pdata, encoding: .utf8) {   // sortedKeys → stable across restarts (dicts are unordered)
            changed = pstr != UserDefaults.standard.string(forKey: "reflect_attribution_json")
            UserDefaults.standard.set(pstr, forKey: "reflect_attribution_json")
        }
        if changed { emitAttribution(map) }
    }

    private func emitDeepLink(_ map: [String: Any]) {
        DispatchQueue.main.async {
            if let l = self.listener { l.onDeepLink(map) } else { self.pendingDeferredDeepLink = map }
        }
    }

    private func emitAttribution(_ map: [String: Any]) {
        DispatchQueue.main.async {
            if let l = self.listener { l.onAttribution(map) } else { self.pendingAttribution = map }
        }
    }

    /// GET/POST JSON, blocking via semaphore on the calling background queue.
    /// Returns the response body on 2xx, else nil.
    private func httpJson(_ urlStr: String, _ method: String, _ body: String?, _ signature: String?) -> String? {
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
        task.resume()
        sem.wait()
        return out
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
    private func verifyPurchaseHttp(_ args: [String: Any]?) -> [String: Any] {
        let productId     = args?["productId"] as? String ?? ""
        let transactionId = args?["transactionId"] as? String ?? ""
        let purchaseToken = args?["purchaseToken"] as? String ?? ""
        let receiptData   = args?["receiptData"] as? String ?? ""
        if baseUrl.isEmpty { return ["status": "unknown", "code": 0, "message": "debug_mode"] }
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
        guard let resp = httpJson("\(baseUrl)/purchase/verify", "POST", body, sig) else {
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
            "is_rooted": isJailbroken(),
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

    /// Jailbreak detection (ported from the Unity ReflectBridge IsJailbroken). */
    private func isJailbroken() -> Bool {
        #if targetEnvironment(simulator)
        return false
        #else
        let fm = FileManager.default
        for p in ["/Applications/Cydia.app", "/Library/MobileSubstrate/MobileSubstrate.dylib",
                  "/bin/bash", "/usr/sbin/sshd", "/etc/apt", "/private/var/lib/apt/"] {
            if fm.fileExists(atPath: p) { return true }
        }
        // A sandboxed (non-jailbroken) app cannot open this file → jailbroken if it can.
        if let f = fopen("/private/var/mobile/Library/Preferences/.GlobalPreferences.plist", "r") {
            fclose(f); return true
        }
        return false
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
        snapIdfv = UIDevice.current.identifierForVendor?.uuidString
        uikitSnapshotted = true
        refreshAttStatus()
        refreshIdfa()
    }

    /// IDFA read on the main thread (ATT/ASIdentifierManager are read here, never
    /// in the background buildDevice). Refreshed on each activation since ATT auth
    /// can change after the prompt. buildDevice still gates on advertisingConsent.
    private func refreshIdfa() {
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
            switch ATTrackingManager.trackingAuthorizationStatus {
            case .authorized:    cachedAttStatus = "authorized"
            case .denied:        cachedAttStatus = "denied"
            case .restricted:    cachedAttStatus = "restricted"
            case .notDetermined: cachedAttStatus = "not_determined"
            @unknown default:    cachedAttStatus = "not_determined"
            }
        }
        #endif
    }

    private func nowMs() -> Int64 { return Int64(Date().timeIntervalSince1970 * 1000) }

    // MARK: - Session manager (feeds aggregates_sessions)

    // All of these run on the serial `queue`. A session spans brief fg/bg flips
    // (subsessions); it ends only after a > sessionGapMs background or is recovered
    // on the next launch if the process died mid-session.

    private func startSession(intervalMs: Int64 = -1) {
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
        emitJsonEvent("session_start", props, nil)
        if sessionCount > 1 {
            DispatchQueue.global(qos: .utility).async { [weak self] in self?.attributionCheck() }
        }
    }

    private func bankActive() {
        if sessionStartElapsed > 0 {
            sessionActiveMs += max(0, monotonicMs() - sessionStartElapsed)
            sessionStartElapsed = 0
            UserDefaults.standard.set(Int(sessionActiveMs), forKey: "reflect_session_active_ms")
        }
    }

    /// Foreground heartbeat (Unity parity): bank+persist the active stint WITHOUT
    /// stopping the timer, so a crash mid-foreground loses ≤30s of session length.
    private func heartbeatBank() {
        if sessionStartElapsed > 0 {
            let now = monotonicMs()
            sessionActiveMs += max(0, now - sessionStartElapsed)
            sessionStartElapsed = now
            UserDefaults.standard.set(Int(sessionActiveMs), forKey: "reflect_session_active_ms")
            UserDefaults.standard.set(Int(nowMs()), forKey: "reflect_last_activity_wall")   // keep cross-kill anchor fresh
        }
    }
    private func startHeartbeat() {
        stopHeartbeat()
        let t = DispatchSource.makeTimerSource(queue: .main)
        t.schedule(deadline: .now() + 30, repeating: 30)
        t.setEventHandler { [weak self] in self?.heartbeatBank() }
        heartbeatTimer = t
        t.resume()
    }
    private func stopHeartbeat() { heartbeatTimer?.cancel(); heartbeatTimer = nil }

    private func emitSessionEnd() {
        bankActive()
        emitJsonEvent("session_end",
            ["session_length_ms": sessionActiveMs, "session_count": sessionCount, "subsession_count": subsessionCount], nil)
        sessionOpen = false
        sessionActiveMs = 0
        let d = UserDefaults.standard
        d.set(false, forKey: "reflect_session_open")
        d.set(0, forKey: "reflect_session_active_ms")
    }

    /// If a prior process died with a session open, emit its banked length now.
    private func recoverInterruptedSession(_ crossKillGapMs: Int64 = -1) {
        // End the interrupted session ONLY if the cross-kill gap exceeded the threshold
        // (or is unknown). A within-threshold relaunch keeps it OPEN so onForeground
        // continues it as a subsession — no phantom extra session (Unity parity).
        if sessionOpen && (crossKillGapMs < 0 || crossKillGapMs > Int64(sessionThresholdMs)) { emitSessionEnd() }
    }

    private func onForeground(_ crossKillGapMs: Int64 = -1) {
        if !trackingEnabled { return }
        let now = monotonicMs()
        // gap=0 when never backgrounded this process (the launch foreground is a
        // continuation of the just-started session, not a 30-min-gap new one).
        let hasPrior = lastBackgroundElapsed > 0
        // Cold launch after a kill → use the persisted WALL-CLOCK gap (monotonic reset).
        let gap: Int64 = crossKillGapMs >= 0 ? crossKillGapMs : (hasPrior ? now - lastBackgroundElapsed : 0)
        if !sessionOpen {
            startSession(intervalMs: (crossKillGapMs >= 0 || hasPrior) ? gap : -1)
        } else if gap > Int64(sessionThresholdMs) {
            emitSessionEnd(); startSession(intervalMs: gap)
        } else if gap > ReflectCore.subsessionFloorMs {
            subsessionCount += 1
            UserDefaults.standard.set(Int(subsessionCount), forKey: "reflect_subsession_count")
            sessionStartElapsed = now
        } else {
            sessionStartElapsed = now
        }
        startHeartbeat()   // periodically bank+persist foreground time (crash granularity)
        UserDefaults.standard.set(Int(nowMs()), forKey: "reflect_last_activity_wall")   // refresh cross-kill anchor
    }

    private func onBackground() {
        if !trackingEnabled { return }
        stopHeartbeat()
        bankActive()
        lastBackgroundElapsed = monotonicMs()
        UserDefaults.standard.set(Int(nowMs()), forKey: "reflect_last_activity_wall")   // cross-kill gap anchor
    }

    /// Enable/disable measurement (and opt back in after a forget-me). Mirrors Android.
    private func setTrackingEnabled(_ enabled: Bool) {
        if enabled == trackingEnabled { return }
        if enabled {
            trackingEnabled = true
            UserDefaults.standard.removeObject(forKey: "reflect_suppressed")
            if installUuid.isEmpty { installUuid = getOrCreateInstallUuid() }
            registerConnectivityDrain()
            queue.addOperation { [weak self] in self?.startSession() }
            trackEventInternal(eventName: "app_open", propertiesJson: nil, referral: nil)
        } else {
            UserDefaults.standard.set(true, forKey: "reflect_suppressed")
            queue.addOperation { [weak self] in
                guard let self = self else { return }
                if self.sessionOpen { self.emitSessionEnd() }   // final session_end before suppressing
                self.trackingEnabled = false
            }
        }
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
            self.queue.addOperation { self.onForeground() }   // session state on the serial queue
        }
        nc.addObserver(forName: UIApplication.willResignActiveNotification, object: nil, queue: .main) { [weak self] _ in
            self?.isForegroundState = false
        }
        nc.addObserver(forName: UIApplication.didEnterBackgroundNotification, object: nil, queue: .main) { [weak self] _ in
            guard let self = self else { return }
            self.isForegroundState = false
            self.queue.addOperation { self.onBackground() }
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
