import Foundation

private enum TestFailure: Error, CustomStringConvertible {
    case assertion(String)
    var description: String {
        switch self { case .assertion(let message): return message }
    }
}

private func check(_ condition: @autoclosure () -> Bool, _ message: String) throws {
    if !condition() { throw TestFailure.assertion(message) }
}

private final class Token {}

private final class LockedState: @unchecked Sendable {
    private let lock = NSLock()
    private var registered = false
    private var cancelled = false

    func setRegistered(_ value: Bool) { lock.lock(); registered = value; lock.unlock() }
    func setCancelled() { lock.lock(); cancelled = true; lock.unlock() }
    func escaped() -> Bool { lock.lock(); defer { lock.unlock() }; return registered && !cancelled }
}

private func testBlockCancelsRegisteredWorkAndInvalidatesPermit() throws {
    let gate = PrivacyTransportGate(initiallyAllowed: true)
    guard let permit = gate.permit() else { throw TestFailure.assertion("missing initial permit") }
    let token = Token()
    let state = LockedState()
    try check(gate.register(token, permit: permit) { state.setCancelled() }, "registration failed")
    try check(gate.activeCountForTesting == 1, "registered task missing")
    gate.block()
    state.setRegistered(true)
    try check(!state.escaped(), "registered task was not cancelled")
    try check(!gate.isValid(permit), "old permit remained valid")
    try check(gate.activeCountForTesting == 0, "active task leaked after block")
    try check(!gate.register(Token(), permit: permit) {}, "stale permit registered after block")
}

private func testRegistrationVersusBlockRaceNeverLeavesLiveWork() throws {
    for iteration in 0..<1_000 {
        let gate = PrivacyTransportGate(initiallyAllowed: true)
        guard let permit = gate.permit() else { throw TestFailure.assertion("missing permit at \(iteration)") }
        let token = Token()
        let start = DispatchSemaphore(value: 0)
        let group = DispatchGroup()
        let state = LockedState()

        group.enter()
        DispatchQueue.global().async {
            start.wait()
            let registered = gate.register(token, permit: permit) { state.setCancelled() }
            state.setRegistered(registered)
            group.leave()
        }
        group.enter()
        DispatchQueue.global().async {
            start.wait()
            gate.block()
            group.leave()
        }
        start.signal(); start.signal()
        try check(group.wait(timeout: .now() + 2) == .success, "race timed out at \(iteration)")
        try check(!state.escaped(), "live request escaped privacy race at \(iteration)")
        try check(gate.activeCountForTesting == 0, "active request leaked at \(iteration)")
    }
}

private func testStaleBuildPermitCannotCrossQuickReopen() throws {
    let gate = PrivacyTransportGate(initiallyAllowed: true)
    guard let permit = gate.permit() else { throw TestFailure.assertion("missing permit") }
    gate.block()
    gate.allow()
    var mutated = false
    try check(!gate.runIfValid(permit) { mutated = true }, "stale permit crossed reopen")
    try check(!mutated, "stale work mutated fresh generation")
}

private func testSensitiveReadIsLinearizedAndCannotRouteAfterQuickReopen() throws {
    let gate = PrivacyTransportGate(initiallyAllowed: true)
    guard let permit = gate.permit() else { throw TestFailure.assertion("missing permit") }
    let readEntered = DispatchSemaphore(value: 0)
    let releaseRead = DispatchSemaphore(value: 0)
    let blockFinished = DispatchSemaphore(value: 0)
    let group = DispatchGroup()
    let routeLock = NSLock()
    var routed = false

    group.enter()
    DispatchQueue.global().async {
        _ = gate.runIfValid(permit) {
            readEntered.signal()
            releaseRead.wait()
        }
        group.leave()
    }
    try check(readEntered.wait(timeout: .now() + 1) == .success, "sensitive read did not start")
    group.enter()
    DispatchQueue.global().async {
        gate.block()
        blockFinished.signal()
        group.leave()
    }

    try check(blockFinished.wait(timeout: .now() + 0.1) == .timedOut,
              "privacy transition crossed an in-progress sensitive read")
    releaseRead.signal()
    try check(blockFinished.wait(timeout: .now() + 1) == .success, "privacy transition did not finish")
    gate.allow()
    _ = gate.runIfValid(permit) {
        routeLock.lock(); routed = true; routeLock.unlock()
    }
    try check(group.wait(timeout: .now() + 2) == .success, "sensitive-read race timed out")
    routeLock.lock(); let didRoute = routed; routeLock.unlock()
    try check(!didRoute, "stale sensitive input routed in a fresh generation")
    try check(!gate.isValid(permit), "sensitive-read permit survived quick reopen")
}

private func testDelayedCallbackCannotCrossPrivacyBoundary() throws {
    let gate = PrivacyTransportGate(initiallyAllowed: true)
    guard let permit = gate.permit() else { throw TestFailure.assertion("missing callback permit") }
    let release = DispatchSemaphore(value: 0)
    let done = DispatchSemaphore(value: 0)
    let stateLock = NSLock()
    var delivered = false

    DispatchQueue.global().async {
        release.wait()
        if gate.runIfValid(permit, {
            stateLock.lock(); delivered = true; stateLock.unlock()
        }) { /* delivered in the originating generation */ }
        done.signal()
    }

    gate.block()
    gate.allow()
    release.signal()
    try check(done.wait(timeout: .now() + 1) == .success, "delayed callback timed out")
    stateLock.lock(); let escaped = delivered; stateLock.unlock()
    try check(!escaped, "delayed callback crossed deny/delete generation")
}

private func testDeleteRequiresAuthenticationAndExplicitReceipt() throws {
    try check(PrivacyDeletePolicy.signingSecret(nil) == nil, "missing secret accepted")
    try check(PrivacyDeletePolicy.signingSecret("") == nil, "empty secret accepted")
    try check(PrivacyDeletePolicy.signingSecret("secret") == "secret", "valid secret rejected")
    let accepted = Data("{\"ok\":true,\"queued\":true}".utf8)
    try check(PrivacyDeletePolicy.isAccepted(statusCode: 202, data: accepted), "valid receipt rejected")
    try check(!PrivacyDeletePolicy.isAccepted(statusCode: 202, data: Data("{\"ok\":true}".utf8)),
              "partial receipt accepted")
    try check(!PrivacyDeletePolicy.isAccepted(statusCode: 204, data: nil), "empty 2xx accepted")
    try check(!PrivacyDeletePolicy.isAccepted(statusCode: 503, data: accepted), "non-2xx accepted")
}

private func testDeleteJournalRequiresInitializedTargetAndIdentifier() throws {
    let validTarget = PrivacyDeleteTarget(
        baseUrl: "https://api.example.test/",
        appKey: "app-a",
        companyKey: "company-a"
    )
    try check(
        PrivacyDeletePolicy.identifierForJournal(
            initialized: false,
            inMemory: "",
            persisted: "persisted-install",
            target: validTarget
        ) == nil,
        "pre-init delete accepted persisted identity without initialized config"
    )
    try check(
        PrivacyDeletePolicy.identifierForJournal(
            initialized: true,
            inMemory: "",
            persisted: "persisted-install",
            target: validTarget
        ) == "persisted-install",
        "initialized delete did not hydrate the persisted install identity"
    )
    try check(
        PrivacyDeletePolicy.identifierForJournal(
            initialized: true,
            inMemory: "",
            persisted: nil,
            target: validTarget
        ) == nil,
        "delete without an identifier was accepted"
    )
    try check(
        PrivacyDeletePolicy.identifierForJournal(
            initialized: true,
            inMemory: "memory-install",
            persisted: nil,
            target: PrivacyDeleteTarget(baseUrl: "https://api.example.test", appKey: "", companyKey: nil)
        ) == nil,
        "delete without a dispatchable app target was accepted"
    )
}

private func testInitialPrivacyPostureAppliesMigrationBeforeStartup() throws {
    let migrated = InitialPrivacyPosture.resolve(
        storedSuppression: nil,
        storedAdvertisingConsent: nil,
        storedThirdPartySharing: nil,
        initialEnabled: false,
        initialAdvertisingConsent: false,
        initialThirdPartySharing: false,
        advertisingHardBlocked: false,
        coppa: false
    )
    try check(!migrated.trackingEnabled, "legacy suppression was not applied at initialization")
    try check(!migrated.advertisingConsent, "legacy advertising opt-out was not applied")
    try check(migrated.thirdPartySharing == false, "legacy sharing opt-out was not applied")

    let restrictiveValueWins = InitialPrivacyPosture.resolve(
        storedSuppression: false,
        storedAdvertisingConsent: true,
        storedThirdPartySharing: true,
        initialEnabled: false,
        initialAdvertisingConsent: false,
        initialThirdPartySharing: false,
        advertisingHardBlocked: true,
        coppa: true
    )
    try check(!restrictiveValueWins.trackingEnabled, "failed wrapper suppression was ignored")
    try check(!restrictiveValueWins.advertisingConsent, "hard advertising gate was bypassed")
    try check(restrictiveValueWins.thirdPartySharing == false, "COPPA sharing gate was bypassed")
    try check(
        InitialPrivacyPosture.resolveConsent(
            stored: "granted",
            initial: "denied",
            requireConsent: false
        ) == "denied",
        "failed wrapper denial was overridden by an older native grant"
    )
    try check(
        !InitialPrivacyPosture.resolve(
            storedSuppression: false,
            storedAdvertisingConsent: true,
            storedThirdPartySharing: true,
            initialEnabled: true,
            initialAdvertisingConsent: true,
            initialThirdPartySharing: true,
            advertisingHardBlocked: false,
            coppa: false,
            durableTrackingSuppressed: true
        ).trackingEnabled,
        "durable tracking tombstone was ignored"
    )
    try check(
        InitialPrivacyPosture.resolveConsent(
            stored: "granted",
            initial: "granted",
            requireConsent: false,
            durableConsentDenied: true
        ) == "denied",
        "durable consent tombstone was ignored"
    )
}

private func testPendingDeleteStorePreservesMultipleLifetimesAndCompareRemove() throws {
    let suite = "reflect.privacy.pending.\(UUID().uuidString)"
    guard let defaults = UserDefaults(suiteName: suite) else {
        throw TestFailure.assertion("unable to create isolated defaults")
    }
    defer { defaults.removePersistentDomain(forName: suite) }
    let store = PendingPrivacyDeleteStore(defaults: defaults)
    let first = "first-install-uuid"
    let second = "second-install-uuid"
    let target = PrivacyDeleteTarget(
        baseUrl: "https://api.example.test/",
        appKey: "app-a",
        companyKey: "company-a"
    )

    let firstJournal = store.journalSuppression(first, target: target)
    let secondJournal = store.journalSuppression(second, target: target)
    try check(firstJournal.durable && firstJournal.identifier == first, "first intent not journaled")
    try check(secondJournal.durable && secondJournal.identifier == second, "second intent not journaled")
    try check(store.all() == [first, second], "pending identifiers were overwritten")
    try check(store.removeIfMatches(firstJournal.intent!), "confirmed first identifier not removed")
    try check(store.all() == [second], "older completion erased newer identifier")
    let repeated = store.journalSuppression("")
    try check(repeated.durable && repeated.identifier == second, "repeated delete lost existing intent")
    try check(defaults.bool(forKey: "reflect_suppressed"), "suppression was not journaled")
}

private func testPendingDeleteTargetsFenceConfigRotationAndExactRemoval() throws {
    let suite = "reflect.privacy.pending.targets.\(UUID().uuidString)"
    guard let defaults = UserDefaults(suiteName: suite) else {
        throw TestFailure.assertion("unable to create isolated defaults")
    }
    defer { defaults.removePersistentDomain(forName: suite) }
    let store = PendingPrivacyDeleteStore(defaults: defaults)
    let targetA = PrivacyDeleteTarget(
        baseUrl: "https://a.example.test///",
        appKey: "app-a",
        companyKey: "company-a"
    )
    let targetB = PrivacyDeleteTarget(
        baseUrl: "https://b.example.test",
        appKey: "app-b",
        companyKey: "company-b"
    )
    try check(targetA.baseUrl == "https://a.example.test", "delete target URL was not normalized")
    let equivalentTargetA = PrivacyDeleteTarget(
        baseUrl: " HTTPS://A.EXAMPLE.TEST:443/// ",
        appKey: " app-a ",
        companyKey: " company-a "
    )
    try check(equivalentTargetA == targetA,
              "equivalent scheme/host/default-port config missed persisted delete target")
    let legacyEncoded = Data(
        "{\"baseUrl\":\"HTTPS://A.EXAMPLE.TEST:443///\",\"appKey\":\" app-a \",\"companyKey\":\" company-a \"}".utf8
    )
    let decodedLegacyTarget = try JSONDecoder().decode(PrivacyDeleteTarget.self, from: legacyEncoded)
    try check(decodedLegacyTarget == targetA,
              "persisted pre-canonical target was not normalized during decode")
    try check(
        PrivacyDeleteTarget(
            baseUrl: "http://API.Example.Test:80/v1///",
            appKey: "app-a",
            companyKey: nil
        ).baseUrl == "http://api.example.test/v1",
        "HTTP default port or trailing path slash was not canonicalized"
    )
    try check(
        PrivacyDeleteTarget(
            baseUrl: "https://api.example.test:8443/v1/",
            appKey: "app-a",
            companyKey: nil
        ).baseUrl == "https://api.example.test:8443/v1",
        "non-default endpoint port was lost"
    )

    let first = store.journalSuppression("same-install", target: targetA)
    let second = store.journalSuppression("same-install", target: targetB)
    try check(first.durable && second.durable, "scoped delete intents were not durable")
    try check(store.all() == ["same-install"], "legacy identifier mirror duplicated an install")
    try check(store.allIntents().count == 2, "target-specific intents were collapsed")
    try check(PrivacyDeletePolicy.canDispatch(first.intent!, using: targetA), "matching target was rejected")
    try check(!PrivacyDeletePolicy.canDispatch(first.intent!, using: targetB), "rotated config accepted an old intent")
    try check(store.removeIfMatches(first.intent!), "exact target intent was not removed")
    try check(store.allIntents() == [second.intent!], "one completion erased another target's intent")

    let legacySuite = "reflect.privacy.pending.legacy.\(UUID().uuidString)"
    guard let legacyDefaults = UserDefaults(suiteName: legacySuite) else {
        throw TestFailure.assertion("unable to create legacy defaults")
    }
    defer { legacyDefaults.removePersistentDomain(forName: legacySuite) }
    legacyDefaults.set("legacy-install", forKey: "reflect_pending_delete")
    let legacy = PendingPrivacyDeleteStore(defaults: legacyDefaults).allIntents()
    try check(legacy.count == 1 && legacy[0].target == nil, "legacy intent was assigned the current config")
    try check(!PrivacyDeletePolicy.canDispatch(legacy[0], using: targetA), "unscoped legacy intent was dispatched")
}

private func testIndependentSuppressionTombstoneIsFailClosed() throws {
    let dir = FileManager.default.temporaryDirectory
        .appendingPathComponent("reflect-tombstone-\(UUID().uuidString)", isDirectory: true)
    defer { try? FileManager.default.removeItem(at: dir) }
    let url = dir.appendingPathComponent("privacy.json")
    let tombstone = PrivacySuppressionTombstone(url: url)
    try check(tombstone.read() == PrivacySuppressionState(), "new tombstone started restrictive")
    try check(tombstone.update(consentDenied: true), "consent tombstone write failed")
    try check(PrivacySuppressionTombstone(url: url).read().consentDenied,
              "consent tombstone did not survive reopen")
    try check(tombstone.update(trackingSuppressed: true, consentDenied: false),
              "tracking tombstone transition failed")
    let trackingOnly = PrivacySuppressionTombstone(url: url).read()
    try check(trackingOnly.trackingSuppressed && !trackingOnly.consentDenied,
              "tombstone flags were not updated independently")

    try Data("not-json".utf8).write(to: url, options: .atomic)
    let malformed = PrivacySuppressionTombstone(url: url).read()
    try check(malformed.trackingSuppressed && malformed.consentDenied,
              "malformed tombstone failed open")
    try check(tombstone.update(trackingSuppressed: false, consentDenied: false),
              "tombstone clear failed")
    try check(!FileManager.default.fileExists(atPath: url.path), "empty tombstone was retained")
}

private func testFailedDeleteJournalDoesNotClaimDurability() throws {
    let suite = "reflect.privacy.pending.failure.\(UUID().uuidString)"
    guard let defaults = UserDefaults(suiteName: suite) else {
        throw TestFailure.assertion("unable to create isolated defaults")
    }
    defer { defaults.removePersistentDomain(forName: suite) }
    let store = PendingPrivacyDeleteStore(
        defaults: defaults,
        synchronizeDefaults: { _ in false }
    )
    let journal = store.journalSuppression("must-retain")
    try check(!journal.durable, "failed synchronization was reported durable")
    try check(journal.identifier == "must-retain", "failed journal lost in-memory identifier")
}

private func testCleanupSurvivesRestartAndPreservesPendingDelete() throws {
    let suite = "reflect.privacy.tests.\(UUID().uuidString)"
    guard let defaults = UserDefaults(suiteName: suite) else {
        throw TestFailure.assertion("unable to create isolated defaults")
    }
    defer { defaults.removePersistentDomain(forName: suite) }
    for key in PrivacyPersistence.allReflectKeys { defaults.set("seed", forKey: key) }

    let dir = FileManager.default.temporaryDirectory
        .appendingPathComponent("reflect-privacy-\(UUID().uuidString)", isDirectory: true)
    try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: dir) }
    let queueURL = dir.appendingPathComponent("reflect_queue.jsonl")
    try "{\"event_id\":\"old\"}".write(to: queueURL, atomically: true, encoding: .utf8)

    PrivacyPersistence.clearIdentityAndEvents(
        defaults: defaults,
        queueURL: queueURL,
        preservePendingDelete: true
    )
    defaults.synchronize()
    guard let restarted = UserDefaults(suiteName: suite) else {
        throw TestFailure.assertion("unable to reopen defaults")
    }
    try check(restarted.object(forKey: "reflect_install_uuid") == nil, "install id survived restart")
    try check(restarted.object(forKey: "reflect_session_id") == nil, "session id survived restart")
    try check(restarted.object(forKey: "reflect_pending_attr") == nil, "pending attribution survived restart")
    try check(restarted.object(forKey: "reflect_pending_delete") != nil, "required delete retry was erased")
    try check(restarted.object(forKey: "reflect_pending_delete_contexts_v2") != nil,
              "required target-bound delete context was erased")
    try check(!FileManager.default.fileExists(atPath: queueURL.path), "event queue survived restart cleanup")
}

private func testSuccessfulDeleteCleanupRemovesPendingIdentifier() throws {
    let suite = "reflect.privacy.tests.\(UUID().uuidString)"
    guard let defaults = UserDefaults(suiteName: suite) else {
        throw TestFailure.assertion("unable to create isolated defaults")
    }
    defer { defaults.removePersistentDomain(forName: suite) }
    defaults.set("install-id", forKey: "reflect_install_uuid")
    defaults.set("delete-id", forKey: "reflect_pending_delete")
    defaults.set("[]", forKey: "reflect_pending_delete_contexts_v2")
    PrivacyPersistence.clearIdentityAndEvents(
        defaults: defaults,
        queueURL: nil,
        includePrivacyChoices: true,
        preservePendingDelete: false
    )
    defaults.synchronize()
    try check(defaults.object(forKey: "reflect_install_uuid") == nil, "delete retained install id")
    try check(defaults.object(forKey: "reflect_pending_delete") == nil, "successful delete retained retry id")
    try check(defaults.object(forKey: "reflect_pending_delete_contexts_v2") == nil,
              "successful delete retained retry target context")
}

private func testAttributionClickRetentionBoundaryAndScrub() throws {
    let expiry: Int64 = 10_000
    try check(!AttributionRetention.isExpired(expiresAtMs: expiry, nowMs: 10_999),
              "click context expired before the server's strict second boundary")
    try check(AttributionRetention.isExpired(expiresAtMs: expiry, nowMs: 11_000),
              "click context survived past the server's strict second boundary")

    let original = #"{"type":"click","partner":"network","campaign":"summer","clickId":"secret-click"}"#
    let scrubbed = AttributionRetention.scrubClickId(
        original,
        expiresAtMs: expiry,
        nowMs: 11_000
    )
    guard let scrubbed,
          let data = scrubbed.data(using: .utf8),
          let object = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
        throw TestFailure.assertion("valid expired attribution did not produce a valid cache")
    }
    try check(object["clickId"] == nil, "expired clickId survived local cache scrub")
    try check(object["partner"] as? String == "network", "coarse attribution outcome was removed")
    try check(AttributionRetention.hasClickId(original), "legacy click cache was not detected")
    try check(!AttributionRetention.hasClickId(#"{"campaign":"summer"}"#),
              "campaign-only cache was treated as unique click context")
    try check(
        AttributionRetention.scrubClickId("{", expiresAtMs: expiry, nowMs: 11_000) == nil,
        "malformed expired attribution failed open"
    )
    let unknownExpiry = AttributionRetention.scrubClickId(
        original,
        expiresAtMs: 0,
        nowMs: 1_000,
        failClosedWhenExpiryUnknown: true
    )
    try check(
        unknownExpiry?.contains("secret-click") == false,
        "legacy click cache with no expiry remained exposed offline"
    )

    let queued = #"{"event_name":"deep_link_opened","event_ts_ms":1000,"properties":{"url":"myapp://open/path?click_id=secret&sub1=x#frag","path":"/path?gclid=secret","source":"DIRECT","is_reattribution":true,"dl_click_id":"secret","dl_custom":"x","click_id":"secret","campaign":"summer","global":"keep"},"props":{"url":{"gclid":"secret"},"path":"/safe?gclid=secret","source":{"click_id":"secret"},"is_reattribution":"yes"}}"#
    guard let safeQueued = AttributionRetention.scrubQueuedEvent(queued, nowMs: 2_000),
          let safeData = safeQueued.data(using: .utf8),
          let safeEvent = try? JSONSerialization.jsonObject(with: safeData) as? [String: Any],
          let safeProps = safeEvent["properties"] as? [String: Any] else {
        throw TestFailure.assertion("deep-link queue scrub returned invalid JSON")
    }
    try check(safeProps["url"] as? String == "myapp://open/path", "deep-link query survived queue scrub")
    try check(
        safeProps["path"] as? String == "/path" &&
            safeProps["source"] as? String == "direct" &&
            safeProps["is_reattribution"] as? Bool == true,
        "deep-link closed schema lost its safe typed fields"
    )
    try check(safeProps["click_id"] == nil && safeProps["dl_custom"] == nil,
              "deep-link click/query copies survived queue scrub")
    try check(safeProps["campaign"] == nil && safeProps["global"] == nil,
              "non-allowlisted deep-link properties survived durable queue scrub")
    guard let legacyProps = safeEvent["props"] as? [String: Any] else {
        throw TestFailure.assertion("legacy deep-link props were not retained")
    }
    try check(
        legacyProps["path"] as? String == "/safe" &&
            legacyProps["url"] == nil &&
            legacyProps["source"] == nil &&
            legacyProps["is_reattribution"] == nil,
        "malformed typed deep-link fields survived the closed schema"
    )
    try check(
        AttributionRetention.urlWithoutQueryOrFragment(
            "myapp://open/path?click_id=secret&campaign=summer&sub1=x#frag"
        ) == "myapp://open/path",
        "replayable deep-link cache retained query or fragment context"
    )

    let referralEvent = #"{"event_name":"app_install","event_ts_ms":86402000,"referral":{"source":"play_install_referrer","click_ts":86401,"raw":"click_id=secret","attribution_token":"token","campaign":"drop","gclid":"drop","custom_id":"drop"}}"#
    guard let safeReferralJSON = AttributionRetention.scrubQueuedEvent(
        referralEvent,
        nowMs: 86_403_000
    ),
          let safeReferralData = safeReferralJSON.data(using: .utf8),
          let safeReferralEvent = try? JSONSerialization.jsonObject(with: safeReferralData) as? [String: Any],
          let safeReferral = safeReferralEvent["referral"] as? [String: Any] else {
        throw TestFailure.assertion("referral queue scrub returned invalid JSON")
    }
    try check(safeReferral["raw"] == nil && safeReferral["attribution_token"] == nil,
              "durable referral retained transient attribution input")
    try check(
        safeReferral["campaign"] == nil &&
            safeReferral["gclid"] == nil &&
            safeReferral["custom_id"] == nil,
        "durable referral retained an arbitrary unique field"
    )
    try check(
        safeReferral["source"] as? String == "play_install_referrer" &&
            (safeReferral["click_ts"] as? Int64 ?? Int64(safeReferral["click_ts"] as? Int ?? -1)) == 86_400,
        "durable referral did not retain only coarse attribution dimensions"
    )
    try check(
        AttributionRetention.hasTransientAttributionContext(referralEvent),
        "raw install referral was not detected for the immediate memory-only send"
    )
    try check(
        !AttributionRetention.hasTransientAttributionContext(safeReferralJSON),
        "coarse queue row was treated as transient attribution context"
    )
    try check(AttributionRetention.scrubQueuedEvent("{") == nil, "malformed queue row failed open")
}

private func testRecursiveQueueRetentionAndExactSourceAge() throws {
    let now: Int64 = 1_000_000
    let event: [String: Any] = [
        "event_name": "purchase",
        "event_ts_ms": now,
        "properties": [
            "safe": "keep",
            "nested": [
                "G-CLID": "secret",
                "landing_url": "https://example.com/p?gclid=secret#frag",
            ],
            "items": [[
                "externalClickId": "secret",
                "redirect_link": "myapp://go?sub1=x",
            ]],
        ],
        "props": ["fbclid": "secret", "path": "/safe"],
        "device": ["msclkid": "secret", "model": "phone"],
        "partner_params": ["sub_1": "secret", "partner": "keep"],
        "callback_params": [
            "customer_clid": "secret",
            "callback_url": "https://cb.test/a?x=1",
        ],
        "user_properties": ["wbraid": "secret", "tier": "paid"],
    ]
    let data = try JSONSerialization.data(withJSONObject: event)
    let json = String(decoding: data, as: UTF8.self)
    guard let scrubbed = AttributionRetention.scrubQueuedEvent(json, nowMs: now),
          let safeData = scrubbed.data(using: .utf8),
          let safe = try JSONSerialization.jsonObject(with: safeData) as? [String: Any],
          let properties = safe["properties"] as? [String: Any],
          let nested = properties["nested"] as? [String: Any],
          let items = properties["items"] as? [[String: Any]],
          let device = safe["device"] as? [String: Any],
          let partner = safe["partner_params"] as? [String: Any],
          let callback = safe["callback_params"] as? [String: Any],
          let user = safe["user_properties"] as? [String: Any] else {
        throw TestFailure.assertion("recursive queue scrub returned invalid JSON")
    }
    try check(properties["safe"] as? String == "keep", "safe property was removed")
    try check(nested["G-CLID"] == nil, "nested click key survived")
    try check(nested["landing_url"] as? String == "https://example.com/p",
              "nested URL query survived")
    try check(items.first?["externalClickId"] == nil, "array click key survived")
    try check(items.first?["redirect_link"] as? String == "myapp://go",
              "array URL query survived")
    try check(device["msclkid"] == nil && device["model"] as? String == "phone",
              "device bag was not sanitized")
    try check(partner["sub_1"] == nil && partner["partner"] as? String == "keep",
              "partner bag was not sanitized")
    try check(callback["customer_clid"] == nil &&
              callback["callback_url"] as? String == "https://cb.test/a",
              "callback bag was not sanitized")
    try check(user["wbraid"] == nil && user["tier"] as? String == "paid",
              "user-property bag was not sanitized")
    try check(!scrubbed.contains("secret"), "click value survived queue serialization")

    let sourceAtMs: Int64 = 1_000
    let expiryMs = sourceAtMs + AttributionRetention.clickContextRetentionMs
    let sourceEvent = #"{"event_name":"offline","event_ts_ms":1000}"#
    try check(
        AttributionRetention.scrubQueuedEvent(sourceEvent, nowMs: expiryMs + 999) != nil,
        "queue row expired before the strict second boundary"
    )
    try check(
        AttributionRetention.scrubQueuedEvent(sourceEvent, nowMs: expiryMs + 1_000) == nil,
        "queue row survived past the strict source boundary"
    )
    try check(
        AttributionRetention.scrubQueuedEvent(
            #"{"event_name":"missing_source"}"#,
            nowMs: sourceAtMs
        ) == nil,
        "missing source timestamp failed open"
    )
    try check(
        AttributionRetention.scrubQueuedEvent(
            #"{"event_name":"future","event_ts_ms":3602000}"#,
            nowMs: sourceAtMs
        ) == nil,
        "unbounded future source timestamp failed open"
    )
}

private func testEphemeralParameterSanitizationAndLegacyRestartCleanup() throws {
    let sourceAtMs: Int64 = 1_000
    let freshSourceAtMs: Int64 = 2_000
    let oldExpiryMs = sourceAtMs + AttributionRetention.clickContextRetentionMs

    try check(
        AttributionRetention.sanitizeEphemeralParameter(
            key: "gclid",
            value: "drop"
        ) == nil,
        "click-key global value survived"
    )
    try check(
        AttributionRetention.sanitizeEphemeralParameter(
            key: "landing_url",
            value: "https://example.com/path?click_id=secret#frag"
        ) as? String == "https://example.com/path",
        "ephemeral URL retained query context"
    )
    let nested = AttributionRetention.sanitizeEphemeralParameter(
        key: "nested",
        value: [
            "safe": "yes",
            "fbclid": "drop",
            "next_url": "https://example.com/next?sub1=secret",
        ]
    ) as? [String: Any]
    try check(nested?["safe"] as? String == "yes", "safe nested value was lost")
    try check(nested?["fbclid"] == nil, "nested click key survived")
    try check(
        nested?["next_url"] as? String == "https://example.com/next",
        "nested URL retained query context"
    )

    try check(
        AttributionRetention.isSourceTimestampRetained(
            sourceAtMs: sourceAtMs,
            nowMs: oldExpiryMs + 999
        ),
        "exact source-age boundary expired early"
    )
    try check(
        !AttributionRetention.isSourceTimestampRetained(
            sourceAtMs: sourceAtMs,
            nowMs: oldExpiryMs + 1_000
        ),
        "source-age boundary survived the next whole second"
    )
    try check(
        AttributionRetention.isSourceTimestampRetained(
            sourceAtMs: freshSourceAtMs,
            nowMs: oldExpiryMs + 1_000
        ),
        "independent newer source inherited the old expiry"
    )

    let suiteName = "reflect-retention-\(UUID().uuidString)"
    guard let defaults = UserDefaults(suiteName: suiteName) else {
        throw TestFailure.assertion("could not create isolated UserDefaults suite")
    }
    defer { defaults.removePersistentDomain(forName: suiteName) }
    defaults.set(#"{"cohort":"legacy","gclid":"secret"}"#,
                 forKey: "reflect_global_props")
    defaults.set(#"{"partner_uid":"legacy","sub1":"secret"}"#,
                 forKey: "reflect_partner_params")
    AttributionRetention.clearLegacyParameterPersistence(defaults: defaults)
    try check(
        defaults.object(forKey: "reflect_global_props") == nil &&
            defaults.object(forKey: "reflect_partner_params") == nil,
        "cold-start cleanup left legacy parameter bytes on disk"
    )
}

/// Regression: a first-install batch refused by iOS tracking-domain policy was
/// classified as ordinary network flakiness, so it burned exponential backoff
/// (persisted as a wall-clock deadline) and nothing retried when the user
/// answered the ATT prompt — the events shipped only on the next launch.
private func testAttBlockedTransportIsNotTreatedAsNetworkFlakiness() throws {
    // The exact refusal iOS delivers for a host in NSPrivacyTrackingDomains
    // while the prompt is unanswered.
    try check(
        AttTransportPolicy.classify(
            errorDomain: NSURLErrorDomain,
            errorCode: NSURLErrorNotConnectedToInternet,
            attStatus: "not_determined"
        ) == .attBlocked,
        "refusal behind an unanswered ATT prompt must not take the backoff path"
    )

    // A missing cached status is the pre-snapshot startup window, which is
    // exactly when the install batch is sent. Treat it as undecided.
    try check(
        AttTransportPolicy.classify(
            errorDomain: NSURLErrorDomain,
            errorCode: NSURLErrorNotConnectedToInternet,
            attStatus: nil
        ) == .attBlocked,
        "unknown ATT status must be treated as undecided, not as flakiness"
    )

    // Once the prompt is answered the gate is no longer an explanation, so a
    // refusal is a real fault and must keep its exponential backoff.
    for decided in ["authorized", "denied", "restricted"] {
        try check(
            AttTransportPolicy.classify(
                errorDomain: NSURLErrorDomain,
                errorCode: NSURLErrorNotConnectedToInternet,
                attStatus: decided
            ) == .retry,
            "post-decision offline error must stay on ordinary backoff (\(decided))"
        )
    }

    // Other transport faults are ordinary retries even while undecided —
    // only the tracking-domain refusal code is special.
    try check(
        AttTransportPolicy.classify(
            errorDomain: NSURLErrorDomain,
            errorCode: NSURLErrorTimedOut,
            attStatus: "not_determined"
        ) == .retry,
        "a timeout is flakiness, not a tracking-domain refusal"
    )
    try check(
        AttTransportPolicy.classify(
            errorDomain: "SomeOtherDomain",
            errorCode: NSURLErrorNotConnectedToInternet,
            attStatus: "not_determined"
        ) == .retry,
        "a matching code from a foreign error domain must not be claimed"
    )
}

/// Regression: the ATT answer is the only signal that reopens a blocked
/// tracking domain (no NWPath transition ever fires), but it must not be
/// confused with merely observing an already-answered prompt at launch.
private func testAttPromptResolutionFiresOnlyOnRealTransition() throws {
    try check(
        AttTransportPolicy.isPromptResolution(previous: "not_determined", current: "authorized"),
        "granting the prompt must reopen transport"
    )
    // A denial does not unblock the domain, but it does end the wait — the
    // queue must stop parking and return to ordinary backoff.
    try check(
        AttTransportPolicy.isPromptResolution(previous: "not_determined", current: "denied"),
        "denying the prompt must still end the parked wait"
    )
    try check(
        AttTransportPolicy.isPromptResolution(previous: "not_determined", current: "restricted"),
        "a restricted decision must still end the parked wait"
    )

    // The load-bearing negative: every launch of an app whose user answered
    // long ago starts with no cached value. Treating that first observation as
    // a resolution would wipe the server-outage backoff that
    // restorePersistedBackoff() exists to carry across restarts.
    try check(
        !AttTransportPolicy.isPromptResolution(previous: nil, current: "authorized"),
        "first observation of an already-answered prompt must not clear backoff"
    )
    try check(
        !AttTransportPolicy.isPromptResolution(previous: "authorized", current: "authorized"),
        "an unchanged status must not re-kick transport"
    )
    try check(
        !AttTransportPolicy.isPromptResolution(previous: "not_determined", current: "not_determined"),
        "a still-unanswered prompt is not a resolution"
    )
    try check(
        !AttTransportPolicy.isPromptResolution(previous: "not_determined", current: nil),
        "losing the cached status is not a resolution"
    )
}

do {
    try testBlockCancelsRegisteredWorkAndInvalidatesPermit()
    try testRegistrationVersusBlockRaceNeverLeavesLiveWork()
    try testStaleBuildPermitCannotCrossQuickReopen()
    try testSensitiveReadIsLinearizedAndCannotRouteAfterQuickReopen()
    try testDelayedCallbackCannotCrossPrivacyBoundary()
    try testDeleteRequiresAuthenticationAndExplicitReceipt()
    try testDeleteJournalRequiresInitializedTargetAndIdentifier()
    try testInitialPrivacyPostureAppliesMigrationBeforeStartup()
    try testPendingDeleteStorePreservesMultipleLifetimesAndCompareRemove()
    try testPendingDeleteTargetsFenceConfigRotationAndExactRemoval()
    try testIndependentSuppressionTombstoneIsFailClosed()
    try testFailedDeleteJournalDoesNotClaimDurability()
    try testCleanupSurvivesRestartAndPreservesPendingDelete()
    try testSuccessfulDeleteCleanupRemovesPendingIdentifier()
    try testAttributionClickRetentionBoundaryAndScrub()
    try testRecursiveQueueRetentionAndExactSourceAge()
    try testEphemeralParameterSanitizationAndLegacyRestartCleanup()
    try testAttBlockedTransportIsNotTreatedAsNetworkFlakiness()
    try testAttPromptResolutionFiresOnlyOnRealTransition()
    print("Reflect iOS privacy transport tests: 19 passed (1,001 race iterations)")
} catch {
    fputs("Reflect iOS privacy transport tests FAILED: \(error)\n", stderr)
    exit(1)
}
