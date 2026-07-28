import Foundation

/// Deterministic startup posture used before any identity, device snapshot, or
/// automatic event work. A restrictive value in either the native store or a
/// wrapper migration mirror wins. Wrappers relax their mirror only after native
/// acknowledgement, so failed opt-outs remain fail-closed across restart.
struct InitialPrivacyPosture: Equatable {
    let trackingEnabled: Bool
    let advertisingConsent: Bool
    let thirdPartySharing: Bool?

    static func resolve(
        storedSuppression: Bool?,
        storedAdvertisingConsent: Bool?,
        storedThirdPartySharing: Bool?,
        initialEnabled: Bool?,
        initialAdvertisingConsent: Bool?,
        initialThirdPartySharing: Bool?,
        advertisingHardBlocked: Bool,
        coppa: Bool,
        durableTrackingSuppressed: Bool = false
    ) -> InitialPrivacyPosture {
        let enabled = !durableTrackingSuppressed && storedSuppression != true && initialEnabled != false
        let advertisingChoice = storedAdvertisingConsent != false && initialAdvertisingConsent != false
        let sharingChoice: Bool? = if storedThirdPartySharing == nil && initialThirdPartySharing == nil {
            nil
        } else {
            storedThirdPartySharing != false && initialThirdPartySharing != false
        }
        return InitialPrivacyPosture(
            trackingEnabled: enabled,
            advertisingConsent: !advertisingHardBlocked && advertisingChoice,
            thirdPartySharing: coppa ? false : sharingChoice
        )
    }

    static func resolveConsent(
        stored: String?,
        initial: String?,
        requireConsent: Bool,
        durableConsentDenied: Bool = false
    ) -> String {
        if durableConsentDenied { return "denied" }
        if stored == "denied" || initial == "denied" { return "denied" }
        if stored == "granted" || initial == "granted" { return "granted" }
        return requireConsent ? "denied" : "granted"
    }
}

/// Linearizable privacy gate for every non-deletion SDK request.
///
/// A request takes a permit and registers its concrete task immediately before
/// resume(). A privacy transition invalidates old permits and synchronously
/// invokes cancellation for every task that won the registration race.
final class PrivacyTransportGate {
    struct Permit: Equatable { fileprivate let generation: UInt64 }

    private let lock = NSLock()
    private var allowed: Bool
    private var generation: UInt64 = 0
    private var active: [ObjectIdentifier: () -> Void] = [:]

    init(initiallyAllowed: Bool = false) {
        allowed = initiallyAllowed
    }

    func allow() {
        lock.lock()
        if !allowed {
            generation &+= 1
            allowed = true
        }
        lock.unlock()
    }

    func block() {
        lock.lock()
        allowed = false
        generation &+= 1
        let cancellations = Array(active.values)
        active.removeAll()
        lock.unlock()

        // Cancellation may synchronously invoke a completion that unregisters.
        // Run it outside the lock to avoid callback deadlocks.
        for cancel in cancellations { cancel() }
    }

    func permit() -> Permit? {
        lock.lock(); defer { lock.unlock() }
        return allowed ? Permit(generation: generation) : nil
    }

    func isValid(_ permit: Permit) -> Bool {
        lock.lock(); defer { lock.unlock() }
        return allowed && permit.generation == generation
    }

    /// Returns false when a privacy transition won the registration race.
    func register(_ token: AnyObject, permit: Permit, cancel: @escaping () -> Void) -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard allowed && permit.generation == generation else { return false }
        active[ObjectIdentifier(token)] = cancel
        return true
    }

    func unregister(_ token: AnyObject) {
        lock.lock()
        active.removeValue(forKey: ObjectIdentifier(token))
        lock.unlock()
    }

    /// Runs a short persistence mutation in the same critical section as block().
    func runIfValid(_ permit: Permit, _ action: () -> Void) -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard allowed && permit.generation == generation else { return false }
        action()
        return true
    }

    var activeCountForTesting: Int {
        lock.lock(); defer { lock.unlock() }
        return active.count
    }
}

/// Strict acceptance policy for the dedicated, authenticated privacy route.
enum PrivacyDeletePolicy {
    static func signingSecret(_ raw: String?) -> String? {
        guard let raw = raw, !raw.isEmpty else { return nil }
        return raw
    }

    static func isAccepted(statusCode: Int, data: Data?) -> Bool {
        guard (200..<300).contains(statusCode), let data = data,
              let response = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any],
              response["ok"] as? Bool == true,
              response["queued"] as? Bool == true else { return false }
        return true
    }

    static func canDispatch(
        _ intent: PendingPrivacyDeleteIntent,
        using currentTarget: PrivacyDeleteTarget
    ) -> Bool {
        guard let target = intent.target else { return false }
        return target.isDispatchable && target == currentTarget
    }

    /// Resolve the identifier at the deletion crash boundary. The in-memory
    /// field is intentionally not authoritative before initialization: an app
    /// restart can have a durable install UUID that the new core has not yet
    /// hydrated. Missing configuration/target/identity is never a successful
    /// no-op because that would falsely acknowledge an unjournaled erasure.
    static func identifierForJournal(
        initialized: Bool,
        inMemory: String,
        persisted: String?,
        target: PrivacyDeleteTarget
    ) -> String? {
        guard initialized, target.isDispatchable else { return nil }
        let candidate = inMemory.isEmpty ? (persisted ?? "") : inMemory
        return candidate.isEmpty ? nil : candidate
    }
}

/// Durable FIFO/set of unconfirmed identifiers. Legacy single-string values are
/// read transparently and migrated only when a second identifier is added.
struct PrivacyDeleteJournal: Equatable {
    let durable: Bool
    let intent: PendingPrivacyDeleteIntent?

    var identifier: String? { intent?.identifier }
}

/// The immutable tenant/endpoint scope under which a privacy deletion was
/// requested. A retry is legal only when the active configuration matches this
/// target exactly; signing an older identifier for a newly configured app could
/// acknowledge deletion in the wrong tenant and orphan the real request.
struct PrivacyDeleteTarget: Codable, Equatable, Hashable {
    let baseUrl: String
    let appKey: String
    let companyKey: String?

    init(baseUrl: String, appKey: String, companyKey: String?) {
        self.baseUrl = Self.normalizeBaseUrl(baseUrl)
        self.appKey = appKey.trimmingCharacters(in: .whitespacesAndNewlines)
        self.companyKey = companyKey
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .flatMap { $0.isEmpty ? nil : $0 }
    }

    var isDispatchable: Bool { !baseUrl.isEmpty && !appKey.isEmpty }

    private enum CodingKeys: String, CodingKey {
        case baseUrl, appKey, companyKey
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        self.init(
            baseUrl: try values.decode(String.self, forKey: .baseUrl),
            appKey: try values.decode(String.self, forKey: .appKey),
            companyKey: try values.decodeIfPresent(String.self, forKey: .companyKey)
        )
    }

    func encode(to encoder: Encoder) throws {
        var values = encoder.container(keyedBy: CodingKeys.self)
        try values.encode(baseUrl, forKey: .baseUrl)
        try values.encode(appKey, forKey: .appKey)
        try values.encodeIfPresent(companyKey, forKey: .companyKey)
    }

    /// Match Android's persisted-target canonicalization so equivalent config
    /// spellings do not strand an otherwise valid deletion journal after restart.
    private static func normalizeBaseUrl(_ raw: String) -> String {
        var trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        while trimmed.hasSuffix("/") { trimmed.removeLast() }
        guard !trimmed.isEmpty,
              let components = URLComponents(string: trimmed),
              let rawScheme = components.scheme,
              let rawHost = components.host,
              components.query == nil,
              components.fragment == nil else { return trimmed }

        let scheme = rawScheme.lowercased()
        let hostValue = rawHost.lowercased()
        let host = hostValue.contains(":") && !hostValue.hasPrefix("[")
            ? "[\(hostValue)]"
            : hostValue
        let port = ((scheme == "https" && components.port == 443) ||
                    (scheme == "http" && components.port == 80))
            ? nil
            : components.port
        var authority = ""
        if let user = components.percentEncodedUser {
            authority += user
            if let password = components.percentEncodedPassword { authority += ":\(password)" }
            authority += "@"
        }
        authority += host
        if let port = port { authority += ":\(port)" }
        var path = components.percentEncodedPath
        while path.hasSuffix("/") { path.removeLast() }
        return "\(scheme)://\(authority)\(path)"
    }
}

struct PendingPrivacyDeleteIntent: Codable, Equatable, Hashable {
    let identifier: String
    let target: PrivacyDeleteTarget?
}

final class PendingPrivacyDeleteStore {
    private let defaults: UserDefaults
    private let key: String
    private let contextsKey: String
    private let synchronizeDefaults: (UserDefaults) -> Bool
    private let lock = NSLock()

    init(
        defaults: UserDefaults,
        key: String = "reflect_pending_delete",
        contextsKey: String? = nil,
        synchronizeDefaults: @escaping (UserDefaults) -> Bool = { $0.synchronize() }
    ) {
        self.defaults = defaults
        self.key = key
        self.contextsKey = contextsKey ?? "\(key)_contexts_v2"
        self.synchronizeDefaults = synchronizeDefaults
    }

    func all() -> [String] {
        lock.lock(); defer { lock.unlock() }
        return allLocked()
    }

    func allIntents() -> [PendingPrivacyDeleteIntent] {
        lock.lock(); defer { lock.unlock() }
        return allIntentsLocked()
    }

    /// Atomically latches suppression and pending remote deletion before local
    /// identity cleanup. synchronize() is intentional at this crash boundary.
    func journalSuppression(
        _ candidate: String,
        target: PrivacyDeleteTarget? = nil,
        suppressionKey: String = "reflect_suppressed"
    ) -> PrivacyDeleteJournal {
        lock.lock(); defer { lock.unlock() }
        var pending = allIntentsLocked()
        let candidateIntent = candidate.isEmpty ? nil : PendingPrivacyDeleteIntent(
            identifier: candidate,
            target: target
        )
        if let candidateIntent = candidateIntent, !pending.contains(candidateIntent) {
            pending.append(candidateIntent)
        }
        defaults.set(true, forKey: suppressionKey)
        writeLocked(pending)
        return PrivacyDeleteJournal(
            durable: synchronizeDefaults(defaults),
            intent: candidateIntent ?? pending.first
        )
    }

    @discardableResult
    func removeIfMatches(_ expected: PendingPrivacyDeleteIntent) -> Bool {
        lock.lock(); defer { lock.unlock() }
        var pending = allIntentsLocked()
        guard let index = pending.firstIndex(of: expected) else { return false }
        pending.remove(at: index)
        writeLocked(pending)
        return true
    }

    private func allIntentsLocked() -> [PendingPrivacyDeleteIntent] {
        var intents: [PendingPrivacyDeleteIntent] = []
        if let raw = defaults.string(forKey: contextsKey),
           let data = raw.data(using: .utf8),
           let decoded = try? JSONDecoder().decode([PendingPrivacyDeleteIntent].self, from: data) {
            for intent in decoded where !intent.identifier.isEmpty && !intents.contains(intent) {
                intents.append(intent)
            }
        }
        // The legacy identifier key remains a compatibility/index mirror. Any
        // identifier without a v2 context is deliberately unscoped and therefore
        // non-dispatchable until the app can re-journal it with known config.
        for identifier in allLocked() where !intents.contains(where: { $0.identifier == identifier }) {
            intents.append(PendingPrivacyDeleteIntent(identifier: identifier, target: nil))
        }
        return intents
    }

    private func allLocked() -> [String] {
        guard let raw = defaults.string(forKey: key), !raw.isEmpty else { return [] }
        guard raw.first == "[", let data = raw.data(using: .utf8),
              let values = try? JSONSerialization.jsonObject(with: data) as? [String] else {
            return [raw]
        }
        var seen = Set<String>()
        return values.filter { !$0.isEmpty && seen.insert($0).inserted }
    }

    private func writeLocked(_ pending: [PendingPrivacyDeleteIntent]) {
        var identifiers: [String] = []
        for intent in pending where !identifiers.contains(intent.identifier) {
            identifiers.append(intent.identifier)
        }
        switch identifiers.count {
        case 0:
            defaults.removeObject(forKey: key)
        case 1:
            defaults.set(identifiers[0], forKey: key)
        default:
            let data = try? JSONSerialization.data(withJSONObject: identifiers)
            defaults.set(data.flatMap { String(data: $0, encoding: .utf8) }, forKey: key)
        }
        if pending.isEmpty {
            defaults.removeObject(forKey: contextsKey)
        } else if let data = try? JSONEncoder().encode(pending) {
            defaults.set(String(data: data, encoding: .utf8), forKey: contextsKey)
        }
    }
}

/// Independent file-backed restrictive posture. This is intentionally separate
/// from UserDefaults so a failed synchronize cannot reopen collection after a
/// process restart. Malformed state is interpreted as maximally restrictive.
struct PrivacySuppressionState: Codable, Equatable {
    var trackingSuppressed = false
    var consentDenied = false
}

final class PrivacySuppressionTombstone {
    private let url: URL?
    private let fileManager: FileManager
    private let lock = NSLock()

    init(url: URL?, fileManager: FileManager = .default) {
        self.url = url
        self.fileManager = fileManager
    }

    func read() -> PrivacySuppressionState {
        lock.lock(); defer { lock.unlock() }
        return readLocked()
    }

    @discardableResult
    func update(trackingSuppressed: Bool? = nil, consentDenied: Bool? = nil) -> Bool {
        lock.lock(); defer { lock.unlock() }
        guard let url = url else { return false }
        var state = readLocked()
        if let trackingSuppressed = trackingSuppressed { state.trackingSuppressed = trackingSuppressed }
        if let consentDenied = consentDenied { state.consentDenied = consentDenied }
        do {
            try fileManager.createDirectory(at: url.deletingLastPathComponent(),
                                            withIntermediateDirectories: true)
            if !state.trackingSuppressed && !state.consentDenied {
                if fileManager.fileExists(atPath: url.path) { try fileManager.removeItem(at: url) }
                return true
            }
            let data = try JSONEncoder().encode(state)
            try data.write(to: url, options: .atomic)
            return true
        } catch {
            return false
        }
    }

    private func readLocked() -> PrivacySuppressionState {
        guard let url = url, fileManager.fileExists(atPath: url.path) else {
            return PrivacySuppressionState()
        }
        guard let data = try? Data(contentsOf: url),
              let state = try? JSONDecoder().decode(PrivacySuppressionState.self, from: data) else {
            return PrivacySuppressionState(trackingSuppressed: true, consentDenied: true)
        }
        return state
    }
}

/// One place for privacy-critical restart cleanup. Keeping this independent of
/// UIKit makes the exact disk/defaults behavior deterministic in unit tests.
enum PrivacyPersistence {
    private static let identityKeys = [
        "reflect_install_uuid", "reflect_install_reported",
        "reflect_attribution_json", "reflect_attr_watermark",
        "reflect_attr_click_context_expires_at_ms",
        "reflect_first_install_ms", "reflect_session_count",
        "reflect_subsession_count", "reflect_session_active_ms",
        "reflect_session_open", "reflect_session_id",
        "reflect_last_activity_wall", "reflect_global_props",
        "reflect_partner_params", "reflect_pending_attr",
        "reflect_pending_deferred_dl", "reflect_coppa_tps_sent",
        "reflect_launch_url", "reflect_launch_url_expires_at_ms", "reflect_backoff_ms",
        "reflect_backoff_deadline",
        // Automatic SKAdNetwork conversion-value state. MUST be cleared with the
        // rest of the identity: a surviving revenue total would let an erased
        // user's spend history set the conversion value of the NEXT install on
        // this device, leaking their behaviour into someone else's postback.
        "reflect_skan_revenue_total", "reflect_skan_last_fine",
        "reflect_skan_cv_schema_json", "reflect_skan_cv_schema_at",
    ]

    static let allReflectKeys = identityKeys + [
        "reflect_ad_consent", "reflect_consent_state",
        "reflect_pending_delete", "reflect_pending_delete_contexts_v2", "reflect_suppressed",
        "reflect_third_party_sharing",
    ]

    static func clearIdentityAndEvents(
        defaults: UserDefaults,
        queueURL: URL?,
        includePrivacyChoices: Bool = false,
        preservePendingDelete: Bool = true
    ) {
        var keys = identityKeys
        if includePrivacyChoices {
            keys += ["reflect_ad_consent", "reflect_consent_state", "reflect_third_party_sharing"]
        }
        if !preservePendingDelete {
            keys += ["reflect_pending_delete", "reflect_pending_delete_contexts_v2"]
        }
        for key in keys { defaults.removeObject(forKey: key) }

        if let queueURL = queueURL {
            try? FileManager.default.removeItem(at: queueURL)
            try? FileManager.default.removeItem(at: queueURL.appendingPathExtension("tmp"))
        }
    }
}
