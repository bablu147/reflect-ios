import Foundation

/// Persistent state for automatic SKAdNetwork conversion values.
///
/// Splits the stateful half of auto-CV away from the pure mapping in
/// `SkanConversionValue`, so the decision logic stays unit-testable off-device
/// (SKAdNetwork itself is unavailable on the simulator — `SKANErrorDomain
/// error 10` — so anything touching it cannot be verified without hardware).
///
/// Responsibilities:
///   - accumulate post-install revenue across launches
///   - cache the operator's schema fetched from `GET /skan/cv-schema`
///   - decide whether a new value is worth sending
///
/// Apple only accepts an INCREASING fine value within a measurement window, and
/// each call can restart the window timer. So this deliberately suppresses
/// updates that would not raise the value — sending the same or a lower number
/// wastes a window and can hold a postback open longer than intended.
public final class SkanAutoConversion {

    // Keys are namespaced with the SDK's existing `reflect_` prefix so
    // PrivacyPersistence's key sweep clears them with everything else on
    // deleteUserData(); a stale revenue total surviving an erasure would leak
    // spend history of the deleted identity into the next install's postbacks.
    static let revenueKey = "reflect_skan_revenue_total"
    static let lastFineKey = "reflect_skan_last_fine"
    static let schemaKey = "reflect_skan_cv_schema_json"
    static let schemaFetchedKey = "reflect_skan_cv_schema_at"

    /// Re-fetch the schema at most once a day. It changes rarely and the
    /// endpoint is KV-cached server-side; a per-launch fetch would be pure cost.
    static let schemaTtlMs: Double = 24 * 60 * 60 * 1000

    private let defaults: UserDefaults

    public init(defaults: UserDefaults = .standard) {
        self.defaults = defaults
    }

    // MARK: - Revenue

    /// Add revenue and return the new running total. Non-finite or negative
    /// amounts are ignored rather than corrupting the accumulator.
    @discardableResult
    public func addRevenue(_ amount: Double) -> Double {
        guard amount.isFinite, amount > 0 else { return totalRevenue }
        let next = totalRevenue + amount
        defaults.set(next, forKey: Self.revenueKey)
        return next
    }

    public var totalRevenue: Double {
        let v = defaults.double(forKey: Self.revenueKey)
        return v.isFinite && v > 0 ? v : 0
    }

    // MARK: - Schema cache

    public func cachedSchema() -> [SkanConversionValue.Bucket]? {
        guard let json = defaults.string(forKey: Self.schemaKey) else { return nil }
        return SkanConversionValue.parseSchema(json)
    }

    /// Store a freshly fetched schema. Rejects payloads that do not parse, so a
    /// bad server response can never evict a good cached schema.
    @discardableResult
    public func storeSchema(_ json: String, nowMs: Double) -> Bool {
        guard SkanConversionValue.parseSchema(json) != nil else { return false }
        defaults.set(json, forKey: Self.schemaKey)
        defaults.set(nowMs, forKey: Self.schemaFetchedKey)
        return true
    }

    public func schemaIsStale(nowMs: Double) -> Bool {
        let at = defaults.double(forKey: Self.schemaFetchedKey)
        if at <= 0 { return true }
        return (nowMs - at) >= Self.schemaTtlMs
    }

    // MARK: - Decide whether to send

    public struct Update: Equatable {
        public let fineValue: Int
        public let coarse: SkanConversionValue.Coarse
    }

    /// Compute the next update, or nil when nothing should be sent.
    ///
    /// Returns nil when there is no schema (never guess a value the operator did
    /// not define) or when the new fine value would not exceed the last one
    /// Apple accepted — SKAdNetwork ignores non-increasing values within a
    /// window and each call can restart the window timer.
    public func nextUpdate(nowRevenue: Double) -> Update? {
        guard let schema = cachedSchema() else { return nil }
        let decision = SkanConversionValue.decide(revenue: nowRevenue, schema: schema)

        let last = defaults.object(forKey: Self.lastFineKey) as? Int
        if let last = last, decision.fineValue <= last { return nil }

        return Update(fineValue: decision.fineValue, coarse: decision.coarse)
    }

    /// Record a value Apple accepted, so it is not resent.
    public func recordSent(fineValue: Int) {
        defaults.set(fineValue, forKey: Self.lastFineKey)
    }

    /// Clear every key this type owns. Called from the SDK's privacy reset so an
    /// erased identity's spend history cannot influence a later install.
    public func reset() {
        for k in [Self.revenueKey, Self.lastFineKey, Self.schemaKey, Self.schemaFetchedKey] {
            defaults.removeObject(forKey: k)
        }
    }
}
