import Foundation

/// Enforces the server's strict click-context retention boundary on the local
/// attribution cache. Coarse attribution outcome fields remain available; only
/// the unique click identifier is removed.
enum AttributionRetention {
    private static let daySeconds: Int64 = 86_400
    static let clickContextRetentionMs: Int64 = 90 * daySeconds * 1_000
    private static let maxEventFutureSkewMs: Int64 = 60 * 60 * 1_000
    private static let maxSanitizeDepth = 20
    private static let clickContextKeys: Set<String> = [
        "gclid", "dclid", "gbraid", "wbraid", "fbclid",
        "msclkid", "ttclid", "twclid", "li_fat_id", "irclickid",
        "epik", "sacid", "click_id", "clickid", "ext_click_id",
        "external_click_id", "attribution_token", "install_referrer", "referrer",
        "sub1", "sub2", "sub3", "sub4", "sub5",
    ]
    private static let referralSources: Set<String> = [
        "adservices",
        "play_install_referrer",
        "meta_install_referrer",
        "samsung_install_referrer",
        "xiaomi_install_referrer",
        "vivo_install_referrer",
        "huawei_install_referrer",
        "preinstall",
    ]
    private static let referralTimestampKeys: Set<String> = [
        "click_ts", "click_server_ts", "install_ts", "install_server_ts",
    ]
    private static let durableDeepLinkSources: Set<String> = [
        "direct", "deferred",
    ]

    static func hasClickId(_ attributionJSON: String?) -> Bool {
        guard let attributionJSON,
              let data = attributionJSON.data(using: .utf8),
              let value = try? JSONSerialization.jsonObject(with: data),
              let object = value as? [String: Any],
              let clickId = object["clickId"] as? String else { return false }
        return !clickId.isEmpty
    }

    static func isExpired(
        expiresAtMs: Int64,
        nowMs: Int64 = Int64(Date().timeIntervalSince1970 * 1000)
    ) -> Bool {
        guard expiresAtMs > 0 else { return false }
        let flooredNowMs = (nowMs / 1000) * 1000
        return expiresAtMs < flooredNowMs
    }

    /// Match the server's strict second-normalized source boundary. A timestamp
    /// exactly 90 days old remains live until the next whole-second tick.
    static func isSourceTimestampRetained(
        sourceAtMs: Int64,
        nowMs: Int64 = Int64(Date().timeIntervalSince1970 * 1000)
    ) -> Bool {
        guard sourceAtMs > 0,
              sourceAtMs <= Int64.max - clickContextRetentionMs else { return false }
        let flooredNowMs = (nowMs / 1_000) * 1_000
        guard flooredNowMs <= Int64.max - maxEventFutureSkewMs,
              sourceAtMs <= flooredNowMs + maxEventFutureSkewMs else { return false }
        return !isExpired(
            expiresAtMs: sourceAtMs + clickContextRetentionMs,
            nowMs: nowMs
        )
    }

    static func urlWithoutQueryOrFragment(_ url: String) -> String {
        let query = url.firstIndex(of: "?") ?? url.endIndex
        let fragment = url.firstIndex(of: "#") ?? url.endIndex
        return String(url[..<min(query, fragment)])
    }

    static func hasUniqueClickContext(_ params: [String: String]) -> Bool {
        params.keys.contains { clickContextKeys.contains($0.lowercased()) }
    }

    static func scrubClickId(
        _ attributionJSON: String?,
        expiresAtMs: Int64,
        nowMs: Int64 = Int64(Date().timeIntervalSince1970 * 1000),
        failClosedWhenExpiryUnknown: Bool = false
    ) -> String? {
        guard let attributionJSON else { return nil }
        let mustScrub =
            (expiresAtMs <= 0 && failClosedWhenExpiryUnknown && hasClickId(attributionJSON)) ||
            isExpired(expiresAtMs: expiresAtMs, nowMs: nowMs)
        guard mustScrub else { return attributionJSON }
        guard let data = attributionJSON.data(using: .utf8),
              let value = try? JSONSerialization.jsonObject(with: data),
              var object = value as? [String: Any] else {
            // A malformed cache must never bypass an expired retention fence.
            return nil
        }
        object.removeValue(forKey: "clickId")
        guard let scrubbed = try? JSONSerialization.data(withJSONObject: object, options: [.sortedKeys]) else {
            return nil
        }
        return String(data: scrubbed, encoding: .utf8)
    }

    /// Scrub legacy durable events on load, enqueue, and immediately before
    /// transmission. Every free-form bag uses the same recursive policy as the
    /// server and the whole row expires from its event-source timestamp.
    static func scrubQueuedEvent(
        _ eventJSON: String,
        nowMs: Int64 = Int64(Date().timeIntervalSince1970 * 1000)
    ) -> String? {
        guard let data = eventJSON.data(using: .utf8),
              let value = try? JSONSerialization.jsonObject(with: data),
              var event = value as? [String: Any] else {
            // Never preserve or transmit a malformed row whose click-context
            // retention status cannot be established.
            return nil
        }
        guard let eventTimestampMs = finiteInt64(event["event_ts_ms"]),
              isSourceTimestampRetained(sourceAtMs: eventTimestampMs, nowMs: nowMs) else {
            return nil
        }

        if event["event_name"] as? String == "deep_link_opened" {
            if let properties = event["properties"] as? [String: Any] {
                event["properties"] = scrubDeepLinkProperties(properties)
            }
            if let props = event["props"] as? [String: Any] {
                event["props"] = scrubDeepLinkProperties(props)
            }
        } else {
            scrubGenericBag(&event, key: "properties")
            scrubGenericBag(&event, key: "props")
        }
        scrubGenericBag(&event, key: "device")
        scrubGenericBag(&event, key: "partner_params")
        scrubGenericBag(&event, key: "callback_params")
        scrubGenericBag(&event, key: "user_properties")

        if let referral = event["referral"] as? [String: Any] {
            let retained = sanitizeReferralForPersistence(referral)
            if retained.isEmpty { event.removeValue(forKey: "referral") }
            else { event["referral"] = retained }
        }

        guard let scrubbed = try? JSONSerialization.data(withJSONObject: event, options: [.sortedKeys]) else {
            return nil
        }
        return String(data: scrubbed, encoding: .utf8)
    }

    /// Sanitize one global/partner entry before it can live in process memory.
    /// Returning nil means the key is click-specific and must be removed.
    static func sanitizeEphemeralParameter(key: String, value: Any) -> Any? {
        guard !key.isEmpty, !isClickContextPropertyKey(key) else { return nil }
        return sanitizeGenericValue(value, key: key, depth: 0)
    }

    /// Remove historical durable copies. Global and partner parameters are
    /// intentionally process-only so no suspended app can retain arbitrary
    /// click context on disk beyond the source-age boundary.
    static func clearLegacyParameterPersistence(
        defaults: UserDefaults = .standard
    ) {
        defaults.removeObject(forKey: "reflect_global_props")
        defaults.removeObject(forKey: "reflect_partner_params")
        _ = defaults.synchronize()
    }

    private static func scrubGenericBag(
        _ event: inout [String: Any],
        key: String
    ) {
        guard let original = event[key] as? [String: Any] else {
            event.removeValue(forKey: key)
            return
        }
        let sanitized = sanitizeGenericObject(original, depth: 0)
        if sanitized.isEmpty {
            event.removeValue(forKey: key)
        } else {
            event[key] = sanitized
        }
    }

    private static func sanitizeGenericObject(
        _ source: [String: Any],
        depth: Int
    ) -> [String: Any] {
        guard depth <= maxSanitizeDepth else { return [:] }
        var output: [String: Any] = [:]
        for key in source.keys.sorted() {
            guard !isClickContextPropertyKey(key),
                  let value = source[key],
                  let clean = sanitizeGenericValue(value, key: key, depth: depth + 1) else {
                continue
            }
            output[key] = clean
        }
        return output
    }

    private static func sanitizeGenericValue(
        _ value: Any,
        key: String,
        depth: Int
    ) -> Any? {
        guard depth <= maxSanitizeDepth else { return nil }
        if value is NSNull { return NSNull() }
        if let string = value as? String {
            return isUrlLikePropertyKey(key)
                ? urlWithoutQueryOrFragment(string)
                : string
        }
        if let object = value as? [String: Any] {
            return sanitizeGenericObject(object, depth: depth)
        }
        if let array = value as? [Any] {
            return array.compactMap {
                sanitizeGenericValue($0, key: key, depth: depth + 1)
            }
        }
        if let number = value as? NSNumber {
            return number.doubleValue.isFinite ? value : nil
        }
        if value is Bool { return value }
        return nil
    }

    private static func isClickContextPropertyKey(_ key: String) -> Bool {
        let lower = key.lowercased()
        if clickContextKeys.contains(lower) { return true }
        let compact = lower.filter { $0.isLetter || $0.isNumber }
        return compact.hasSuffix("clickid")
            || compact.hasSuffix("clid")
            || compact.hasSuffix("braid")
            || compact == "attributiontoken"
            || ["sub1", "sub2", "sub3", "sub4", "sub5"].contains(compact)
    }

    private static func isUrlLikePropertyKey(_ key: String) -> Bool {
        let compact = key.lowercased().filter { $0.isLetter || $0.isNumber }
        return compact.contains("url")
            || compact.contains("uri")
            || compact.contains("deeplink")
            || compact.contains("redirectlink")
            || compact == "link"
            || compact == "links"
    }

    private static func scrubDeepLinkProperties(_ properties: [String: Any]) -> [String: Any] {
        var result: [String: Any] = [:]
        if let url = properties["url"] as? String {
            result["url"] = urlWithoutQueryOrFragment(url)
        }
        if let path = properties["path"] as? String {
            result["path"] = urlWithoutQueryOrFragment(path)
        }
        if let source = properties["source"] as? String,
           durableDeepLinkSources.contains(source.lowercased()) {
            result["source"] = source.lowercased()
        }
        if let reattribution = properties["is_reattribution"] as? Bool {
            result["is_reattribution"] = reattribution
        }
        return result
    }

    /// Raw referrers and AdServices tokens are attribution inputs, not durable
    /// analytics fields. The queue stores only the server's coarse allowlist.
    private static func sanitizeReferralForPersistence(
        _ referral: [String: Any]
    ) -> [String: Any] {
        var retained: [String: Any] = [:]
        for (key, value) in referral {
            let lower = key.lowercased()
            if lower == "source",
               let source = value as? String,
               referralSources.contains(source.lowercased()) {
                retained["source"] = source.lowercased()
            } else if lower == "google_play_instant", let instant = value as? Bool {
                retained["google_play_instant"] = instant
            } else if referralTimestampKeys.contains(lower),
                      let coarse = coarseEpochDaySeconds(value) {
                retained[lower] = coarse
            }
        }
        return retained
    }

    static func hasTransientAttributionContext(_ eventJSON: String) -> Bool {
        guard let data = eventJSON.data(using: .utf8),
              let event = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any],
              event["event_name"] as? String == "app_install",
              let referral = event["referral"] as? [String: Any] else { return false }
        let raw = referral["raw"] as? String
        let token = referral["attribution_token"] as? String
        return !(raw?.isEmpty ?? true) || !(token?.isEmpty ?? true)
    }

    private static func int64(_ value: Any?) -> Int64 {
        if let value = value as? Int64 { return value }
        if let value = value as? Int { return Int64(value) }
        if let value = value as? Double { return Int64(value) }
        if let value = value as? NSNumber { return value.int64Value }
        if let value = value as? String { return Int64(value) ?? 0 }
        return 0
    }

    private static func finiteInt64(_ value: Any?) -> Int64? {
        if let value = value as? Int64 { return value }
        if let value = value as? Int { return Int64(value) }
        if let value = value as? NSNumber {
            guard value.doubleValue.isFinite,
                  value.doubleValue >= Double(Int64.min),
                  value.doubleValue <= Double(Int64.max) else { return nil }
            return value.int64Value
        }
        if let value = value as? Double, value.isFinite,
           value >= Double(Int64.min), value <= Double(Int64.max) {
            return Int64(value)
        }
        return nil
    }

    private static func coarseEpochDaySeconds(_ value: Any) -> Int64? {
        let numeric = int64(value)
        guard numeric > 0 else { return nil }
        let seconds = numeric >= 100_000_000_000 ? numeric / 1000 : numeric
        return (seconds / daySeconds) * daySeconds
    }
}
