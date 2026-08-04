import Foundation

/// Automatic SKAdNetwork conversion-value computation.
///
/// WHY THIS EXISTS
/// ---------------
/// `updatePostbackConversionValue` was reachable only as a passthrough: the host
/// app had to decide the number itself and call `Reflect.UpdateConversionValue`.
/// Nothing in the SDK ever computed one, so in practice every app shipped
/// conversion value 0 and every SKAdNetwork postback carried no post-install
/// signal at all — Apple reported installs and nothing else.
///
/// This type turns accumulated post-install revenue into the (fineValue,
/// coarseValue) pair Apple expects, using the operator-authored schema served by
/// `GET /skan/cv-schema`. It is deliberately PURE — no I/O, no SKAdNetwork, no
/// UserDefaults — so the mapping can be unit-tested off-device. SKAdNetwork is
/// unavailable on the simulator (`SKANErrorDomain error 10`), which makes a pure
/// core the only part of this that can be verified without hardware.
///
/// CONTRACT WITH THE SERVER
/// ------------------------
/// The schema is a JSON array of EXACTLY 64 entries — index == fine value, which
/// is 6 bits, so Apple can send 0...63 and every one must decode. The server
/// validates and canonicalises this on write (`admin-api/src/lib/skan-cv-core.ts`)
/// and the same 64-entry rule is enforced in
/// `workers/src/routes/admin/skan-cv-schema.ts`. Entry shape:
///
///     { "revenue_min": Double, "revenue_max": Double, "name": String? }
///
/// Keep those three in sync: a mismatch here silently mis-buckets revenue rather
/// than failing, because Apple accepts any value in range.
public enum SkanConversionValue {

    /// Apple's fine conversion value is 6 bits.
    public static let maxFineValue = 63
    /// Number of buckets a schema must define — one per representable value.
    public static let requiredEntryCount = 64

    public struct Bucket: Equatable {
        public let revenueMin: Double
        public let revenueMax: Double
        public let name: String?
    }

    /// Apple's coarse value, used when the install volume is below Apple's
    /// crowd-anonymity threshold and the fine value is withheld.
    public enum Coarse: String {
        case low, medium, high
    }

    public struct Decision: Equatable {
        public let fineValue: Int
        public let coarse: Coarse
        /// True when no bucket matched and the value was clamped rather than
        /// invented. Callers log this; a schema that leaves gaps is an operator
        /// error we surface rather than silently paper over.
        public let clamped: Bool
    }

    // MARK: - Schema parsing

    /// Parse the server's schema JSON. Returns nil (never a partial schema) when
    /// the payload does not match the contract, so a malformed fetch leaves any
    /// previously cached schema in place instead of replacing it with garbage.
    public static func parseSchema(_ json: String) -> [Bucket]? {
        guard let data = json.data(using: .utf8),
              let raw = try? JSONSerialization.jsonObject(with: data) as? [[String: Any]],
              raw.count == requiredEntryCount
        else { return nil }

        var out: [Bucket] = []
        out.reserveCapacity(raw.count)
        for e in raw {
            // Accept Int or Double from JSON; reject anything else rather than
            // coercing, because a coerced 0 silently makes a paid bucket free.
            guard let minV = numeric(e["revenue_min"]),
                  let maxV = numeric(e["revenue_max"]),
                  minV.isFinite, maxV.isFinite,
                  minV >= 0, maxV >= minV
            else { return nil }
            out.append(Bucket(revenueMin: minV, revenueMax: maxV, name: e["name"] as? String))
        }
        return out
    }

    private static func numeric(_ v: Any?) -> Double? {
        if let d = v as? Double { return d }
        if let i = v as? Int { return Double(i) }
        if let n = v as? NSNumber { return n.doubleValue }
        return nil
    }

    // MARK: - Decision

    /// Map accumulated post-install revenue to a conversion value.
    ///
    /// Buckets are matched on `revenueMin <= revenue <= revenueMax`, scanning
    /// HIGHEST index first so that overlapping ranges resolve to the most
    /// valuable bucket — an operator who writes overlapping tiers almost
    /// certainly means "at least this much", and under-reporting revenue is the
    /// worse error for bidding.
    ///
    /// Revenue above every bucket clamps to the highest-index bucket rather than
    /// falling back to 0; a whale must not look like a non-payer.
    public static func decide(revenue: Double, schema: [Bucket]) -> Decision {
        guard !schema.isEmpty else {
            return Decision(fineValue: 0, coarse: .low, clamped: true)
        }
        let value = revenue.isFinite && revenue > 0 ? revenue : 0

        for idx in stride(from: schema.count - 1, through: 0, by: -1) {
            let b = schema[idx]
            if value >= b.revenueMin && value <= b.revenueMax {
                return Decision(fineValue: min(idx, maxFineValue),
                                coarse: coarse(forFine: idx, of: schema.count),
                                clamped: false)
            }
        }

        // No bucket contains it. Above the top bucket -> clamp high; otherwise
        // (a gap in the schema) -> 0, flagged so the caller can log it.
        if let top = schema.last, value > top.revenueMax {
            let idx = min(schema.count - 1, maxFineValue)
            return Decision(fineValue: idx, coarse: coarse(forFine: idx, of: schema.count), clamped: true)
        }
        return Decision(fineValue: 0, coarse: .low, clamped: true)
    }

    /// Derive Apple's coarse value by terciles of the schema. Apple sends the
    /// coarse value INSTEAD of the fine one for low-volume campaigns, so it must
    /// carry a usable signal on its own rather than always being `.low`
    /// (which is what the SDK previously sent, unconditionally, in `armSkan`).
    public static func coarse(forFine fine: Int, of count: Int) -> Coarse {
        guard count > 1 else { return .low }
        let ratio = Double(fine) / Double(count - 1)
        if ratio >= 2.0 / 3.0 { return .high }
        if ratio >= 1.0 / 3.0 { return .medium }
        return .low
    }
}
