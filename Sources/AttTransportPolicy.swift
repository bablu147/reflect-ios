import Foundation

/// Policy for transport failures caused by Apple's App Tracking Transparency
/// gate rather than by the network.
///
/// WHY THIS EXISTS
/// ---------------
/// An app that lists its ingest host in `NSPrivacyTrackingDomains` — which an
/// MMP host must, since the SDK declares `NSPrivacyTracking` true — has EVERY
/// connection to that host refused by iOS until the user resolves the ATT
/// prompt. The refusal arrives as a plain `NSURLErrorNotConnectedToInternet`,
/// indistinguishable at the URLSession layer from a device with no signal.
///
/// Treating it as network flakiness is wrong twice over:
///
///   1. Exponential backoff never helps. The gate does not open with time, it
///      opens when the user answers the prompt. Worse, that backoff is
///      persisted as a wall-clock deadline, so the next launch — which DOES
///      have the answer — still sits out a deadline earned before it.
///   2. No `NWPath` transition ever fires to recover it. The network was
///      reachable throughout and only this host was refused, so the SDK's one
///      recovery hook for "offline" is structurally blind to the condition.
///
/// Together those produce the reported symptom: the install/first-open batch is
/// queued, fails silently behind the gate, and ships only when the user
/// relaunches the app.
///
/// Deliberately pure — no UIKit, no AppTrackingTransparency, no URLSession — so
/// both rules are unit-testable on any macOS host. The device path they guard
/// cannot be: ATT always reports `.notDetermined` in the simulator and
/// tracking-domain enforcement needs a real, App-Store-signed install.
enum AttTransportPolicy {

    /// The wire value `ReflectCore` caches for an unanswered ATT prompt.
    static let undecided = "not_determined"

    enum Failure: Equatable {
        /// Ordinary transient failure — back off exponentially.
        case retry
        /// Refused by tracking-domain policy — wait for the ATT answer instead.
        case attBlocked
    }

    /// Classify a URLSession failure. Only the exact code iOS returns for a
    /// blocked tracking domain counts, and only while the prompt is genuinely
    /// unanswered — once it is answered, a refusal is a real network fault and
    /// must keep its exponential backoff.
    ///
    /// A device that is merely offline while the prompt is pending is
    /// indistinguishable here and will be classified as blocked. That is the
    /// safe direction: the caller then re-polls on a flat, modest cadence
    /// instead of backing off, and a genuine reconnect still arrives through
    /// NWPathMonitor.
    static func classify(
        errorDomain: String,
        errorCode: Int,
        attStatus: String?
    ) -> Failure {
        guard (attStatus ?? undecided) == undecided else { return .retry }
        guard errorDomain == NSURLErrorDomain,
              errorCode == NSURLErrorNotConnectedToInternet else { return .retry }
        return .attBlocked
    }

    /// True when the ATT prompt just went from unanswered to answered.
    ///
    /// Only an observed `not_determined` → decided transition counts. A first
    /// observation (`previous == nil`) must NOT: it happens on every launch of
    /// an app whose user answered long ago, and treating it as a resolution
    /// would clear the legitimate server-outage backoff that
    /// `restorePersistedBackoff()` exists to carry across restarts.
    ///
    /// A denial counts as much as a grant. It does not reopen a declared
    /// tracking domain, but it does end the wait: the queue must stop parking
    /// on a decision that has already been made and resume ordinary backoff.
    static func isPromptResolution(previous: String?, current: String?) -> Bool {
        guard previous == undecided else { return false }
        guard let current = current, current != undecided else { return false }
        return true
    }
}
