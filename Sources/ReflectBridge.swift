import Foundation

// ─────────────────────────────────────────────────────────────────────────────
//  Wrapper-agnostic boundary types for the shared Reflect native engine (iOS).
//
//  The engine in ReflectCore moves over verbatim; only the I/O boundary changes.
//  ReflectResult mirrors Flutter's FlutterResult closure shape so the engine's
//  existing result(value) calls are unchanged. Every host wrapper (Flutter
//  FlutterMethodChannel, React Native, Unity) maps onto the same surface:
//  ReflectCore.handle(method:args:result:) + a listener for the streams.
// ─────────────────────────────────────────────────────────────────────────────

/// Async result callback. Same closure shape as Flutter's FlutterResult, so the
/// engine's result(value) calls compile unchanged. The wrapper translates the
/// sentinels below back into its own framework's error/not-implemented values.
public typealias ReflectResult = (Any?) -> Void

/// Flutter-free error value the engine can pass to result(...); the wrapper maps
/// it onto its framework's error type. (The current engine never returns one —
/// it reports failures as ordinary result payloads — but kept for future use.)
public struct ReflectError {
    public let code: String
    public let message: String?
    public let details: Any?
    public init(code: String, message: String?, details: Any?) {
        self.code = code; self.message = message; self.details = details
    }
}

/// Sentinel passed to result(...) for an unhandled method; the wrapper maps it
/// onto e.g. FlutterMethodNotImplemented.
public enum ReflectNotImplemented { case instance }

/// Host-side listener for the deep-link + attribution streams (replaces the
/// Flutter EventChannel sinks that used to live inside the engine).
public protocol ReflectListener: AnyObject {
    func onDeepLink(_ data: Any)
    func onAttribution(_ data: Any)
}
