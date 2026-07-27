// swift-tools-version: 5.9
import PackageDescription

// A platform-independent executable test target. The production core remains a
// CocoaPod because it imports UIKit, while this gate/cleanup suite intentionally
// runs on any macOS CI host without a simulator or Xcode XCTest installation.
let package = Package(
    name: "ReflectIOSPrivacyTests",
    platforms: [.macOS(.v13)],
    products: [
        .executable(name: "reflect-privacy-tests", targets: ["ReflectPrivacyTests"]),
    ],
    targets: [
        .executableTarget(
            name: "ReflectPrivacyTests",
            path: ".",
            exclude: [
                ".gitignore",
                "README.md",
                "ReflectCore.podspec",
                "Sources/ReflectBridge.swift",
                "Sources/ReflectCore.swift",
            ],
            sources: [
                "Sources/AttributionRetention.swift",
                "Sources/PrivacyTransportState.swift",
                "Tests/ReflectPrivacySupportTests/main.swift",
            ]
        ),
    ]
)
