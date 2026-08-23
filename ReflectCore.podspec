Pod::Spec.new do |s|
  s.name             = 'ReflectCore'
  s.version          = '1.1.4'
  s.summary          = 'Reflect MMP shared native iOS engine (wrapper-agnostic).'
  s.description      = <<-DESC
The shared Reflect SDK engine for iOS — sessions, durable queue, HMAC signing,
batching, client-side dedup, device collection, deep links, and attribution.
Wrapper-agnostic (no Flutter types): consumed by the Flutter, Unity, and React
Native wrappers via the ReflectCore.handle(method:args:result:) + ReflectListener
surface. All SDK logic lives here; each platform ships only a thin bridge.
                       DESC
  s.homepage         = 'https://github.com/bablu147/reflect-ios'
  s.license          = { :type => 'MIT' }
  s.author           = { 'Reflect' => 'hello@reflect.cloud' }
  # Published from the public repo. Tag the repo to match s.version EXACTLY
  # (currently `1.1.1`). Never re-cut an existing tag — `1.1.0` is already public.
  # Consumers get it via `pod trunk push` (CocoaPods trunk) or, without trunk, a
  # Podfile line: pod 'ReflectCore', :git => 'https://github.com/bablu147/reflect-ios.git', :tag => '1.1.0'
  s.source           = { :git => 'https://github.com/bablu147/reflect-ios.git', :tag => s.version.to_s }
  s.source_files     = 'Sources/**/*.swift'

  s.platform         = :ios, '13.0'
  s.swift_version    = '5.0'

  s.frameworks       = 'UIKit', 'AdSupport', 'StoreKit', 'CoreTelephony'
  # Weak-linked: present only on newer OSes (ATT iOS 14+, AdServices 14.3+).
  s.weak_frameworks  = 'AppTrackingTransparency', 'AdServices'

  s.pod_target_xcconfig = { 'DEFINES_MODULE' => 'YES' }
end
