# reflect-ios

The shared native **iOS engine** for the Reflect MMP SDK (`ReflectCore`) — sessions,
durable queue, HMAC-signed ingest, batching, client-side dedup, device signal
collection, deep links, attribution, SKAdNetwork/AdAttributionKit, and ATT.

It's **wrapper-agnostic** (no Flutter/RN types): the Reflect Flutter, React Native, and
Unity SDKs are thin bridges over `ReflectCore.handle(method:args:result:)` +
`ReflectListener`. All SDK logic lives here; each platform ships only a small wrapper.

## Use it (CocoaPods)

```ruby
# Once published to trunk:
pod 'ReflectCore', '~> 1.0'

# Or straight from git:
pod 'ReflectCore', :git => 'https://github.com/bablu147/reflect-ios.git', :tag => '1.0.0'
```

## License

MIT
