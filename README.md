# reflect-ios

The shared native **iOS engine** for the Reflect MMP SDK (`ReflectCore`) — sessions,
durable queue, HMAC-signed ingest, batching, client-side dedup, device signal
collection, deep links, attribution, SKAdNetwork/AdAttributionKit, and ATT.

It's **wrapper-agnostic** (no Flutter/RN types): the Reflect Flutter, React Native, and
Unity SDKs are thin bridges over `ReflectCore.handle(method:args:result:)` +
`ReflectListener`. All SDK logic lives here; each platform ships only a small wrapper.

## Privacy transport guarantees

- `setEnabled(false)` synchronously blocks/cancels analytics transport and erases
  the durable event queue while retaining identity for an explicit later re-enable.
  App-owned user/profile/global/partner state remains available for a same-process
  re-enable but deliberately does not rehydrate after a cold start. Restrictive
  consent/enable choices are also written to an independent atomic-file tombstone,
  so a failed `UserDefaults.synchronize()` cannot reopen collection on restart. A
  grant/re-enable clears that tombstone only after the ordinary privacy store
  commits; any relaxation failure stays blocked.
- Consent denial blocks/cancels transport, erases queued events, and removes local
  measurement identifiers. A later grant starts with a fresh identity.
- Native consent grants and re-enables reject before `initialize`, so a permissive
  pre-init call cannot remove a fail-closed tombstone without initialized app/tenant
  state. Wrapper pre-init denial/disable is carried into native initialization.
- `deleteUserData` performs the same local cleanup, persists suppression, and allows
  only the dedicated `/privacy/delete` request (including restart retries). Every
  unconfirmed install ID is retained independently across new measurement lifetimes.
  The current server contract requires a non-empty `signingSecret`; without one,
  no unauthenticated request is attempted and deletion remains durably pending.
  Pending state clears only after a 2xx JSON receipt containing both
  `{"ok":true,"queued":true}`. Each pending intent is bound to the normalized
  endpoint, app key, and company key under which it was created. Rotating config
  cannot send or acknowledge the old intent against a new tenant; legacy unscoped
  records stay pending rather than being guessed. Endpoint normalization matches
  Android (lowercase scheme/host, omitted default HTTP(S) port, trimmed trailing
  path slash), so equivalent config spellings match the same journal target.
  Deletion reloads a persisted install UUID when memory is empty and rejects rather
  than returning success when initialization, target, or identifier is missing. A
  future device-scoped delete
  credential should replace distribution of an app-wide HMAC secret in binaries.
- Raw Apple AdServices/deferred-referral input is never written to disk. The durable
  event queue is written immediately with only the server-reviewed coarse referral
  shape (provider source and click/install timestamps floored to the UTC day). The
  raw payload remains in process memory for at most 30 seconds as an override for an
  immediate online install send, then disappears on acknowledgement, timeout,
  privacy reset, or process death.
- Every disk-queue row requires a finite `event_ts_ms` and expires from that source
  timestamp at the server's strict, whole-second 90-day boundary. Queue load,
  enqueue, and each pre-send snapshot apply the same recursive sanitizer to
  `properties`/`props`, `device`, `partner_params`, `callback_params`, and
  `user_properties`: click-like keys are removed at any depth and URL-like values
  lose query/fragment context. Missing, malformed, expired, or unbounded-future
  source timestamps fail closed.
- Global properties and global partner parameters are deliberately process-only.
  Values use the same recursive sanitizer, carry a separate in-memory source clock
  per entry, and expire after 90 days; updating one entry cannot restart another
  entry's clock. No setter writes them to `UserDefaults`, and startup removes all
  historical plain-map or versioned records without rehydrating them.
- The pending combined-Worker release recursively strips click-token/query context
  from event bags, omits every free-form bag from long-lived R2 event copies, and,
  after the gated migration/Worker rollout, removes all such bags from the retained
  D1 event shell at the 90-day source boundary. This is a locally verified source
  candidate, not the current production server contract.
- This storage limit intentionally changes offline attribution: if the immediate
  send does not succeed within 30 seconds, or the app restarts first, deterministic
  matching that needs the raw AdServices/referral input may be lost. The durable
  install can then use only the coarse signal, fingerprint fallback, or organic
  attribution.
- An attribution response may deliver an unexpired `clickId` through that response's
  live callback only. `clickId` and its exact `click_context_expires_at_ms` are never
  persisted; the cache/getter/restart path contains only coarse
  type/partner/campaign. Upgrade cleanup removes legacy persisted ID and expiry keys
  immediately, including offline, and forces a signed full refresh.

The platform-independent gate and restart-cleanup race suite runs without an iOS
simulator:

```bash
swift run reflect-privacy-tests
```

## Use it (CocoaPods)

> **Release gate:** the privacy implementation above is verified in source but is
> not present in the published/tagged `1.0.0` core. Do not use `1.0.0` as the
> Phase-0 privacy release. Publish an immutable artifact from this source and pass
> clean CocoaPods consumer plus simulator/device gates before documenting a pod
> coordinate here.

## License

MIT
