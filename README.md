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

## ATT tracking-domain transport

**The ingest host is NOT declared in `NSPrivacyTrackingDomains` by default.**
Declaring it means iOS refuses **every** connection to that host until the user
answers the ATT prompt — ATT-undecided *and* ATT-denying users then deliver
nothing at all (measured live 2026-08 on a production fleet: only ~32% of
active iOS devices — roughly the ATT accept rate — ever delivered an SDK
event). Identifier collection needs no such fence: iOS itself returns an
all-zero IDFA until ATT is authorized. The Unity build post-processor therefore
injects the entry only when the integrator opts in
(`ReflectBuildPostProcessor.DeclareTrackingDomain` /
`REFLECT_DECLARE_TRACKING_DOMAIN=1`); hosts on the other wrappers make the same
choice in their own app manifest. Everything below describes the transport
behavior **when a host opts into the declaration** (or declares the domain
itself):

The refusal is delivered as `NSURLErrorNotConnectedToInternet` — the same code a
device with no signal returns — and no `NWPath` transition ever accompanies it,
because the network was reachable the whole time and only that one host was
refused. Two consequences the core must handle, both encoded in
`AttTransportPolicy` (pure, covered by `swift run reflect-privacy-tests`):

- **A refusal while ATT is unanswered is not flakiness.** It is classified
  `attBlocked` and backs off against a 5-minute ceiling instead of the
  one-hour server-outage ceiling — it still backs off, so a host that never
  presents the prompt is not polled forever. Nothing is persisted, so a
  relaunch that already carries the answer sends immediately rather than
  serving out a deadline earned behind the gate. Once ATT is answered, the same
  error code returns to ordinary backoff.
- **The ATT answer reopens transport.** `refreshAttStatus()` detects the
  `not_determined` → decided transition and clears the send gate. Without it a
  first-install batch stays queued until the user relaunches the app, which was
  the observed symptom. Only a real observed transition counts — a first
  observation of an already-answered prompt must not clear a legitimate
  server-outage backoff that `restorePersistedBackoff()` carries across
  restarts.

A **denial** does not reopen the domain: declaring a host in
`NSPrivacyTrackingDomains` means ATT-denying users cannot reach it at all, so
their events are queued and eventually expire rather than delivered. That is a
property of the declaration, not of this SDK — and it is exactly why the
default posture is now *undeclared* (the Adjust pattern: only a dedicated
consent host is declared; the analytics host is not). Operators who opt into
the declaration accept losing ATT-undecided/denying users, or split tracking
traffic onto a declared host and keep first-party analytics on an undeclared
one.

The platform-independent gate and restart-cleanup race suite runs without an iOS
simulator:

```bash
swift run reflect-privacy-tests
```

## Use it (CocoaPods)

> **Use `1.1.4` or newer.**
>
> - **`1.0.0`** predates the privacy implementation above (fail-closed posture,
>   transport gate, 90-day click-context retention). Do not use it.
> - **`1.1.0`** carries the privacy work but still hard-codes
>   `X-Reflect-Platform: "flutter"`, so every event it sends — from Unity, React
>   Native, Flutter or the native iOS SDK alike — is branded Flutter on the wire.
> - **`1.1.1`** is the first tag with both. It is a behaviour fix with no API
>   change, so the `~> 1.1` pins already used by the Flutter and React Native
>   podspecs pick it up with no edit on their side.
> - **`1.1.2`** adds the batch-size/byte clamps.
> - **`1.1.3`** (2026-08-20) fixes `isJailbroken()` false-positiving on **all**
>   real iPhones — production data showed every iOS attribution stamped
>   `fraud_flag=device_rooted`. Behaviour fix, no API change; `~> 1.1` pins pick
>   it up on the next `pod update`.
> - **`1.1.4`** (2026-08-24) fixes first-session delivery: the install latches
>   only at durable persistence (a dropped first launch re-fires, server-deduped
>   per install_uuid), legacy-uuid adoption no longer silences unreported
>   installs, swallowed drain wakeups replay, and a bounded background-task
>   window flushes the queue when the app is backgrounded. Behaviour fix, no
>   API change; `~> 1.1` picks it up on the next `pod update`.

**Publishing checklist** (CocoaPods resolves `s.source` from a GitHub tag):
1. Mirror this directory to `github.com/bablu147/reflect-ios` (it is a
   subdirectory of the private monorepo; the public repo is the pod source).
2. Tag the release and push the tag — it must match `s.version` exactly
   (latest: `1.1.4`, published 2026-08-24).
3. `pod spec lint ReflectCore.podspec` against the pushed tag.
4. `1.1.0` is already public and may be in use — **never re-cut an existing tag.**

## License

MIT
