# In-memory .NET administration controls

This standalone executable follows the existing .NET conformance-project
pattern, imports the repository's exact `SdkPin.props` and `packages.lock.json`,
and links `../../shared/SdkCustody.cs` without modifying it. It needs no server,
signing key, service account, or provisioned resource. `InMemoryTransport` creates
SDK request objects but never calls a network transport's `Process` method.
The credential is an unusable constant created only for the synthetic pipeline.
No Authorization header is recorded.

For each selector it checks:

- Queue, topic and subscription definition/runtime collection calls have
  identical emitted method, path and raw query, matching the fixture manifest.
- The default API version and initial `$skip=0&$top=100` selectors are present.
- Nonzero synthetic runtime values and timestamps survive SDK parsing, including
  the current-only topic filter-count properties.
- HTTP 400 refuses both collection methods with `ArgumentException`, not an
  empty enumeration or an empty successful page.
- Singleton definition/runtime requests differ by `enrich=False` / `True`.

The response bytes remain synthetic. Successful execution observes the selected
SDK's in-process request construction and response parsing, **not** socket wire
bytes, an Azure service, Switchyard endpoint behavior, strict XML rejection, or
Sift. Pagination beyond the first short page and remaining `controls.json`
adapter vectors are not covered by this executable.

## Custody and qualification

The executable requires a selected pin, absolute fixture/approval paths, a
prelaunch SHA-256 of the entry DLL, a prelaunch SHA-256 of the approved-assets
file, and the existing 64-hex-character `SWITCHYARD_SDK_NONCE`. It checks loaded
ServiceBus/Core DLL bytes against that selected approval and requires them to
come from the entry's output directory. Every fixture read is bounded, hashed,
and rechecked. The shared custody recorder emits start/completion identities
and rechecks the three loaded assemblies after the workflow returns.

`SWITCHYARD_ATOM_OBSERVATION` records contain only synthetic request shapes and
the invocation nonce. `SWITCHYARD_ATOM_COMPLETE` binds the selector and fixture
hashes. **Neither marker alone establishes success**: the parent must require a
zero exit and matching shared `SWITCHYARD_SDK_CUSTODY` start/complete records,
and independently compare the supplied prelaunch hashes with post-run files.
Freeze source inputs during build/run. A prehashed stale binary is still stale;
the caller must retain build/source provenance. This standalone launcher is not
the existing Rust `SdkRun` process-family custody gate.

## Parent-owned commands

Only the primary build owner should run this, serially for `declared-current`
and `previous`, from the repository root with a distinct external output/cache
root for each. Do not
restore or build into the source directory. Missing .NET 10/NuGet prerequisites,
restore drift, hash mismatch or a failed assertion are failures, never skips.
The commands below are a launch recipe, not evidence that they ran. The normal
long-running command owner must retain its exit marker and logs.

```sh
set -euo pipefail
repo=$(pwd)
selector=declared-current # Repeat separately with previous.
run=$(mktemp -d "$HOME/.cache/switchyard-atom-${selector}.XXXXXX")
mkdir -p "$run/obj" "$run/bin" "$run/packages" "$run/http-cache" \
  "$run/scratch" "$run/plugins-cache" "$run/cli-home"
export DOTNET_CLI_HOME="$run/cli-home"
export DOTNET_ADD_GLOBAL_TOOLS_TO_PATH=false
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_SKIP_FIRST_TIME_EXPERIENCE=1
export DOTNET_GENERATE_ASPNET_CERTIFICATE=false
export DOTNET_MULTILEVEL_LOOKUP=0 DOTNET_PROCESSOR_COUNT=2
export DOTNET_CLI_USE_MSBUILD_SERVER=0 MSBUILDDISABLENODEREUSE=1
export NUGET_PACKAGES="$run/packages" NUGET_HTTP_CACHE_PATH="$run/http-cache"
export NUGET_SCRATCH="$run/scratch" NUGET_PLUGINS_CACHE_PATH="$run/plugins-cache"
export NUGET_FALLBACK_PACKAGES=
project="$repo/crates/conformance/atom-fixtures/dotnet/Switchyard.Conformance.Atom.csproj"
approval="$repo/crates/conformance/pins/$selector/approved-assets.json"
lock="$repo/crates/conformance/pins/$selector/packages.lock.json"
props="$repo/crates/conformance/pins/$selector/SdkPin.props"
sha256sum "$lock" "$props" "$approval" > "$run/pins.before"
dotnet restore "$project" --locked-mode --force --disable-parallel \
  --configfile "$repo/crates/conformance/pins/NuGet.Config" \
  --maxcpucount:2 /nodeReuse:false \
  -p:AtomSdkSelector="$selector" \
  -p:BaseIntermediateOutputPath="$run/obj/" -p:MSBuildProjectExtensionsPath="$run/obj/"
dotnet build "$project" --configuration Release --no-restore --disable-build-servers \
  --maxcpucount:2 /nodeReuse:false \
  -p:AtomSdkSelector="$selector" \
  -p:BaseIntermediateOutputPath="$run/obj/" -p:MSBuildProjectExtensionsPath="$run/obj/" \
  -p:OutputPath="$run/bin/" -p:AppendTargetFrameworkToOutputPath=false
entry="$run/bin/Switchyard.Conformance.Atom.dll"
entry_sha=$(sha256sum "$entry" | cut -d ' ' -f 1)
approval_sha=$(sha256sum "$approval" | cut -d ' ' -f 1)
export SWITCHYARD_SDK_NONCE=$(od -An -N32 -tx1 /dev/urandom | tr -d ' \n')
sha256sum "$entry" "$approval" > "$run/runtime.before"
timeout --signal=TERM --kill-after=5s 60s \
  dotnet "$entry" "$selector" "$repo/crates/conformance/atom-fixtures" \
  "$approval" "$entry_sha" "$approval_sha" | tee "$run/observations.log"
sha256sum "$entry" "$approval" > "$run/runtime.after"
sha256sum "$lock" "$props" "$approval" > "$run/pins.after"
cmp "$run/runtime.before" "$run/runtime.after"
cmp "$run/pins.before" "$run/pins.after"
```

Parent qualification should also run deliberate wrong-entry-hash,
wrong-approval-hash and wrong-selector controls and require nonzero exit without
an Atom completion or shared custody completion record. Keep those invocations'
nonces/logs separate. Do not promote source fixtures to observations or mark
the source-only Sift pin executed based on a .NET result.
