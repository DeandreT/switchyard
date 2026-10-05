# Paired Metadata Refusal

The three generic durable openers, `FjallStore`, `FjallReplicaStore`, and
`FjallCatalogReplicaStore`, now refuse a directory containing any reserved paired
metadata marker. This is a reject-only guard, not a paired-store constructor,
writer, format producer, migration, inspector, or adoption API. Existing format
versions, profiles, public traits, and error variants are unchanged.

## Presence Before Admission

The closed marker set is `[0x22, 0x01]`, `[0x22, 0x02]`, and `[0x22, 0x03]` in
the metadata keyspace. Every present value fences the directory, including an
empty, malformed, unsupported, wrong-role, or oversized value. There is no
valid-shape bypass. Identical keys in the business-record keyspace remain ordinary
caller records and do not trigger the guard.

Each opener retains its existing database open and metadata-keyspace acquisition,
then pins one temporary snapshot. The private helper performs at most three
fixed-key `size_of` probes, stopping at the first present marker. It does not
retrieve marker values, parse them, scan either dictionary, copy caller bytes, or
convert a declared marker length into an allocation. Backend lookup and recovery
internals are not thereby allocation-free.

Only a clear result allows record-keyspace acquisition and the existing
format/profile/initialization checks or fresh-directory stamp. Marker refusal
therefore precedes those application actions and ordinary-role admission. It
returns the existing `StorageError::CorruptMetadata` with the static detail
`paired replica metadata cannot be opened by a generic store`. That refusal
contains no marker value, directory, identifier, or hash.

A probe failure returns the existing backend error with operation
`check a paired replica marker`; it does not continue to admit a writer. The
underlying backend cause remains a low-level cause, not a sanitized external
diagnostic. For directories without markers, the old logical behavior and
subsequent refusal ordering remain unchanged, but the extra probes add read
failure possibilities and resource/timing cost. Exact old I/O traces are not
promised. Retry policy, deadlines, durability, and production worker configuration
are unchanged.

## Recovery Boundary

This guard is not a noncreating physical opener. Fjall recovery can replay a
journal, synthesize internal metadata, or remove stray/uninitialized native
keyspaces before the guard runs. Metadata-keyspace acquisition can also create
that keyspace. Those existing backend effects remain allowed.

The guarantee is no new application format/profile/initialization stamp,
record-keyspace acquisition, or generic admission after marker discovery. It is
not filesystem immutability, arbitrary damaged-directory inspection, physical
repair prevention, or a retroactive change to historical binaries. The guard
does not certify a marker's contents or grant paired-role authority.

There is no paired writer, canonical domain/native seed validation, engine
activation, adoption/completion receipt, or resource-custody guarantee in this
increment. Reserving marker presence is a prerequisite for future isolated work,
not evidence that those features exist.

The separate [controlled paired-storage prototype](paired-storage-prototype.md)
adds private test-only physical writers. It does not turn this production guard
into a constructor, inspector, adoption capability or custody receipt.

## Evidence

Eight private test families cover every marker and all three public generic
openers; seven value shapes including an actual value over 128 KiB; every common
header subset and matching profile; malformed headers and multiple markers;
marker-only metadata without a record keyspace; pinned-view presence across live
insertion/deletion; unchanged no-marker behavior and refusal ordering; and static
marker-refusal diagnostics. The source uses only presence probes; these tests
are not a global allocation-counter proof.

Fixtures have valid native layouts. Logical damage is written through ordinary
synchronous batches, not by corrupting native files. Refused opens compare
complete owned logical metadata/business inventories before and after two
independent recovery reopens, with all physical handles dropped between opens.
The marker-only fixture checks record-keyspace absence without acquiring it.
No-marker cases include initialized records, retained catalogs, fresh profiles,
and marker-byte keys stored as ordinary business records.

The private capture helpers operate on controlled small fixtures; they are not a
new bounded production inspection API. Fixture and capture builders request one
backend worker, while public openers retain their existing defaults. Focused
tests run serially, and broad suites use two test threads. These are per-builder
or test-run settings, not a global thread, allocation, or RSS cap. Database Drop
does not supply an actual native-thread join receipt. No new application owner
threads or Tokio tasks are introduced.

## Verification

The focused suite passed all eight tests, followed by ten serial repeats with
eight passes each. The all-feature storage suite passed 146 tests across five
targets. Both all-feature and default-feature workspace suites passed 4,625
tests across 131 targets, with ten pre-existing ignored tests in each run.

Formatting, strict all-target workspace lint checks in both feature
configurations, both workspace builds, the administration protocol descriptor,
and whitespace checks passed. The four source files remained byte-identical
through this final verification group. No live SDK gate was selected, and this
increment does not explain or fix the retained earlier SDK failure.
