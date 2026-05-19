# Agent Instructions

## Country DB Build Memory

Before starting any new country fundamentals DB build or major country-cache redesign, read `COUNTRY_DB_BUILD_INSIGHTS.md` first and use it as the checklist for universe definition, source selection, quote enrichment, cache fields, audit outputs, and range-scan behavior.

When working on country fundamentals databases, range scans, cache builders, ticker/universe mapping, official filing parsers, fallback data sources, or cache audit/debugging, update `COUNTRY_DB_BUILD_INSIGHTS.md` whenever a reusable insight is learned.

Record insights when they are likely to help future country DB work, including:

- universe definition and filtering rules
- exchange instrument quirks
- issuer/ticker/company-id mapping issues
- official filing parser limitations
- fallback source behavior
- currency and unit conversion rules
- manual override or unavailable-marker patterns
- range scan filtering behavior
- performance, audit, and validation lessons
- commands or workflows that should be reused

Keep entries concise and practical. Do not add transient debugging notes, one-off local paths, secrets, API keys, or raw cache outputs. Prefer general rules, failure modes, and verified fixes.

If the user asks for an implementation change and a new lesson is learned during the work, update the code first, verify it, then add or update the relevant section in `COUNTRY_DB_BUILD_INSIGHTS.md` before finalizing.

## Country DB Cache Execution Boundary

Full fundamentals cache builds are user-run operations because they can take a long time, consume API quotas, and depend on the user's local credentials and rate limits.

For country DB work, the agent should:

- run only small smoke/test cache builds needed to validate code paths, parsers, resume behavior, and audit outputs
- avoid launching full-country cache builds unless the user explicitly asks the agent to run that long job
- provide the exact full-cache command for the user to run manually when implementation is ready
- recommend writing full rebuilds to a new output file first, then validating counts/status/audit files before replacing the production cache
- explain expected runtime/rate-limit implications and how to resume or rerun safely
- verify with unit tests, syntax checks, fixture data, and small targeted samples rather than full API sweeps
