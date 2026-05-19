# Agent Instructions

## Country DB Build Memory

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

