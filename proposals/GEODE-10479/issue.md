# GEODE-10479 — parent issue reference

Source: https://issues.apache.org/jira/browse/GEODE-10479
Reconciled September 13, 2026. Public state: Open; assignee Sai Boorlagadda.

The issue addresses Java 17 migration deprecation/removal warnings and incremental warning
re-enablement. Its acceptance criteria include removing global suppressions, enabling
`options.deprecation`, zero deprecation/removal warnings in a clean build, CI warning gates,
updated API documentation and no performance regression.

Jinwoo clarified that Java 21 migration follows the Jakarta migration; this cleanup does not
itself establish the Java 21 build/runtime support matrix. Proposed changes to scope and
exceptions must be agreed in the public issue/discussion, not inferred from a branch checklist.

See [todo.md](todo.md) for merged work and residuals, [spec.md](spec.md) for the proposed
warning policy and [plan.md](plan.md) for remaining work. This concise reference replaces the
stale copied issue text; consult Jira for the complete current description and conversation.
