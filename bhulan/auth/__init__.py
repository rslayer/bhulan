"""User accounts + request history (opt-in).

The module is additive — the public ``/v1`` insights surface continues to
work anonymously. When :attr:`~bhulan.config.settings.Settings.BHULAN_AUTH_ENABLED`
is true and :attr:`~bhulan.config.settings.Settings.BHULAN_DB_PATH` points
at a writable location, the API gains magic-link login, a session-token
bearer flow, and server-side persistence of each ``/v1/insights`` run so
the caller can browse their own history.
"""
