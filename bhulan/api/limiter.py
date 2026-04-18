"""
Shared slowapi :class:`Limiter` instance.

Route decorators (``@limiter.limit(...)``) and slowapi's exception handler
(which reads ``request.app.state.limiter._inject_headers``) must operate on
the *same* Limiter object — otherwise limits are tracked on one instance
while header injection reads from another that has no knowledge of the
window state. Keeping the constructor in its own module avoids a circular
import: ``app.py`` imports the router from ``routes/insights.py``, and both
sides need the limiter at module load time.
"""

from slowapi import Limiter
from slowapi.util import get_remote_address

#: Single process-wide rate limiter. Individual per-route limits are attached
#: via ``@limiter.limit(...)`` on the handlers in ``bhulan/api/routes``.
limiter = Limiter(key_func=get_remote_address, default_limits=[])
