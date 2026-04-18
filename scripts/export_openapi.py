"""
Export the FastAPI app's OpenAPI schema to ``openapi.json`` at the repo root.

Used by the frontend codegen pipeline (``npm run gen:api``) so TypeScript
types stay in lockstep with the pydantic request/response models. Also
committed to the repo as the source of truth for clients who don't want to
run the backend locally just to inspect the schema.
"""

from __future__ import annotations

import json
from pathlib import Path

from bhulan.api.app import app


def main() -> None:
    schema = app.openapi()
    out = Path(__file__).resolve().parent.parent / "openapi.json"
    out.write_text(json.dumps(schema, indent=2, sort_keys=False) + "\n")
    print(f"wrote {out} ({len(schema.get('paths', {}))} paths)")


if __name__ == "__main__":
    main()
