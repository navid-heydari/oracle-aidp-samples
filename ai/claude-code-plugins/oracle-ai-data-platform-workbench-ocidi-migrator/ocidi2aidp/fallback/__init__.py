"""The LLM path of the compiler (approach "C").

``migrate`` writes one work order, ``fallback/<id>.json``, per construct it
could not translate deterministically, and leaves a stub in the notebook that
raises. This package turns a work order into a prompt, accepts an answer
(``fallback/<id>.py``), validates it statically, and splices it into the
notebook under an ``LLM-ASSISTED -- REVIEW REQUIRED`` banner.

Who writes the answer:

* ``session`` (default) -- Claude in the Claude Code session, through the
  ``ocidi-fallback`` skill and the ``ocidi-fallback-author`` agent. Nothing
  leaves the machine beyond what the session already sends.
* ``anthropic`` -- the Anthropic API directly (``ANTHROPIC_API_KEY`` or an
  ``ant auth login`` profile), for headless / CI runs.

Either way the answer goes through the same validator; an answer that only
raises ``NotImplementedError`` is recorded as *declined* and the object is
marked ``manual``.
"""
