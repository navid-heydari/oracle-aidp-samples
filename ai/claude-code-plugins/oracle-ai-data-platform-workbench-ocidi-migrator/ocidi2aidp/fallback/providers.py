"""Headless fallback author: the Anthropic API.

Used by ``ocidi2aidp fallback run --provider anthropic``. Inside Claude Code
the default is the session itself (the ``ocidi-fallback`` skill); this path
exists for CI and batch runs. Credentials resolve the SDK's usual way
(``ANTHROPIC_API_KEY``, ``ANTHROPIC_AUTH_TOKEN`` or an ``ant auth login``
profile) -- nothing is read from or written to the migration config.

What is sent: the work order (DI operator JSON, field names, parameter names
and defaults, the reason). No row data. See PRIVACY.md.
"""
from __future__ import annotations

from pathlib import Path

from .workorder import load_orders, render_prompt, response_path

DEFAULT_MODEL = "claude-opus-5-5"

SYSTEM = ("You write PySpark for a data-migration compiler. Your output is spliced into a "
          "notebook after static validation and is then read by a human reviewer. Faithfulness "
          "to the source semantics matters more than producing an answer: when the information "
          "given is not enough, raise NotImplementedError and say what is missing.")


class ProviderError(Exception):
    pass


def run_anthropic(out_dir, *, model: str = DEFAULT_MODEL, only=None, overwrite: bool = False,
                  client=None) -> list:
    """Fill every pending work order. Returns [(id, state, detail)]."""
    try:
        import anthropic
    except ImportError as exc:
        raise ProviderError("the anthropic package is not installed: pip install anthropic") \
            from exc
    client = client or anthropic.Anthropic()
    results = []
    for order in load_orders(Path(out_dir)):
        fid = order["id"]
        if only and fid not in only:
            continue
        target = response_path(Path(out_dir), fid)
        if target.exists() and not overwrite:
            results.append((fid, "skipped", "an answer already exists"))
            continue
        try:
            response = client.beta.messages.create(
                model=model,
                max_tokens=16000,
                system=SYSTEM,
                output_config={"effort": "high"},
                betas=["server-side-fallback-2026-07-01"],
                fallbacks="default",
                messages=[{"role": "user", "content": render_prompt(order)}],
            )
        except anthropic.AuthenticationError as exc:
            raise ProviderError("Anthropic authentication failed; set ANTHROPIC_API_KEY or run "
                                "`ant auth login`") from exc
        except anthropic.BadRequestError as exc:
            results.append((fid, "error", f"bad request: {exc.message}"))
            continue
        except anthropic.RateLimitError:
            results.append((fid, "error", "rate limited; re-run later"))
            continue
        except anthropic.APIStatusError as exc:
            results.append((fid, "error", f"API error {exc.status_code}"))
            continue
        except anthropic.APIConnectionError:
            results.append((fid, "error", "network error reaching the Anthropic API"))
            continue
        if response.stop_reason == "refusal":
            results.append((fid, "refused", "the model declined this work order"))
            continue
        text = "".join(b.text for b in response.content if b.type == "text")
        if not text.strip():
            results.append((fid, "error", f"empty answer (stop_reason {response.stop_reason})"))
            continue
        target.write_text(text, encoding="utf-8")
        results.append((fid, "written", str(target)))
    return results
