"""`snowmig.py provision` end to end against a fake transport: what reaches
the operator -- exit code, provision_result.json, PROVISION.md, stderr --
when the environment does not cooperate.
"""
import json

import pytest

import snowmig
from target import provisioning
from target.provisioning import ProvisionTransportError
from test_provisioning import Fake


OCID = "ocid1.aidataplatform.oc1.iad.fakefakefakefake"


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr(provisioning.time, "sleep", lambda _s: None)


def _install(monkeypatch, fake):
    monkeypatch.setattr(provisioning, "make_provision_call",
                        lambda ocid, **kw: fake)


def _provision(tmp_path, *extra):
    return snowmig.main(["provision", "--datalake-ocid", OCID,
                         "--workspace-name", "acme", "--skip-libraries",
                         "--out-dir", str(tmp_path), *extra])


class Always409(Fake):
    """The workspace is still CREATING; every cluster POST is a 409."""

    def __call__(self, operation, **kw):
        if operation == "create_cluster":
            self.ops.append((operation, kw))
            raise ProvisionTransportError(
                "create_cluster failed (exit 0): 409 Conflict ongoing "
                "operation")
        return super().__call__(operation, **kw)


def test_a_cluster_409_still_writes_the_provision_record(tmp_path,
                                                         monkeypatch):
    """The first live `provision --execute` on a fresh DataLake is expected
    to hit this. It used to exit 1 with a bare `error:` line and NO
    artifact, so the workspace it had just created was recorded nowhere."""
    fake = Always409()
    _install(monkeypatch, fake)
    rc = _provision(tmp_path, "--execute")
    assert rc == 1
    res = json.loads((tmp_path / "provision_result.json").read_text(
        encoding="utf-8"))
    assert any(s["step"] == "workspace" and s["verified"] is True
               for s in res["steps"])
    assert res["workspace"]["key"] == "ws-acme"
    md = (tmp_path / "PROVISION.md").read_text(encoding="utf-8")
    assert "--reuse-existing" in md and "ws-acme" in md
    assert fake.workspaces, "the workspace really was created server-side"
