"""A small Feldera REST client: ingress, completion tokens, egress streaming.

Only the endpoints the connectors need are implemented, and every response is
parsed defensively: the pipeline-manager's exact envelope differs between the
version distributed as a wheel, the ``latest`` container image, and the source
tree, so the client accepts the known shapes rather than pinning one.
"""

from __future__ import annotations

import contextlib
import json
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import httpx

JSON_ARRAY_INGRESS = "json"


class FelderaError(Exception):
    """The Feldera HTTP API returned an error or an unusable response."""


class PipelineNotFound(FelderaError):
    pass


@dataclass(frozen=True)
class CompletionStatus:
    status: str
    step: int | None = None

    @property
    def complete(self) -> bool:
        return self.status.lower() in ("complete", "completed", "done")


@dataclass(frozen=True)
class OutputChunk:
    """One chunk of a Feldera output stream."""

    sequence_number: int
    processed_steps: int
    snapshot: int | bool | None
    updates: tuple[Mapping[str, Any], ...]
    raw: Mapping[str, Any] = field(default_factory=dict)

    @property
    def is_snapshot(self) -> bool:
        return bool(self.snapshot)


def _token_from(response: httpx.Response) -> str | None:
    body: Any = None
    try:
        body = response.json()
    except (json.JSONDecodeError, ValueError):
        body = None
    if isinstance(body, str) and body:
        return body
    if isinstance(body, Mapping):
        for key in ("token", "completion_token", "id"):
            value = body.get(key)
            if isinstance(value, str) and value:
                return value
    for header in ("x-completion-token", "completion-token", "x-request-id", "x-feldera-token"):
        value = response.headers.get(header)
        if value:
            return value
    return None


def _updates_from_chunk(obj: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    data = obj.get("json_data", obj.get("data"))
    if data is None:
        return ()
    if isinstance(data, Mapping):
        collected: list[Mapping[str, Any]] = []
        for op in ("delete", "insert"):
            value = data.get(op)
            if value is None:
                continue
            if isinstance(value, list):
                collected.extend({op: row} for row in value)
            else:
                collected.append({op: value})
        return tuple(collected)
    if isinstance(data, list):
        return tuple(item for item in data if isinstance(item, Mapping))
    if isinstance(data, str):
        parsed = json.loads(data)
        if isinstance(parsed, Mapping):
            return _updates_from_chunk({"json_data": parsed})
        return tuple(item for item in parsed if isinstance(item, Mapping))
    return ()


def chunk_from_json(obj: Mapping[str, Any]) -> OutputChunk:
    return OutputChunk(
        sequence_number=int(obj.get("sequence_number", 0)),
        processed_steps=int(obj.get("processed_steps", 0)),
        snapshot=obj.get("snapshot"),
        updates=_updates_from_chunk(obj),
        raw=obj,
    )


class FelderaClient:
    """HTTP client for one pipeline-manager instance."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 30.0,
        stream_timeout: float = 600.0,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout, follow_redirects=True)
        self.stream_timeout = stream_timeout

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> FelderaClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- generic -----------------------------------------------------------

    def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        url = f"{self.base_url}{path}"
        try:
            response = self._client.request(method, url, **kwargs)
        except httpx.HTTPError as exc:
            raise FelderaError(f"{method} {url} failed: {exc}") from exc
        if response.status_code == 404:
            raise PipelineNotFound(f"{method} {url}: {response.text[:400]}")
        if response.status_code >= 400:
            raise FelderaError(
                f"{method} {url}: HTTP {response.status_code}: {response.text[:800]}"
            )
        return response

    def get_json(self, path: str, **kwargs: Any) -> Any:
        return self.request("GET", path, **kwargs).json()

    def health(self) -> Any:
        return self.get_json("/v0/config")

    # -- pipelines ---------------------------------------------------------

    def create_pipeline(
        self,
        name: str,
        sql: str,
        *,
        profile: str = "unoptimized",
        description: str = "",
        runtime_config: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        body: dict[str, Any] = {
            "name": name,
            "description": description,
            "program_code": sql,
            "program_config": {"profile": profile},
            "runtime_config": dict(runtime_config or {}),
        }
        return self.request("POST", "/v0/pipelines", json=body).json()

    def put_pipeline(self, name: str, sql: str, **kwargs: Any) -> Mapping[str, Any]:
        """Create the pipeline, or replace it if it already exists."""
        try:
            return self.create_pipeline(name, sql, **kwargs)
        except FelderaError as exc:
            if "already exists" not in str(exc).lower():
                raise
            body: dict[str, Any] = {
                "name": name,
                "description": kwargs.get("description", ""),
                "program_code": sql,
                "program_config": {"profile": kwargs.get("profile", "unoptimized")},
                "runtime_config": dict(kwargs.get("runtime_config") or {}),
            }
            return self.request("PUT", f"/v0/pipelines/{name}", json=body).json()

    def pipeline(self, name: str) -> Mapping[str, Any] | None:
        try:
            return self.get_json(f"/v0/pipelines/{name}")
        except PipelineNotFound:
            return None

    def pipeline_status(self, name: str) -> str | None:
        """The pipeline's most specific known state.

        ``/stats`` is only available once the pipeline is deployed and answers
        503 otherwise, so the pipeline object's ``deployment_status`` (and, while
        compiling, ``program_status``) is the fallback.
        """
        try:
            body = self.get_json(f"/v0/pipelines/{name}/stats")
            state = body.get("global_metrics", {}).get("state") or body.get("state")
            if state:
                return str(state)
        except PipelineNotFound:
            return None
        except FelderaError:
            pass
        try:
            pipeline = self.get_json(f"/v0/pipelines/{name}")
        except PipelineNotFound:
            return None
        return str(pipeline.get("deployment_status") or pipeline.get("program_status") or "unknown")

    def start(self, name: str) -> None:
        self.request("POST", f"/v0/pipelines/{name}/start")

    def start_and_wait(
        self, name: str, *, timeout: float = 240.0, poll_interval: float = 0.5
    ) -> Mapping[str, Any]:
        """Start the pipeline and wait until it is actually running.

        A pipeline is created in ``Pending`` and compiled in the background; an
        immediate start therefore has to wait for ``program_status`` to settle
        before the deployment is usable.  Compilation failures are reported
        with the compiler message instead of a timeout.
        """
        self.start(name)
        deadline = time.monotonic() + timeout
        last: tuple[Any, ...] | None = None
        last_start = time.monotonic()
        while time.monotonic() < deadline:
            info = self.pipeline(name)
            if info is None:
                raise PipelineNotFound(f"pipeline {name} disappeared while starting")
            program = str(info.get("program_status", "")).lower()
            deployment = str(info.get("deployment_status", "")).lower()
            current = (deployment, program)
            if current != last:
                last = current
            # A failed provision leaves the deployment ``stopped`` and reports
            # the reason here, rather than through the start request.
            error = info.get("deployment_error")
            if error:
                raise FelderaError(f"pipeline {name} failed to start: {json.dumps(error)[:1200]}")
            if any(state in program for state in ("sqlerror", "rusterror", "systemerror")):
                raise FelderaError(
                    f"pipeline {name} failed to compile: "
                    f"{json.dumps(info.get('program_error'))[:1200]}"
                )
            if deployment in ("running", "paused", "unavailable"):
                return info
            # A start that arrives before compilation finishes is accepted
            # (202) and then dropped, leaving the deployment stopped with no
            # error at all.  Re-issue it until the deployment moves.
            if program == "success" and time.monotonic() - last_start > 2.0:
                last_start = time.monotonic()
                with contextlib.suppress(FelderaError):
                    self.start(name)
            time.sleep(poll_interval)
        raise FelderaError(f"pipeline {name} did not start within {timeout}s (last: {last})")

    def pause(self, name: str) -> None:
        self.request("POST", f"/v0/pipelines/{name}/pause")

    def stop(self, name: str, *, force: bool = False) -> None:
        """Stop a pipeline.

        A graceful stop is an enterprise-only feature in the community edition
        (``501 EnterpriseFeature``), so fall back to a forceful stop rather
        than leaving the pipeline running.
        """
        query = "?force=true" if force else ""
        try:
            self.request("POST", f"/v0/pipelines/{name}/stop{query}")
        except FelderaError as exc:
            if force or "enterprise-only" not in str(exc):
                raise
            self.request("POST", f"/v0/pipelines/{name}/stop?force=true")

    def delete(self, name: str, *, timeout: float = 120.0) -> None:
        """Delete a pipeline, stopping it first if necessary.

        The API refuses to delete a pipeline that is not fully stopped, and
        stopping is asynchronous, so deletion is a stop-and-retry loop rather
        than a single request.
        """
        deadline = time.monotonic() + timeout
        while True:
            try:
                self.request("DELETE", f"/v0/pipelines/{name}")
                return
            except PipelineNotFound:
                return
            except FelderaError as exc:
                # Refusing to delete something that is still up is expected;
                # anything else is a real error.
                if "HTTP 400" not in str(exc) and "not fully stopped" not in str(exc):
                    raise
                with contextlib.suppress(FelderaError):
                    self.stop(name, force=True)
            if time.monotonic() >= deadline:
                raise FelderaError(f"pipeline {name} could not be deleted within {timeout}s")
            time.sleep(1.0)

    def wait_for_state(
        self,
        name: str,
        states: Sequence[str],
        *,
        timeout: float = 120.0,
        poll_interval: float = 0.2,
    ) -> str:
        """Block until the pipeline enters one of ``states`` (matched loosely)."""
        wanted = {s.lower() for s in states}
        deadline = time.monotonic() + timeout
        last: str | None = None
        while time.monotonic() < deadline:
            state = self.pipeline_status(name)
            if state is not None:
                last = state
                if state.lower() in wanted or any(s in state.lower() for s in wanted):
                    return state
            time.sleep(poll_interval)
        raise FelderaError(
            f"pipeline {name} did not reach {list(states)} within {timeout}s (last: {last})"
        )

    # -- ingress -----------------------------------------------------------

    def push_updates(
        self,
        pipeline: str,
        table: str,
        updates: Iterable[Mapping[str, Any]],
        *,
        fmt: str = JSON_ARRAY_INGRESS,
        array: bool = True,
    ) -> str | None:
        """Push insert/delete updates and return the completion token, if any."""
        payload = list(updates)
        params = {"format": fmt}
        if array:
            params["array"] = "true"
        response = self.request(
            "POST",
            f"/v0/pipelines/{pipeline}/ingress/{table}",
            params=params,
            content=json.dumps(payload),
            headers={"content-type": "application/json"},
        )
        return _token_from(response)

    def adhoc_query(
        self, pipeline: str, sql: str, *, timeout: float = 60.0
    ) -> list[dict[str, Any]]:
        """Run an ad-hoc SQL query against a running pipeline.

        Only materialized tables and views can be queried, and the ``json``
        format answers with newline-delimited objects rather than an array.
        """
        response = self.request(
            "GET",
            f"/v0/pipelines/{pipeline}/query",
            params={"sql": sql, "format": "json"},
            timeout=timeout,
        )
        return [json.loads(line) for line in response.text.splitlines() if line.strip()]

    def completion_status(self, pipeline: str, token: str) -> CompletionStatus:
        body = self.get_json(f"/v0/pipelines/{pipeline}/completion_status", params={"token": token})
        if isinstance(body, Mapping):
            status = str(body.get("status", "unknown"))
            step = body.get("step")
            return CompletionStatus(status=status, step=int(step) if step is not None else None)
        return CompletionStatus(status=str(body))

    def wait_for_completion(
        self,
        pipeline: str,
        token: str | None,
        *,
        timeout: float = 120.0,
        poll_interval: float = 0.05,
    ) -> CompletionStatus:
        """Wait until the token's effects are visible to output consumers.

        A ``None`` token means the ingress response carried no token, which the
        REST API allows when the request is processed synchronously; the caller
        then has no ordering guarantee and the returned status says so.
        """
        if token is None:
            return CompletionStatus(status="untracked", step=None)
        deadline = time.monotonic() + timeout
        while True:
            status = self.completion_status(pipeline, token)
            if status.complete:
                return status
            if time.monotonic() >= deadline:
                raise FelderaError(
                    f"completion token {token} on {pipeline} still {status.status} after {timeout}s"
                )
            time.sleep(poll_interval)

    # -- egress ------------------------------------------------------------

    def egress(
        self,
        pipeline: str,
        view: str | None = None,
        *,
        backpressure: bool = True,
        send_snapshot: bool = False,
        update_format: str = "insert_delete",
        fmt: str = "json",
        timeout: float | None = None,
    ) -> Iterator[OutputChunk]:
        """Stream output chunks for ``view`` (or all views when it is ``None``)."""
        path = f"/v0/pipelines/{pipeline}/egress"
        if view is not None:
            path = f"{path}/{view}"
        params = {
            "format": fmt,
            "backpressure": "true" if backpressure else "false",
            "send_snapshot": "true" if send_snapshot else "false",
            "update_format": update_format,
        }
        url = f"{self.base_url}{path}"
        stream_timeout = self.stream_timeout if timeout is None else timeout
        with httpx.Client(timeout=httpx.Timeout(stream_timeout, read=stream_timeout)) as client:
            try:
                with client.stream("POST", url, params=params) as response:
                    if response.status_code >= 400:
                        response.read()
                        raise FelderaError(
                            f"POST {url}: HTTP {response.status_code}: {response.text[:800]}"
                        )
                    for line in response.iter_lines():
                        line = line.strip()
                        if not line or line.startswith(":"):
                            continue
                        try:
                            obj = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if isinstance(obj, Mapping):
                            yield chunk_from_json(obj)
            except httpx.HTTPError as exc:
                raise FelderaError(f"POST {url} stream failed: {exc}") from exc
