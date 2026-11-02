{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

let
  python = pkgs.python313;
in
{
  # ---------------------------------------------------------------------------
  # Packages
  # ---------------------------------------------------------------------------
  packages = [
    pkgs.git
    pkgs.uv
    pkgs.jq
    pkgs.curl
    pkgs.duckdb # the CLI, for poking at a catalog by hand
    pkgs.docker-client # to run the Feldera stack
    pkgs.lean4 # to re-check the protocol model this connector refines
    pkgs.postgresql_17 # a psql client for a PostgreSQL-backed DuckLake catalog
  ];

  # ---------------------------------------------------------------------------
  # Python
  # ---------------------------------------------------------------------------
  # `uv` owns the virtualenv (`.venv`) and the lockfile; the interpreter comes
  # from the shell so that the venv never downloads one of its own.
  languages.python = {
    enable = true;
    package = python;
    venv.enable = false;
  };

  env = {
    UV_PYTHON_PREFERENCE = "only-system";
    UV_PYTHON = "${python}/bin/python3.13";
    UV_PROJECT_ENVIRONMENT = "${config.devenv.root}/.venv";
    FELDERA_URL = "http://127.0.0.1:18080";
    # DuckLake catalogs live under the project, not in $HOME.
    FD_CATALOG_ROOT = "${config.devenv.root}/.scratch/catalogs";
  };

  # ---------------------------------------------------------------------------
  # Services
  # ---------------------------------------------------------------------------
  # A real DuckLake catalog is PostgreSQL; the scenarios use a local DuckDB
  # metadata catalog so they need nothing, but the end-to-end test can point
  # `--catalog` at this server to exercise the production shape.
  services.postgres = {
    enable = true;
    package = pkgs.postgresql_17;
    initialDatabases = [
      {
        name = "ducklake";
        user = "ducklake";
        pass = "ducklake";
      }
    ];
    listen_addresses = "127.0.0.1";
    port = 5432;
  };

  # ---------------------------------------------------------------------------
  # Tasks
  # ---------------------------------------------------------------------------
  tasks."fd:sync".exec = "uv sync --group dev";

  # ---------------------------------------------------------------------------
  # Scripts
  # ---------------------------------------------------------------------------
  scripts = {
    "fd-test".exec = ''
      uv run pytest tests/ -q "$@"
    '';

    # The fast check: the publication protocol against throwaway DuckLake
    # catalogs.  No Feldera needed.
    "fd-scenarios".exec = ''
      uv run fd-connectors validate-local "$@"
    '';

    # The end-to-end check: a real pipeline-manager plus a real DuckLake.
    # Skips itself if FELDERA_URL is not answering.
    "fd-e2e".exec = ''
      uv run pytest tests/test_end_to_end.py -q "$@"
    '';

    "feldera-up".exec = ''
      set -eu
      name="''${FELDERA_CONTAINER:-feldera}"
      if docker ps --format '{{.Names}}' | grep -qx "$name"; then
        echo "$name is already running"
      else
        docker rm -f "$name" >/dev/null 2>&1 || true
        docker run -d --name "$name" -p 18080:8080 \
          images.feldera.com/feldera/pipeline-manager:latest
      fi
      for _ in $(seq 1 60); do
        if curl -sf http://127.0.0.1:18080/v0/config >/dev/null; then
          echo "feldera up: http://127.0.0.1:18080"
          exit 0
        fi
        sleep 2
      done
      echo "feldera did not become healthy" >&2
      exit 1
    '';

    "feldera-down".exec = ''
      docker rm -f "''${FELDERA_CONTAINER:-feldera}" >/dev/null 2>&1 || true
    '';
  };

  # ---------------------------------------------------------------------------
  # Shell
  # ---------------------------------------------------------------------------
  enterShell = ''
    uv sync --group dev --quiet
    echo "fd-connectors dev shell"
    echo "  fd-scenarios   protocol scenarios against throwaway DuckLake catalogs"
    echo "  fd-e2e         end-to-end test against a live Feldera stack"
    echo "  feldera-up     start the Feldera pipeline-manager container"
  '';

  enterTest = ''
    uv run pytest tests/ -q
    uv run fd-connectors validate-local
  '';
}