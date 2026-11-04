# DuckDB 2.0.0 and the DuckLake extension, pinned to the exact builds the
# connector talks to.
#
# Why `fetchurl` of the official release rather than `pkgs.duckdb`: nixpkgs
# carries DuckDB 1.5.2, and a DuckLake extension is loadable only by the DuckDB
# build it was compiled against — the version is part of the ABI, not a
# preference.  Building 2.0.0 from source in Nix would work and would cost a C++
# build on every evaluation; the published `duckdb-shared-libs-*` asset is the
# same bytes the release ships and is what users of the connector will have.
#
# Why the extension is vendored: the DuckLake extension is fetched from
# `extensions.duckdb.org` at `INSTALL` time.  A test that needs egress is not a
# test, and the bytes are version-specific anyway.  The official build is
# *signed*, so neither the CLI nor the connector needs
# `allow_unsigned_extensions` — verified: the signature validates against
# DuckDB's built-in keys (the S1 spike ran with the flag off).
#
# Hashes are the SRI digests of the published assets.  If a hash is missing for
# a platform, `throw` rather than guess: a wrong hash is a broken test env, and
# the message says how to record it.  To record one:
#
#   nix store prefetch-file --json \
#     https://github.com/duckdb/duckdb/releases/download/v2.0.0/<asset> \
#     | jq -r .hash
#
# Verified: aarch64-darwin, x86_64-linux.
{ pkgs }:
let
  inherit (pkgs) lib stdenv;
  version = "2.0.0";

  # DuckDB names the release assets differently from the extension repository:
  # `osx-arm64` vs `osx_arm64`, and `linux-amd64` vs `linux_amd64`.  Both are
  # spelled out here so a typo fails at evaluation, not at the first `LOAD`.
  platforms = {
    aarch64-darwin = {
      asset = "osx-arm64";
      extension = "osx_arm64";
      library = "libduckdb.dylib";
      libHash = "sha256-w11gCAsxul0Ro85QWmgxvnhPADtMV10ypMK62ztBzfA=";
      cliHash = "sha256-GIo2/oiD71LmkWWgy7YDHvZ5yObESKHOIpUZZBSbf5Y=";
      extHash = "sha256-STxhjArlmYQJFF+EZ6iF5w7507YxKTS4azBoF7J904w=";
      scannerHash = "sha256-SWozhpVYKpnbKCZZev/AMwek1+17ksly4mGt/kDwP+M=";
    };
    x86_64-darwin = {
      asset = "osx-amd64";
      extension = "osx_amd64";
      library = "libduckdb.dylib";
      libHash = null;
      cliHash = null;
      extHash = null;
      scannerHash = null;
    };
    x86_64-linux = {
      asset = "linux-amd64";
      extension = "linux_amd64";
      library = "libduckdb.so";
      libHash = "sha256-mTrYnn38pmzW2plMLK0JDC/BYN359VBJigmKtLsTz7E=";
      cliHash = "sha256-xlByBrr3eTxgE9vYI2hGmkXyDcDhem+S8gRgL2Uu0rw=";
      extHash = "sha256-/xxzCl1afLFO2VR7oVwlnCSAXrTPEsF3J8Kk+v/hrac=";
      scannerHash = "sha256-VLPXUcr8n/pDmIaxYH0O51jatNwHDwFx53m5Xy8+VU8=";
    };
    aarch64-linux = {
      asset = "linux-arm64";
      extension = "linux_arm64";
      library = "libduckdb.so";
      libHash = null;
      cliHash = null;
      extHash = null;
      scannerHash = null;
    };
  };

  system = stdenv.hostPlatform.system;
  platform =
    platforms.${system}
      or (throw "ducklake-2.0: no DuckDB ${version} assets recorded for ${system}");

  need = name: value:
    if value == null then
      throw "ducklake-2.0: no hash recorded for ${system} ${name}; record one with `nix store prefetch-file --json <url> | jq -r .hash`"
    else
      value;

  # A DuckDB release asset, e.g. `duckdb-shared-libs-osx-arm64`.
  release = name: hash:
    pkgs.fetchurl {
      url = "https://github.com/duckdb/duckdb/releases/download/v${version}/${name}.tar.gz";
      inherit hash;
    };  # The extension files, in the order the install phase unpacks them.
  extensionSources = [
    (pkgs.fetchurl {
      url = "https://extensions.duckdb.org/v${version}/${platform.extension}/ducklake.duckdb_extension.gz";
      hash = need "extension" platform.extHash;
    })
    (pkgs.fetchurl {
      url = "https://extensions.duckdb.org/v${version}/${platform.extension}/postgres_scanner.duckdb_extension.gz";
      hash = need "postgres_scanner" platform.scannerHash;
    })
  ];
in
{
  inherit version;

  # The file name of the shared library inside `libduckdb`, so callers can build
  # `$DUCKLAKE_DUCKDB_LIB` without repeating the per-platform suffix.
  library = platform.library;

  # `libduckdb` — the shared library the connector `dlopen`s.  Also carries the
  # headers, so anything that wants the C API declaration can diff against them.
  # `libduckdb` — the shared library the connector `dlopen`s.  Also carries the
  # headers, so anything that wants the C API declaration can diff against them.
  libduckdb = stdenv.mkDerivation {
    pname = "libduckdb";
    inherit version;
    src = release "duckdb-shared-libs-${platform.asset}" (need "shared libs" platform.libHash);
    # The tarball is flat: `libduckdb.*` and the headers side by side.
    sourceRoot = ".";
    dontConfigure = true;
    dontBuild = true;
    installPhase = ''
      runHook preInstall
      mkdir -p $out/lib $out/include
      cp ${platform.library} $out/lib/
      cp *.h $out/include/
      runHook postInstall
    '';
  };

  # The CLI, version-matched: a 1.x CLI cannot open a 2.0 catalog file, so the
  # devenv one is useless for inspecting these catalogs.
  cli = stdenv.mkDerivation {
    pname = "duckdb";
    inherit version;
    src = release "duckdb-cli-${platform.asset}" (need "cli" platform.cliHash);
    # The tarball holds a single flat binary rather than a directory.
    sourceRoot = ".";
    dontConfigure = true;
    dontBuild = true;
    installPhase = ''
      runHook preInstall
      install -D -m755 duckdb $out/bin/duckdb
      runHook postInstall
    '';
  };

  # The extensions, laid out the way `extension_directory` expects:
  # `<dir>/v<version>/<platform>/<name>.duckdb_extension`.
  #
  # `postgres_scanner` is vendored beside `ducklake` because a PostgreSQL catalog
  # makes DuckDB autoload it: without it here, the first
  # `ATTACH 'ducklake:postgres:…'` tries to download it into whichever
  # `extension_directory` is set — which on Nix is a read-only store path, and
  # therefore fails with `Permission denied` rather than an honest "no network".
  extension = stdenv.mkDerivation {
    pname = "ducklake-extension";
    inherit version;
    srcs = extensionSources;
    nativeBuildInputs = [ pkgs.gzip ];
    # Stdenv would try to unpack the `.gz` files itself; they are not tarballs.
    dontUnpack = true;
    dontConfigure = true;
    dontBuild = true;
    installPhase = ''
      runHook preInstall
      mkdir -p $out/v${version}/${platform.extension}
      gunzip -c ${builtins.elemAt extensionSources 0} > $out/v${version}/${platform.extension}/ducklake.duckdb_extension
      gunzip -c ${builtins.elemAt extensionSources 1} > $out/v${version}/${platform.extension}/postgres_scanner.duckdb_extension
      runHook postInstall
    '';
  };
}
