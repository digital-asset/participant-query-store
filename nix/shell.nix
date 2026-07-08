{ pkgs, ci }:
let
  inherit (pkgs) stdenv;
  helm-values-schema-json = import ./helm-values-schema-json.nix;
  helm-unittest = import ./helm-unittest.nix;
  dpm = import ./dpm.nix;
  requiredPackages = with pkgs; ([
    # these packages are required both in CI and for local development
    ## dynamic makefile packages:
    gnumake
    ## etx legacy packages:
    bashInteractive
    coreutils
    curl
    (callPackage dpm { })
    file
    gawk
    git
    gh
    gnugrep
    gnused
    jq
    (wrapHelm kubernetes-helm { plugins = [
      kubernetes-helmPlugins.helm-diff
      (callPackage helm-unittest { })
      (callPackage helm-values-schema-json { })
    ]; })
    ## operator supplied packages:
    entr
    go
    gopls
    go-jsonnet
    jdk21_headless
    jsonnet-bundler
    (mill.override { jre = pkgs.jdk21_headless; })
    ## assistant defaults:
    google-cloud-sdk
    oras
    ## CircleCI dynamic workflow packages:
    circleci-cli
    cue
    ## Blackduck scanning
    maven
  ] ++ (if ci then [
    # these packages should only be installed on CI
  ] else [
    # these packages are only installed on developer machines locally
    ## copyright header management packages:
    pre-commit
    python3
    ## operator supplied dev only packages:
    scalafmt
  ])) ++ (lib.optionals stdenv.isDarwin [
          pkgs.libiconv
          ]);
in
pkgs.mkShell {
  packages = requiredPackages;
  LC_ALL = if stdenv.isDarwin then "" else "C.UTF-8";

  shellHook = ''
    # there is a nix bug that the directory deleted by _nix_shell_clean_tmpdir can be the same as the general $TEMPDIR
    eval "$(declare -f _nix_shell_clean_tmpdir | sed 's/_nix_shell_clean_tmpdir/orig__nix_shell_clean_tmpdir/')"
    _nix_shell_clean_tmpdir() {
        orig__nix_shell_clean_tmpdir "$@"
        mkdir -p "$TEMPDIR" # ensure system TEMPDIR still exists
    }
    # Inject DEVENV_ROOT to assist with migration from devenv:
    export DEVENV_ROOT=$(git rev-parse --show-toplevel)

    # Inject provided bin directory in to path:
    export PATH=''$(pwd)/.scaffold/bin:$PATH
    '';
}
