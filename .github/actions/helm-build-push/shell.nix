let
  rev = "25.11";
  sha256 = "1zn1lsafn62sz6azx6j735fh4vwwghj8cc9x91g5sx2nrg23ap9k";

  nixPkgs = import (builtins.fetchTarball {
    url = "https://github.com/NixOS/nixpkgs/archive/${rev}.tar.gz";
    inherit sha256;
  }) { };

in
nixPkgs.mkShell (
  with nixPkgs;
  {
    packages = [
      (wrapHelm kubernetes-helm { plugins = [ kubernetes-helmPlugins.helm-unittest ]; })
      yq-go
    ];
  }
)
