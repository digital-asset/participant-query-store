{
  buildGoModule,
  fetchFromGitHub,
  lib,
}:

buildGoModule rec {
  pname = "helm-values-schema-json";
  version = "HEAD";

  src = fetchFromGitHub {
    owner = "losisin";
    repo = pname;
    rev = "9dd66662903a6a8dbf0dcfdca1b76e91f6ddc7bc";
    hash = "sha256-q+akFINLujpGDj2ne5h0dM4K7kedJcXMM1RT/1X99TU=";
  };

  vendorHash = "sha256-ammxgC7m29AjwnnQr/Kl8dyotOC54HRmvU43jf/mChg=";

  # NOTE: Remove the install and upgrade hooks.
  postPatch = ''
    sed -i '/^hooks:/,+2 d' plugin.yaml
  '';

  postInstall = ''
    install -dm755 $out/${pname}
    mv $out/bin/helm-values-schema-json $out/${pname}/schema
    rmdir $out/bin
    install -m644 -Dt $out/${pname} plugin.yaml
  '';
}
