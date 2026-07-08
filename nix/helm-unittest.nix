{
  buildGoModule,
  fetchFromGitHub,
  lib,
  yq-go
}:

buildGoModule rec {
  pname = "helm-unittest";
  version = "HEAD";

  src = fetchFromGitHub {
    owner = pname;
    repo = pname;
    rev = "844bfe2bddc43483146f0ccd05f6ab1a14253a6d";
    hash = "sha256-tlweCQ0rRRjyp96tUOvo/SlBZeNIPYlS7JUn4phHHLI=";
  };

  vendorHash = "sha256-j0s0hPCGx5zn5s7dO3bSxFsAXkGxIFPfqUizxF3nkR4=";

  nativeBuildInputs = [yq-go];

  # NOTE: Remove the install and upgrade hooks.
  postPatch = ''
    sed -i '/^hooks:/,+2 d' plugin.yaml
  '';

  postInstall = ''
    install -dm755 $out/${pname}
    mv $out/bin/helm-unittest $out/${pname}/untt
    rmdir $out/bin
    install -m644 -Dt $out/${pname} plugin.yaml
  '';
}
