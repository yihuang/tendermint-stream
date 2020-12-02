with (import <nixpkgs> { });
let env = pkgs.poetry2nix.mkPoetryEnv {
  projectDir = ./.;
};
in
mkShell {
  buildInputs = [
    env
  ] ++
  (with python3Packages; [ flake8 black isort ]);
}
