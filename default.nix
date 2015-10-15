{ mkDerivation, base, conduit, conduit-combinators, containers
, monad-control, persistent, split, stdenv
}:
mkDerivation {
  pname = "persistent-chunked";
  version = "0.0.4";
  src = ./.;
  buildDepends = [
    base conduit conduit-combinators containers monad-control
    persistent split
  ];
  homepage = "https://github.com/circuithub/persistent-chunked";
  description = "Helpers for running chunked queries with persistent";
  license = stdenv.lib.licenses.mit;
}
