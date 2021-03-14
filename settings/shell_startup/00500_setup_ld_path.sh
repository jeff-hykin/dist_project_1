# find the path to the c lib
output="$(
    nix-instantiate --eval -E '"${
        (rec {
            packageJson = builtins.fromJSON (builtins.readFile ./settings/requirements/simple_nix.json);
            mainRepo = builtins.fetchTarball {url="https://github.com/NixOS/nixpkgs/archive/${packageJson.nix.mainRepo}.tar.gz";};
            mainPackages = builtins.import mainRepo {
                config = packageJson.nix.config;
            };
            path = mainPackages.stdenv.cc.cc.lib;
        }).path
    }"' | sed -E 's/^"|"$//g'
)"
# prevents the libstdc++.so.6 errors 
export LD_LIBRARY_PATH="$output/lib/:$LD_LIBRARY_PATH"
# FIXME: don't hardcode this stuff
path_to_openssl_file="/nix/store/r74ag7zrmm6yyfhgxbifz5xvnks3slg6-openssl-1.1.1g-dev/include/openssl/opensslv.h"
if ! [[ -f "$path_to_openssl_file" ]]
then
    echo 
    echo 
    echo "Update the _setup_ld_path.sh file REF: ifj305835"
    echo "use the .nix_raw_find opensslv.h to find a path and update the variable in this file"
    echo 
    echo 
else
    export LD_LIBRARY_PATH="$path_to_openssl_file:$LD_LIBRARY_PATH"
fi