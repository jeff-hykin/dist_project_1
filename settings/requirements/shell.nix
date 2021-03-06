# 
# how to add packages?
# 
    # you can search for them here: https://search.nixos.org/packages
    # to find them in the commandline use:
    #     nix-env -qP --available PACKAGE_NAME_HERE | cat
    # ex:
    #     nix-env -qP --available opencv
    #
    # NOTE: some things (like setuptools) just don't show up in the 
    # search results for some reason, and you just have to guess and check 🙃 

# Lets setup some definitions
let        
    definitions = rec {
        # 
        # load the simple_nix.json cause were going to extract basically everything from there
        # 
        packageJson = builtins.fromJSON (builtins.readFile ./simple_nix.json);
        # 
        # load the store with all the packages, and load it with the config
        # 
        mainRepo = builtins.fetchTarball {url="https://github.com/NixOS/nixpkgs/archive/${packageJson.nix.mainRepo}.tar.gz";};
        mainPackages = builtins.import mainRepo {
            config = packageJson.nix.config;
        };
        # 
        # reorganize the list of packages from:
        #    [ { load: "blah", from:"blah-hash" }, ... ]
        # into a list like:
        #    [ { name: "blah", commitHash:"blah-hash", source: (*an object*) }, ... ]
        # 
        packagesWithSources = builtins.map (
            each: ({
                name = each.load;
                commitHash = each.from;
                source = builtins.getAttr each.load (
                    builtins.import (
                        builtins.fetchTarball {url="https://github.com/NixOS/nixpkgs/archive/${each.from}.tar.gz";}
                    ) {
                        config = packageJson.nix.config;
                    }
                );
            })
        ) packageJson.nix.packages;
    };
  
    # TODO: add support for the simple_nix.json to have nested packages
    nestedPackages = [
        # 
        # this is just a list of all of the standard unix tools
        # 
        definitions.mainPackages.unixtools.arp         # depends on openssl_1_0_2     
        definitions.mainPackages.unixtools.ifconfig    # depends on openssl_1_0_2         
        definitions.mainPackages.unixtools.netstat     # depends on openssl_1_0_2         
        definitions.mainPackages.unixtools.ping        # depends on openssl_1_0_2     
        definitions.mainPackages.unixtools.route       # depends on openssl_1_0_2         
        # definitions.mainPackages.unixtools.logger # fail on macos
        # definitions.mainPackages.unixtools.wall   # fail on macos
        definitions.mainPackages.unixtools.col
        definitions.mainPackages.unixtools.column
        definitions.mainPackages.unixtools.fdisk
        definitions.mainPackages.unixtools.fsck
        definitions.mainPackages.unixtools.getconf
        definitions.mainPackages.unixtools.getent
        definitions.mainPackages.unixtools.getopt
        definitions.mainPackages.unixtools.hexdump
        definitions.mainPackages.unixtools.hostname
        definitions.mainPackages.unixtools.killall
        definitions.mainPackages.unixtools.locale
        definitions.mainPackages.unixtools.more
        definitions.mainPackages.unixtools.mount
        definitions.mainPackages.unixtools.ps
        definitions.mainPackages.unixtools.quota
        definitions.mainPackages.unixtools.script
        definitions.mainPackages.unixtools.sysctl
        definitions.mainPackages.unixtools.top
        definitions.mainPackages.unixtools.umount
        definitions.mainPackages.unixtools.whereis
        definitions.mainPackages.unixtools.write
        definitions.mainPackages.unixtools.xxd
    ];
    
    majorCustomDependencies = rec {
        python = [
            definitions.mainPackages.python27
            definitions.mainPackages.python27Packages.setuptools
            definitions.mainPackages.python27Packages.pip
            definitions.mainPackages.python27Packages.virtualenv
            definitions.mainPackages.python27Packages.wheel
        ];
    };
    
    subDepedencies = [] ++ majorCustomDependencies.python;
    
    # TODO: add support for the simple_nix.json to have OS-specific packages (if statement inside package inclusion)
    packagesForMacOnly = [] ++ definitions.mainPackages.lib.optionals (definitions.mainPackages.stdenv.isDarwin) [
        
    ];
    
# using those definitions
in
    # create a shell
    definitions.mainPackages.mkShell {
        # inside that shell, make sure to use these packages
        buildInputs = subDepedencies ++ nestedPackages ++ packagesForMacOnly ++ builtins.map (each: each.source) definitions.packagesWithSources;
        
        # run some bash code before starting up the shell
        shellHook = ''
        export PROJECT_HOME="settings/home"
        export PROJECT_FOLDER="$PWD"
        # we don't want to give nix or other apps our home folder
        if [[ "$HOME" != "$(pwd)/$PROJECT_HOME" ]] 
        then
            #
            # find and run all the startup scripts in alphabetical order
            #
            # for file in ./settings/shell_startup/#pre_changing_home/*
            # do
            #    # make sure its a file
            #    if [[ -f "$file" ]]; then
            #        source "$file"
            #    fi
            # done
            
            mkdir -p "$PROJECT_HOME/.cache/"
            ln -s "$HOME/.cache/nix" "$PROJECT_HOME/.cache/" &>/dev/null
            
            # so make the home folder the same as the project folder
            export HOME="$(pwd)/$PROJECT_HOME"
            # make it explicit which nixpkgs we're using
            export NIX_PATH="nixpkgs=${definitions.mainRepo}:."
        fi
        '';
    }
