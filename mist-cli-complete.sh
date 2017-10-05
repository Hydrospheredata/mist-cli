_mist_cli_completion() {
    COMPREPLY=( $( env COMP_WORDS="${COMP_WORDS[*]}" \
                   COMP_CWORD=$COMP_CWORD \
                   _MIST_CLI_COMPLETE=complete $1 ) )
    return 0
}

complete -F _mist_cli_completion -o default mist-cli;
