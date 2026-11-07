#!/bin/sh
script_user=`whoami`
script_dir=$(realpath "$(dirname "$0")")

echo "USER:" ${script_user}
echo "DIR:" ${script_dir}
echo

sudo chown vscode .venv

curl -LsSf https://astral.sh/uv/install.sh | sh

# Shell autocompletion

# Determine your shell (e.g., with `echo $SHELL`), then run one of:
echo 'eval "$(uv generate-shell-completion bash)"' >> ~/.bashrc
echo 'eval "$(uv generate-shell-completion zsh)"' >> ~/.zshrc
# echo 'uv generate-shell-completion fish | source' >> ~/.config/fish/config.fish
# echo 'eval (uv generate-shell-completion elvish | slurp)' >> ~/.elvish/rc.elv

# Determine your shell (e.g., with `echo $SHELL`), then run one of:
echo 'eval "$(uvx --generate-shell-completion bash)"' >> ~/.bashrc
echo 'eval "$(uvx --generate-shell-completion zsh)"' >> ~/.zshrc
# echo 'uvx --generate-shell-completion fish | source' >> ~/.config/fish/config.fish
# echo 'eval (uvx --generate-shell-completion elvish | slurp)' >> ~/.elvish/rc.elv

uv self update
