# Development Toolchain

This repository uses a small set of tools to keep local development reproducible
without requiring every developer to install the same tools globally.

## Official Toolchain

| Responsibility | Tool | Repository usage |
|---|---|---|
| Runtime versions | [mise](https://mise.jdx.dev/) | Installs and selects Java 21, Node 22, and Python 3.11 from `.mise.toml`. |
| Java dependencies and builds | [Maven Wrapper](https://maven.apache.org/wrapper/) | Use `./fr-batch-service/mvnw`; no system Maven is required. |
| Node dependencies and scripts | [npm](https://docs.npmjs.com/) | Uses `batch-ops-ui/package.json` and `package-lock.json`. |
| Python CLI applications | [pipx](https://pipx.pypa.io/stable/) | Recommended for isolated tools such as `podman-compose`. |

The project intentionally does not require SDKMAN!, pyenv, fnm, nodenv, or
system-wide Maven. They remain valid alternatives described below.

## mise

[mise](https://mise.jdx.dev/) manages multiple language runtimes and development
tools from a project configuration. It automatically selects versions when the
shell is activated inside the repository.

```bash
mise trust
mise install

java -version
node --version
python --version
```

Without shell activation, run commands through mise explicitly:

```bash
mise exec -- java -version
mise exec -- python --version
```

The pinned versions live in [`.mise.toml`](.mise.toml).

## Java

Java dependencies are declared in `pom.xml` files and are resolved by Maven.
Use the repository wrapper:

```bash
./fr-batch-service/mvnw test
./fr-batch-service/mvnw package
task deps
```

The wrapper downloads Maven when needed. Installing Maven globally with
[SDKMAN!](https://sdkman.io/) is optional, but it is not used by project tasks.

### Java alternatives

- [SDKMAN!](https://sdkman.io/): manages JDKs and JVM tools such as Maven,
  Gradle, Kotlin, and Groovy.
- [asdf](https://asdf-vm.com/): general version manager with Java plugins;
  `mise` is compatible with its `.tool-versions` format.
- [jenv](https://www.jenv.be/): selects installed JDKs without managing the
  complete development toolchain.

## Node.js

The UI uses npm and keeps its dependency graph in
`batch-ops-ui/package-lock.json`:

```bash
cd batch-ops-ui
npm install
npm ci
npm run check
npm test
npm run build
```

Do not commit another lockfile or mix package managers for the same installation.
`pnpm` and Yarn are possible future alternatives, but switching would require a
separate migration and a new lockfile.

### Node alternatives

- [fnm](https://github.com/Schniz/fnm): fast, Rust-based, cross-platform Node
  version manager; supports `.node-version` and `.nvmrc`.
- [nodenv](https://github.com/nodenv/nodenv): Unix-oriented Node version manager
  using `.node-version`; Node installation is provided by
  [node-build](https://github.com/nodenv/node-build).
- [nvm](https://github.com/nvm-sh/nvm): widely adopted shell-based Node version
  manager for Unix-like systems.
- [asdf](https://asdf-vm.com/): general-purpose version manager with a Node
  plugin.

## Python

Python is included in `.mise.toml` because local scripts and Python-based CLI
tools need a predictable interpreter. The repository does not currently define a
Python application or Python dependency lockfile.

### Recommended CLI installation: pipx

[pipx](https://pipx.pypa.io/stable/) installs each Python CLI application in its
own virtual environment and exposes its executable on `PATH`:

```bash
pipx install podman-compose
pipx upgrade podman-compose
pipx uninstall podman-compose
```

On macOS, one supported installation is:

```bash
brew install pipx
pipx ensurepath
```

### Alternative: pip and virtual environments

For a Python project, use the standard-library `venv` module and install packages
only inside that environment:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

The third-party [virtualenv](https://virtualenv.pypa.io/) tool is another way to
create environments. [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv)
adds environment management to [pyenv](https://github.com/pyenv/pyenv). Neither
is required when using mise and `python -m venv`.

### Alternative: uv

[uv](https://docs.astral.sh/uv/) can install Python versions, create virtual
environments, manage dependencies, and install CLI tools:

```bash
uv python install 3.11
uv venv --python 3.11
uv pip install -r requirements.txt
uv tool install podman-compose
```

`uv` is a good choice if this repository later gains a real Python component.
For the current scripts, `mise` plus `pipx` is intentionally simpler.

### Python version alternatives

- [pyenv](https://github.com/pyenv/pyenv): installs and selects multiple Python
  versions globally, per shell, or per project.
- [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv): manages virtual
  environments associated with pyenv versions.
- [asdf](https://asdf-vm.com/): manages Python through a plugin alongside other
  runtimes.
- [uv](https://docs.astral.sh/uv/): combines Python installation, environments,
  package management, and CLI tools.

## Why These Choices

- `mise` centralizes runtime versions and prevents Java, Node, or Python drift.
- Maven Wrapper makes Java builds independent of globally installed Maven.
- npm matches the existing UI lockfile and requires no migration.
- pipx isolates Python command-line applications such as `podman-compose`.
- Alternatives are documented for contributors who already use another manager,
  but project instructions should continue using the official toolchain above.
