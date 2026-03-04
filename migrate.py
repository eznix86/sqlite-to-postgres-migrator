#!/usr/bin/env python3
"""Interactive CLI tool for migrating SQLite databases to PostgreSQL via pgloader."""

import json
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path
from typing import Any, NoReturn

PGLOADER_IMAGE = "dimitri/pgloader:latest"
CONTAINER_SQLITE_PATH = "/data/production.sqlite3"

CONFIG_DIR = Path.home() / ".config" / "pgloader-migrator"
BIN_DIR = CONFIG_DIR / "bin"
CONFIG_FILE = CONFIG_DIR / "config.json"

PINNED_GUM_VERSION = "0.16.2"
GUM_RELEASE_API = f"https://api.github.com/repos/charmbracelet/gum/releases/tags/v{PINNED_GUM_VERSION}"
PGLOADER_RELEASE_API = "https://api.github.com/repos/dimitri/pgloader/releases/latest"

gum_cmd: str = "gum"


def die(msg: str, code: int = 1) -> NoReturn:
    _log("error", msg, use_stderr=True)
    sys.exit(code)


def ok(msg: str) -> None:
    _log("info", msg, prefix="OK")


def info(msg: str) -> None:
    _log("info", msg)


def _log(
    level: str, msg: str, *, prefix: str | None = None, use_stderr: bool = False
) -> None:
    stream = sys.stderr if use_stderr else sys.stdout

    if shutil.which(gum_cmd):
        cmd = [gum_cmd, "log", "--level", level]
        if prefix:
            cmd += ["--prefix", prefix]
        cmd.append(msg)
        subprocess.run(cmd, check=False, stdout=stream, stderr=stream)
        return

    label = prefix or level.upper()
    print(f"[{label}] {msg}", file=stream)


def load_config() -> dict[str, Any]:
    if not CONFIG_FILE.exists():
        return {}
    try:
        return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_config(data: dict[str, Any]) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    """Run a command, capturing stdout and stderr."""
    return subprocess.run(
        cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )


def run_gum(
    *args: str, input_text: str | None = None
) -> subprocess.CompletedProcess[str]:
    """Run a gum sub-command, capturing stdout while keeping stderr on the terminal."""
    return subprocess.run(
        [gum_cmd, *args],
        text=True,
        input=input_text,
        stdin=sys.stdin if input_text is None else None,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
    )


def spin(title: str, cmd: list[str]) -> subprocess.CompletedProcess[str]:
    """Run a command behind a gum spinner, falling back gracefully if gum is absent."""
    if shutil.which(gum_cmd):
        return run([gum_cmd, "spin", f"--title={title}", "--", *cmd])
    return run(cmd)


def ask(
    header: str, *, default: str = "", prompt: str = "> ", password: bool = False
) -> str:
    args = ["input", f"--header={header}", f"--prompt={prompt}"]
    if default:
        args.append(f"--value={default}")
    if password:
        args.append("--password")

    result = run_gum(*args)
    if result.returncode != 0:
        die("Input cancelled.")
    return result.stdout.strip()


def choose(header: str, options: list[str]) -> str:
    result = run_gum(
        "choose", "--header", header, "--height", "10", "--no-show-help", *options
    )
    if result.returncode != 0 or not result.stdout.strip():
        die("No option selected.")
    return result.stdout.strip()


def default_migration_mode(config: dict[str, Any]) -> str:
    mode = config.get("migration_mode")
    if mode in {"schema_with_data", "data_only", "schema_only"}:
        return mode

    # Backward compatibility with the old boolean flag.
    if bool(config.get("migrate_schema", False)):
        return "schema_with_data"

    return "data_only"


def confirm(prompt: str, *, default_yes: bool) -> bool:
    cmd = [gum_cmd, "confirm", prompt]
    if default_yes:
        cmd.append("--default")

    code = subprocess.run(cmd).returncode
    if code == 0:
        return True
    if code == 1:
        return False

    # Treat anything else (e.g. Ctrl+C → 130) as a clean abort.
    sys.exit(code)


def pick_file(header: str) -> Path:
    result = run_gum(
        "filter", f"--header={header}", "--placeholder=Type to search files..."
    )
    if result.returncode != 0 or not result.stdout.strip():
        die("No file selected.")
    return Path(result.stdout.strip()).expanduser().resolve()


def print_summary_table(rows: list[tuple[str, str]]) -> None:
    if not shutil.which(gum_cmd):
        for key, value in rows:
            print(f"  {key}: {value}")
        return

    lines = "\n".join(f"{k}|{v}" for k, v in rows)
    result = run_gum(
        "table",
        "--print",
        "--separator",
        "|",
        "--columns",
        "Setting,Value",
        "--border",
        "rounded",
        "--no-show-help",
        input_text=lines,
    )
    if result.stdout:
        print(result.stdout, end="")


def fetch_json(
    url: str,
) -> dict[str, Any] | None:
    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        info(f"Network request failed ({url}): {exc}")
        return None


def strip_version_prefix(value: str) -> str:
    return value.strip().lstrip("v")


def parse_semver(text: str) -> str | None:
    match = re.search(r"(\d+\.\d+\.\d+)", text)
    return match.group(1) if match else None


def _gum_asset_name(version: str) -> str:
    system = platform.system()
    arch = platform.machine().lower()
    is_arm = arch in {"arm64", "aarch64"}

    if system == "Darwin":
        suffix = "arm64" if is_arm else "x86_64"
        return f"gum_{version}_Darwin_{suffix}.tar.gz"

    if system == "Linux":
        suffix = "arm64" if is_arm else "x86_64"
        return f"gum_{version}_Linux_{suffix}.tar.gz"

    die(f"Unsupported OS: {system}")


def _install_gum(version: str, assets: list[dict[str, Any]]) -> None:
    asset_filename = _gum_asset_name(version)
    download_url = next(
        (a["browser_download_url"] for a in assets if a.get("name") == asset_filename),
        None,
    )
    if not download_url:
        die(f"No gum release asset found for this platform: {asset_filename}")

    with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    info(f"Downloading gum {version}...")
    with urllib.request.urlopen(download_url, timeout=60) as response:
        tmp_path.write_bytes(response.read())

    with tarfile.open(tmp_path, "r:gz") as archive:
        binary = next(
            (
                m
                for m in archive.getmembers()
                if m.isfile() and Path(m.name).name == "gum"
            ),
            None,
        )
        if binary is None:
            die("Could not find gum binary inside the release archive.")

        extracted = archive.extractfile(binary)
        if extracted is None:
            die("Could not read gum binary from the release archive.")

        gum_path = BIN_DIR / "gum"
        gum_path.write_bytes(extracted.read())

    gum_path.chmod(0o755)
    tmp_path.unlink(missing_ok=True)
    ok(f"Installed gum {version} → {gum_path}")


def _current_gum_version() -> str | None:
    if not shutil.which(gum_cmd):
        return None
    result = run([gum_cmd, "--version"])
    return (
        parse_semver(result.stdout + result.stderr) if result.returncode == 0 else None
    )


def ensure_gum(config: dict[str, Any]) -> None:
    global gum_cmd

    managed = BIN_DIR / "gum"
    gum_cmd = (
        str(managed) if managed.exists() else (shutil.which("gum") or str(managed))
    )

    release = fetch_json(GUM_RELEASE_API)
    if release is None:
        die(
            "Could not fetch gum release info from GitHub. Check your internet connection."
        )

    target = strip_version_prefix(release.get("tag_name", ""))
    if not target:
        die("Could not resolve pinned gum version from GitHub.")

    installed = _current_gum_version()

    if installed is None:
        BIN_DIR.mkdir(parents=True, exist_ok=True)
        _install_gum(target, release.get("assets", []))
        gum_cmd = str(managed)

    elif strip_version_prefix(installed) != target:
        if confirm(f"Update gum v{installed} → v{target}?", default_yes=True):
            _install_gum(target, release.get("assets", []))
            gum_cmd = str(managed)
        else:
            target = installed  # keep whatever is installed

    else:
        ok(f"gum is up to date (v{installed})")

    config["gum_version"] = target


def require_docker() -> None:
    if shutil.which("docker") is None:
        die("'docker' not found. Install Docker and retry.")

    result = spin("Checking Docker daemon...", ["docker", "info"])
    if result.returncode != 0:
        die("Docker is not running (or you lack permission). Start Docker and retry.")

    ok("Docker daemon is running")


def _image_exists(image: str) -> bool:
    return (
        spin(
            f"Checking for image {image}...", ["docker", "image", "inspect", image]
        ).returncode
        == 0
    )


def ensure_pgloader_image(config: dict[str, Any]) -> None:
    if not _image_exists(PGLOADER_IMAGE):
        info(f"Image not found locally: {PGLOADER_IMAGE}")
        if confirm("Pull pgloader Docker image now?", default_yes=True):
            subprocess.run(["docker", "pull", PGLOADER_IMAGE], check=True)
            ok(f"Pulled: {PGLOADER_IMAGE}")
        else:
            die("pgloader image is required. Aborting.")
    else:
        ok(f"Docker image available: {PGLOADER_IMAGE}")

    release = fetch_json(PGLOADER_RELEASE_API)
    if release is None:
        info("Could not check for pgloader updates (continuing anyway).")
        return

    latest = strip_version_prefix(release.get("tag_name", ""))
    current = config.get("pgloader_version", "")

    if latest and current and current != latest:
        if confirm(
            f"Upgrade pgloader metadata v{current} → v{latest}?", default_yes=True
        ):
            subprocess.run(["docker", "pull", PGLOADER_IMAGE], check=True)
            config["pgloader_version"] = latest
            ok(f"pgloader updated to v{latest}")
            return

    if latest:
        config["pgloader_version"] = latest


def _validate_port(port: str) -> str:
    if not port.isdigit() or not (1 <= int(port) <= 65535):
        die(f"Invalid port number: {port!r}. Must be an integer between 1 and 65535.")
    return port


_LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0"}
_DOCKER_HOST = "host.docker.internal"


def _normalize_host(host: str) -> str:
    """Remap loopback addresses to host.docker.internal for Docker networking."""
    if host.strip().lower() in _LOOPBACK_HOSTS:
        info(f"Remapping {host!r} → {_DOCKER_HOST!r} for Docker networking.")
        return _DOCKER_HOST
    return host


def main() -> None:
    BIN_DIR.mkdir(parents=True, exist_ok=True)

    config = load_config()
    ensure_gum(config)
    save_config(config)

    require_docker()
    ensure_pgloader_image(config)

    previous_sqlite = config.get("sqlite_path", "")
    if previous_sqlite and Path(previous_sqlite).exists():
        choice = choose(
            "SQLite source",
            [f"Reuse previous file ({previous_sqlite})", "Choose another file"],
        )
        sqlite_path = (
            Path(previous_sqlite)
            if choice.startswith("Reuse")
            else pick_file("Select your SQLite DB file")
        )
    else:
        sqlite_path = pick_file("Select your SQLite DB file")

    if not sqlite_path.is_file():
        die(f"SQLite file not found: {sqlite_path}")

    db_name = ask("Postgres DB name:", default=config.get("db_name", "your_db"))
    host = _normalize_host(
        ask("Postgres host:", default=config.get("host", "localhost"))
    )
    port = _validate_port(ask("Postgres port:", default=config.get("port", "5432")))
    user = ask("Postgres user:", default=config.get("user", "postgres"))
    password = ask(
        "Postgres password:", default=config.get("password", ""), password=True
    )

    if password and not config.get("password_warning_shown"):
        info(
            "Note: your password will be saved in plaintext in ~/.config/pgloader-migrator/config.json."
        )
        config["password_warning_shown"] = True

    migration_mode_options = {
        "Schema + data": "schema_with_data",
        "Data only": "data_only",
        "Schema only": "schema_only",
    }
    current_mode = default_migration_mode(config)
    default_mode_label = next(
        label
        for label, value in migration_mode_options.items()
        if value == current_mode
    )

    mode_label = choose(
        f"Migration mode (current: {default_mode_label})",
        list(migration_mode_options.keys()),
    )
    migration_mode = migration_mode_options[mode_label]

    verbose = confirm(
        "Enable verbose output?",
        default_yes=bool(config.get("verbose", True)),
    )
    migrate_schema = migration_mode != "data_only"

    config.update(
        {
            "sqlite_path": str(sqlite_path),
            "db_name": db_name,
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "migration_mode": migration_mode,
            "migrate_schema": migrate_schema,
            "verbose": verbose,
        }
    )
    save_config(config)

    pg_uri = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

    docker_cmd = [
        "docker",
        "run",
        "--rm",
        "-it",
        "-v",
        f"{sqlite_path}:{CONTAINER_SQLITE_PATH}",
        PGLOADER_IMAGE,
        "pgloader",
    ]
    if migration_mode == "data_only":
        docker_cmd += ["--with", "DATA ONLY"]
    if migration_mode == "schema_only":
        docker_cmd += ["--with", "SCHEMA ONLY"]
    if verbose:
        docker_cmd += ["--verbose"]
    docker_cmd += [f"sqlite:///{CONTAINER_SQLITE_PATH}", pg_uri]

    masked_uri = f"postgresql://{user}:***@{host}:{port}/{db_name}"
    print_summary_table(
        [
            ("SQLite", str(sqlite_path)),
            ("Postgres", masked_uri),
            ("Migration mode", mode_label),
            ("Verbose", "yes" if verbose else "no"),
            ("Image", PGLOADER_IMAGE),
        ]
    )

    exit_code = 0
    if confirm("Run migration now?", default_yes=True):
        exit_code = subprocess.run(docker_cmd).returncode
    else:
        ok("Cancelled.")

    if confirm(
        "Clear saved config (~/.config/pgloader-migrator/config.json)?",
        default_yes=False,
    ):
        save_config({})
        ok("Config cleared.")

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
