#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

DATABASE_URL_DEFAULT="postgresql+psycopg://app:password@localhost:5433/app_db"
PSQL_URL_DEFAULT="postgresql://app:password@localhost:5433/app_db"
BOOTSTRAP_STATE_FILE=".bootstrap-state.json"

SNAPSHOT_PATH=""
EMPTY_DB=false
RESET_DB=false
SKIP_FRONTEND=false
SKIP_VERIFY=false

# Print the bootstrap CLI usage text.
# Parameters:
#   None.
# Output:
#   Writes the command synopsis, supported flags, and behavior notes to stdout.
usage() {
  cat <<'EOF'
Usage:
  ./scripts/bootstrap.sh [--snapshot PATH] [--empty-db] [--reset-db] [--skip-frontend] [--skip-verify]

Options:
  --snapshot PATH   Import a specific SQL snapshot file.
  --empty-db        Apply migrations only and leave the Docker DB empty.
  --reset-db        Drop managed schemas, reapply migrations, and reimport the snapshot.
  --skip-frontend   Skip npm install/build for my-app.
  --skip-verify     Skip smoke validation and frontend build verification.
  -h, --help        Show this help.

Notes:
  - Default behavior imports the bundled snapshot only when snapshot-managed tables are empty.
  - If no bundled snapshot is available, use --snapshot PATH or --empty-db.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot)
      SNAPSHOT_PATH="${2:-}"
      if [[ -z "$SNAPSHOT_PATH" ]]; then
        echo "Missing value for --snapshot." >&2
        exit 1
      fi
      shift 2
      ;;
    --empty-db)
      EMPTY_DB=true
      shift
      ;;
    --reset-db)
      RESET_DB=true
      shift
      ;;
    --skip-frontend)
      SKIP_FRONTEND=true
      shift
      ;;
    --skip-verify)
      SKIP_VERIFY=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "$EMPTY_DB" == true && -n "$SNAPSHOT_PATH" ]]; then
  echo "--empty-db cannot be combined with --snapshot." >&2
  exit 1
fi

# Detect the operating-system family for prerequisite guidance.
# Parameters:
#   None.
# Output:
#   Writes one of: macos, ubuntu, linux, other.
detect_os() {
  case "$(uname -s)" in
    Darwin)
      echo "macos"
      ;;
    Linux)
      if [[ -r /etc/os-release ]]; then
        # shellcheck disable=SC1091
        source /etc/os-release
        if [[ "${ID:-}" == "ubuntu" || "${ID_LIKE:-}" == *"ubuntu"* || "${ID_LIKE:-}" == *"debian"* ]]; then
          echo "ubuntu"
          return
        fi
      fi
      echo "linux"
      ;;
    *)
      echo "other"
      ;;
  esac
}

OS_FLAVOR="$(detect_os)"

# Compute the SHA-256 hash of one file.
# Parameters:
#   $1: Path to the file to hash.
# Output:
#   Writes the lowercase SHA-256 hex digest to stdout.
sha256_file() {
  local file_path="$1"
  python3.12 - "$file_path" <<'PY'
import hashlib
import sys
from pathlib import Path

path = Path(sys.argv[1])
digest = hashlib.sha256()
with path.open("rb") as handle:
    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
        digest.update(chunk)
print(digest.hexdigest())
PY
}

# Resolve a relative or absolute path to an absolute repo-aware path.
# Parameters:
#   $1: Raw path string from CLI input or internal defaults.
# Output:
#   Writes the resolved absolute path to stdout.
resolve_abs_path() {
  local raw_path="$1"
  python3.12 - "$REPO_ROOT" "$raw_path" <<'PY'
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
raw_path = Path(sys.argv[2])
if not raw_path.is_absolute():
    raw_path = repo_root / raw_path
print(raw_path.resolve())
PY
}

# Resolve the default bundled snapshot path when one exists.
# Parameters:
#   None.
# Output:
#   Writes the resolved snapshot path to stdout and returns 0 when found.
#   Returns 1 when no bundled snapshot exists.
resolve_default_snapshot() {
  local candidate
  for candidate in "shared_data_snapshot.sql" "data_platform/runtime/shared_data_snapshot.sql"; do
    if [[ -f "$candidate" ]]; then
      resolve_abs_path "$candidate"
      return 0
    fi
  done
  return 1
}

# Read one key from the local bootstrap state file.
# Parameters:
#   $1: JSON key to read from .bootstrap-state.json.
# Output:
#   Writes the stored value to stdout when present.
#   Returns 1 when the state file does not exist yet.
state_get() {
  local key="$1"
  [[ -f "$BOOTSTRAP_STATE_FILE" ]] || return 1
  python3.12 - "$BOOTSTRAP_STATE_FILE" "$key" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
key = sys.argv[2]
data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
value = data.get(key, "")
if value is None:
    value = ""
print(value)
PY
}

# Persist the bootstrap dependency and snapshot metadata for future reruns.
# Parameters:
#   $1: SHA-256 hash of requirements.txt.
#   $2: SHA-256 hash of my-app/package-lock.json, or an empty string.
#   $3: Snapshot mode string: none, empty, or snapshot.
#   $4: Absolute snapshot path, or an empty string.
#   $5: SHA-256 hash of the active snapshot, or an empty string.
# Output:
#   Writes .bootstrap-state.json and returns through exit status only.
write_state() {
  local python_hash="$1"
  local npm_hash="$2"
  local snapshot_mode="$3"
  local snapshot_path="$4"
  local snapshot_hash="$5"
  python3.12 - "$BOOTSTRAP_STATE_FILE" "$python_hash" "$npm_hash" "$snapshot_mode" "$snapshot_path" "$snapshot_hash" <<'PY'
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

state_path = Path(sys.argv[1])
payload = {
    "python_requirements_hash": sys.argv[2],
    "npm_lock_hash": sys.argv[3],
    "snapshot_mode": sys.argv[4],
    "snapshot_path": sys.argv[5],
    "snapshot_hash": sys.argv[6],
    "last_success_utc": datetime.now(timezone.utc).isoformat(),
}
state_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
}

# Print OS-specific remediation steps for a missing prerequisite.
# Parameters:
#   $1: Dependency label such as docker, python3.12, node, or npm.
# Output:
#   Writes install or startup guidance to stdout.
print_prereq_help() {
  local dependency="$1"
  case "$OS_FLAVOR" in
    macos)
      case "$dependency" in
        docker)
          cat <<'EOF'
Install Docker Desktop for Mac and make sure it is running:
  https://www.docker.com/products/docker-desktop/
EOF
          ;;
        python3.12)
          cat <<'EOF'
Install Python 3.12 with Homebrew:
  brew install python@3.12
Ensure `python3.12` is on PATH before rerunning bootstrap.
EOF
          ;;
        node)
          cat <<'EOF'
Install Node.js 22.12.0 or newer with Homebrew or nvm:
  brew install node@22
or:
  nvm install 22.12.0 && nvm use 22.12.0
EOF
          ;;
        npm)
          cat <<'EOF'
Install Node.js 22.12.0 or newer; npm is bundled with the Node distribution:
  brew install node@22
EOF
          ;;
      esac
      ;;
    ubuntu)
      case "$dependency" in
        docker)
          cat <<'EOF'
Install Docker Engine and the Compose plugin, then start Docker:
  sudo apt-get update
  sudo apt-get install docker.io docker-compose-plugin
  sudo systemctl enable --now docker
EOF
          ;;
        python3.12)
          cat <<'EOF'
Install Python 3.12 and venv support:
  sudo apt-get update
  sudo apt-get install python3.12 python3.12-venv
EOF
          ;;
        node)
          cat <<'EOF'
Install Node.js 22.12.0 or newer, then rerun bootstrap.
For example with nvm:
  nvm install 22.12.0 && nvm use 22.12.0
EOF
          ;;
        npm)
          cat <<'EOF'
Install Node.js 22.12.0 or newer; npm ships with Node.
EOF
          ;;
      esac
      ;;
    *)
      cat <<EOF
Install ${dependency} and rerun bootstrap.
EOF
      ;;
  esac
}

# Fail fast when a required command is missing from PATH.
# Parameters:
#   $1: Executable name to validate.
# Output:
#   On success, no stdout output.
#   On failure, writes an error plus remediation to stderr and exits non-zero.
require_command() {
  local dependency="$1"
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "Missing required dependency: $dependency" >&2
    print_prereq_help "$dependency" >&2
    exit 1
  fi
}

require_command docker
if ! docker compose version >/dev/null 2>&1; then
  echo "Missing required dependency: docker compose" >&2
  print_prereq_help docker >&2
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "Docker is installed but the daemon is not reachable. Start Docker and rerun bootstrap." >&2
  print_prereq_help docker >&2
  exit 1
fi

require_command python3.12
require_command node
require_command npm

if ! node -e 'const [major, minor] = process.versions.node.split(".").map(Number); process.exit(major > 22 || (major === 22 && minor >= 12) ? 0 : 1);'; then
  echo "Node.js 22.12.0 or newer is required. Found: $(node --version)" >&2
  print_prereq_help node >&2
  exit 1
fi

# Copy an example env file into place only when the destination is missing.
# Parameters:
#   $1: Source template path.
#   $2: Destination path.
# Output:
#   Writes a creation message to stdout when a copy occurs.
ensure_env_file() {
  local source_path="$1"
  local target_path="$2"
  if [[ -f "$target_path" || ! -f "$source_path" ]]; then
    return
  fi
  cp "$source_path" "$target_path"
  echo "Created $target_path from $source_path"
}

ensure_env_file ".env.example" ".env"
ensure_env_file "kalshi-scraper/.env.example" "kalshi-scraper/.env"

CURRENT_REQUIREMENTS_HASH="$(sha256_file requirements.txt)"
PREVIOUS_REQUIREMENTS_HASH="$(state_get python_requirements_hash || true)"
PREVIOUS_NPM_LOCK_HASH="$(state_get npm_lock_hash || true)"
PREVIOUS_SNAPSHOT_MODE="$(state_get snapshot_mode || true)"
PREVIOUS_SNAPSHOT_PATH="$(state_get snapshot_path || true)"
PREVIOUS_SNAPSHOT_HASH="$(state_get snapshot_hash || true)"

if [[ ! -d ".venv" ]]; then
  echo "Creating Python virtual environment..."
  python3.12 -m venv .venv
fi

PYTHON_BIN=".venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing $PYTHON_BIN after virtualenv creation." >&2
  exit 1
fi

if [[ ! -f "$BOOTSTRAP_STATE_FILE" || "$CURRENT_REQUIREMENTS_HASH" != "$PREVIOUS_REQUIREMENTS_HASH" ]]; then
  echo "Installing Python dependencies..."
  "$PYTHON_BIN" -m pip install --upgrade pip
  "$PYTHON_BIN" -m pip install -r requirements.txt
else
  echo "Python dependencies unchanged; skipping reinstall."
fi

CURRENT_NPM_LOCK_HASH=""
if [[ "$SKIP_FRONTEND" == false ]]; then
  CURRENT_NPM_LOCK_HASH="$(sha256_file my-app/package-lock.json)"
  if [[ ! -d "my-app/node_modules" || ! -f "$BOOTSTRAP_STATE_FILE" || "$CURRENT_NPM_LOCK_HASH" != "$PREVIOUS_NPM_LOCK_HASH" ]]; then
    echo "Installing frontend dependencies..."
    npm ci --prefix my-app
  else
    echo "Frontend dependencies unchanged; skipping npm ci."
  fi
else
  echo "Skipping frontend dependency installation."
fi

CURRENT_SNAPSHOT_MODE="none"
CURRENT_SNAPSHOT_PATH=""
CURRENT_SNAPSHOT_HASH=""
if [[ "$EMPTY_DB" == true ]]; then
  CURRENT_SNAPSHOT_MODE="empty"
elif [[ -n "$SNAPSHOT_PATH" ]]; then
  CURRENT_SNAPSHOT_MODE="snapshot"
  CURRENT_SNAPSHOT_PATH="$(resolve_abs_path "$SNAPSHOT_PATH")"
  if [[ ! -f "$CURRENT_SNAPSHOT_PATH" ]]; then
    echo "Snapshot not found: $CURRENT_SNAPSHOT_PATH" >&2
    exit 1
  fi
  CURRENT_SNAPSHOT_HASH="$(sha256_file "$CURRENT_SNAPSHOT_PATH")"
elif DEFAULT_SNAPSHOT_PATH="$(resolve_default_snapshot)"; then
  CURRENT_SNAPSHOT_MODE="snapshot"
  CURRENT_SNAPSHOT_PATH="$DEFAULT_SNAPSHOT_PATH"
  CURRENT_SNAPSHOT_HASH="$(sha256_file "$CURRENT_SNAPSHOT_PATH")"
fi

DB_ARGS=()
REQUEST_DB_RESET="$RESET_DB"
RESET_REASON=""

if [[ "$EMPTY_DB" == true ]]; then
  REQUEST_DB_RESET=true
  RESET_REASON="empty DB requested"
fi

if [[ "$CURRENT_SNAPSHOT_MODE" == "snapshot" ]]; then
  DB_ARGS+=(--snapshot "$CURRENT_SNAPSHOT_PATH")
  if [[ -n "$SNAPSHOT_PATH" && ( -z "$PREVIOUS_SNAPSHOT_HASH" || "$CURRENT_SNAPSHOT_HASH" != "$PREVIOUS_SNAPSHOT_HASH" || "$CURRENT_SNAPSHOT_PATH" != "$PREVIOUS_SNAPSHOT_PATH" ) ]]; then
    REQUEST_DB_RESET=true
    RESET_REASON="explicit snapshot override requested"
  elif [[ -n "$PREVIOUS_SNAPSHOT_HASH" && "$CURRENT_SNAPSHOT_HASH" != "$PREVIOUS_SNAPSHOT_HASH" ]]; then
    REQUEST_DB_RESET=true
    RESET_REASON="tracked snapshot changed"
  elif [[ -n "$PREVIOUS_SNAPSHOT_PATH" && "$CURRENT_SNAPSHOT_PATH" != "$PREVIOUS_SNAPSHOT_PATH" ]]; then
    REQUEST_DB_RESET=true
    RESET_REASON="tracked snapshot path changed"
  elif [[ "$PREVIOUS_SNAPSHOT_MODE" == "empty" ]]; then
    REQUEST_DB_RESET=true
    RESET_REASON="switching from empty DB mode to snapshot mode"
  fi
elif [[ "$CURRENT_SNAPSHOT_MODE" == "empty" && "$PREVIOUS_SNAPSHOT_MODE" == "snapshot" ]]; then
  REQUEST_DB_RESET=true
  RESET_REASON="switching from snapshot mode to empty DB mode"
fi

if [[ "$REQUEST_DB_RESET" == true ]]; then
  DB_ARGS+=(--reset-db)
  if [[ -n "$RESET_REASON" ]]; then
    echo "Refreshing database state: $RESET_REASON."
  fi
fi

if [[ "$SKIP_VERIFY" == true ]]; then
  DB_ARGS+=(--skip-validate)
fi

echo "Bootstrapping Docker database..."
if [[ ${#DB_ARGS[@]} -gt 0 ]]; then
  "$PYTHON_BIN" scripts/setup_collab_db.py "${DB_ARGS[@]}"
else
  "$PYTHON_BIN" scripts/setup_collab_db.py
fi

if [[ "$SKIP_VERIFY" == false && "$SKIP_FRONTEND" == false ]]; then
  echo "Running frontend production build..."
  npm --prefix my-app run build
fi

NEXT_NPM_LOCK_HASH="$PREVIOUS_NPM_LOCK_HASH"
if [[ "$SKIP_FRONTEND" == false ]]; then
  NEXT_NPM_LOCK_HASH="$CURRENT_NPM_LOCK_HASH"
fi
write_state "$CURRENT_REQUIREMENTS_HASH" "$NEXT_NPM_LOCK_HASH" "$CURRENT_SNAPSHOT_MODE" "$CURRENT_SNAPSHOT_PATH" "$CURRENT_SNAPSHOT_HASH"

echo
echo "Bootstrap complete."
echo "Backend:"
echo "  export DATABASE_URL=\"${DATABASE_URL:-$DATABASE_URL_DEFAULT}\""
echo "  export PSQL_URL=\"${PSQL_URL:-$PSQL_URL_DEFAULT}\""
echo "  $PYTHON_BIN -m uvicorn data_platform.api.server:app --reload --host 127.0.0.1 --port 8000"
if [[ "$SKIP_FRONTEND" == false ]]; then
  echo "Frontend:"
  echo "  npm --prefix my-app run dev"
fi
