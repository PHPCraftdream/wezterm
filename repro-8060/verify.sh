#!/usr/bin/env bash
#
# Runs the whole before/after matrix for the three defects in PR #8060 and
# prints a verdict.
#
#   bash repro-8060/verify.sh
#
# Expects the working tree in the "before" state (see README.md) on branch
# repro-8060. The script applies and reverts the patch itself, so it leaves
# the tree exactly where it found it -- including if you interrupt it.
#
# Two of these tests are supposed to hang, so they have to be killed. They are
# run as the built test binary rather than through `cargo test`: killing cargo
# mid-run can leave the build directory locked, and the next cargo invocation
# then blocks on that lock rather than on anything to do with the test.

cd "$(dirname "$0")/.." || exit 1

PATCH=repro-8060/apply-fix.patch
SRC=promise/src/spawn.rs

RED=$'\033[31m'; GREEN=$'\033[32m'; DIM=$'\033[2m'; OFF=$'\033[0m'

fail() { printf '%sABORT:%s %s\n' "$RED" "$OFF" "$*" >&2; exit 1; }

# --- preconditions ---------------------------------------------------------

[ -f "$SRC" ]   || fail "no $SRC -- run this from the wezterm repository root"
[ -f "$PATCH" ] || fail "no $PATCH"

grep -q 'tx.send(res).unwrap();' "$SRC" \
  || fail "$SRC is not in the BEFORE state. Revert it: git apply -R $PATCH"

git apply --check "$PATCH" \
  || fail "$PATCH does not apply cleanly to the current $SRC"

grep -q 'mod repro {' "$SRC" \
  || fail "$SRC has no test scaffolding -- wrong branch?"

printf '%sbranch:%s %s   %sbase:%s %s\n\n' \
  "$DIM" "$OFF" "$(git branch --show-current)" \
  "$DIM" "$OFF" "$(git log --oneline -1 --format=%h\ %s)"

# --- building --------------------------------------------------------------

BIN=""

build() {
  local out
  out=$(env -u CARGO_INCREMENTAL cargo test -p promise --lib --no-run 2>&1) \
    || { printf '%s\n' "$out" >&2; fail "the tests did not build"; }
  BIN=$(printf '%s\n' "$out" \
        | sed -n 's/.*Executable unittests[^(]*(\(.*\))/\1/p' | tail -1)
  [ -n "$BIN" ] || fail "could not work out where the test binary was written"
  [ -x "$BIN" ] || fail "test binary not executable: $BIN"
}

# --- running a single test -------------------------------------------------
# sets RESULT to PASS | FAIL | HANG, and EVIDENCE to a telling output line

RESULT=""
EVIDENCE=""

run_one() {
  local name=$1 secs=$2 out rc
  out=$(timeout "$secs" "$BIN" --exact "spawn::tests::$name" 2>&1)
  rc=$?
  case $rc in
    0)   RESULT=PASS ; EVIDENCE=$(printf '%s\n' "$out" | grep -m1 'test result:') ;;
    124) RESULT=HANG ; EVIDENCE="killed after ${secs}s" ;;
    *)   RESULT=FAIL ; EVIDENCE=$(printf '%s\n' "$out" \
           | grep -m1 -E "unwrap\(\)|assertion" ) ;;
  esac
  [ -n "$EVIDENCE" ] || EVIDENCE="(rc=$rc)"
}

check() {   # test name, expected outcome, timeout, label
  local name=$1 expect=$2 secs=$3 label=$4
  printf '  %-40s ' "$label"
  run_one "$name" "$secs"
  if [ "$RESULT" = "$expect" ]; then
    printf '%s%-4s%s  %sas expected%s  %s%s%s\n' \
      "$GREEN" "$RESULT" "$OFF" "$DIM" "$OFF" "$DIM" "$EVIDENCE" "$OFF"
  else
    printf '%s%-4s%s  %sEXPECTED %s%s  %s\n' \
      "$RED" "$RESULT" "$OFF" "$RED" "$expect" "$OFF" "$EVIDENCE"
    BAD=1
  fi
}

BAD=0

# --- BEFORE ----------------------------------------------------------------

printf '%sbuilding tests...%s\n' "$DIM" "$OFF"
build

printf '\nBEFORE -- upstream code as it stands (%s)\n' "$(git log --oneline -1 --format=%h)"
check happy_path_returns_the_closures_result           PASS 60 'control: happy path'
check a_result_sent_inside_the_poll_window_is_not_lost HANG 30 'defect (2): lost wakeup'
check a_cancelled_consumer_does_not_panic_the_worker   FAIL 60 'defect (1): worker panic'
check a_panicking_worker_reports_an_error_instead_of_hanging HANG 30 'defect (3): panic in f()'

# --- AFTER -----------------------------------------------------------------

git apply "$PATCH" || fail "could not apply the patch"
touch "$SRC"
trap 'git apply -R "$PATCH" 2>/dev/null; touch "$SRC"' EXIT

printf '\nAFTER -- same file plus the production fix from PR #8060\n'
printf '%sbuilding...%s\n' "$DIM" "$OFF"
build

check happy_path_returns_the_closures_result           PASS 60 'control: happy path'
check a_result_sent_inside_the_poll_window_is_not_lost PASS 60 'defect (2): lost wakeup'
check a_cancelled_consumer_does_not_panic_the_worker   PASS 60 'defect (1): worker panic'
check a_panicking_worker_reports_an_error_instead_of_hanging PASS 60 'defect (3): panic in f()'

# --- revert ----------------------------------------------------------------

trap - EXIT
git apply -R "$PATCH" || fail "could not revert the patch -- fix this by hand"
touch "$SRC"
grep -q 'tx.send(res).unwrap();' "$SRC" || fail "the revert did not restore the BEFORE state"

printf '\ntree restored to the BEFORE state\n'

if [ "$BAD" = 0 ]; then
  printf '\n%sALL EIGHT OUTCOMES MATCHED%s\n' "$GREEN" "$OFF"
  exit 0
else
  printf '\n%sMISMATCHES -- see the lines above%s\n' "$RED" "$OFF"
  exit 1
fi
