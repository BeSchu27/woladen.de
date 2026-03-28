#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ANDROID_DIR="$ROOT_DIR/android"
OUTPUT_DIR="$ROOT_DIR/output/play-store/android"
TEST_CLASS="de.woladen.android.PlayStoreScreenshotTest"
RUNNER="de.woladen.android.test/androidx.test.runner.AndroidJUnitRunner"

declare -a TARGETS=(
  "emulator-5554:phone-portrait:0"
  "emulator-5556:tablet-landscape:3"
)

mkdir -p "$OUTPUT_DIR"

pushd "$ANDROID_DIR" >/dev/null
./gradlew :app:installDebug :app:installDebugAndroidTest
popd >/dev/null

for entry in "${TARGETS[@]}"; do
  IFS=":" read -r serial profile rotation <<<"$entry"
  target_dir="$OUTPUT_DIR/$profile"

  mkdir -p "$target_dir"

  adb -s "$serial" shell settings put system accelerometer_rotation 0
  adb -s "$serial" shell settings put system user_rotation "$rotation"
  sleep 2
  adb -s "$serial" emu geo fix 13.4050 52.5200
  adb -s "$serial" shell pm clear de.woladen.android >/dev/null || true
  adb -s "$serial" shell am instrument -w -e class "$TEST_CLASS" "$RUNNER"

  for name in 01-list 02-detail 03-map 04-favorites 05-info; do
    adb -s "$serial" pull \
      "/sdcard/Download/play-store-screenshots/$profile/$name.png" "$target_dir/$name.png" >/dev/null
  done
done

echo "Saved Android Play Store screenshots under $OUTPUT_DIR"
