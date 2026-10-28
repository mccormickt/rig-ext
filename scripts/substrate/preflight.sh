#!/usr/bin/env bash
# Run the pinned mount tests without host mounts, devices, or network access.
set -euo pipefail
: "${SUBSTRATE:?Set SUBSTRATE to the clean 6a67f431 checkout}"
test "$(git -C "$SUBSTRATE" rev-parse HEAD)" = 6a67f431e0f2d407eaaa73fa03756fac8a07ffcb
test -z "$(git -C "$SUBSTRATE" status --porcelain)"
BUILD=$(mktemp -d)
NAME=rig-overlay-$(basename "$BUILD")
CONTAINER=
VOLUME=
cleanup() {
  if [[ -n "$CONTAINER" ]]; then docker rm -f "$CONTAINER" >/dev/null; fi
  if [[ -n "$VOLUME" ]]; then docker volume rm "$VOLUME" >/dev/null; fi
  rm -rf "$BUILD"
}
trap cleanup EXIT
(cd "$SUBSTRATE"; CGO_ENABLED=0 go test -mod=vendor -c -o "$BUILD/imagecache.test" ./internal/imagecache)
docker volume create --name "$NAME" --label rig-proof=overlay-preflight >/dev/null
VOLUME=$NAME
CONTAINER=$(docker create --name "$NAME" --network none --cap-add=SYS_ADMIN \
  --tmpfs /ram:rw,exec,size=1g --mount "type=volume,src=$VOLUME,dst=/disk" \
  ubuntu:24.04 sh -ec '
    mount --make-rprivate /
    uname -a
    for storage in /ram /disk; do
      echo "Testing lowerdir+ on $storage"
      TMPDIR=$storage /imagecache.test -test.v -test.run "^TestSetupBundleRootfs_(MountAndUnmount|ManyLayers)$"
    done
  ')
docker cp "$BUILD/imagecache.test" "$CONTAINER":/imagecache.test
docker start -a "$CONTAINER"
docker inspect "$CONTAINER" --format 'exit={{.State.ExitCode}} image={{.Image}}'
