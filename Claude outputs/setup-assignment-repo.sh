#!/usr/bin/env bash
# ============================================================================
# DSA Assignment 2 — Repo Setup Script
# Run this from inside your cloned assignment repository.
# ============================================================================
set -e

BRANCH_NAME="dev-1-tapesh"

echo "==> Step 1: Adding GitHub Actions workflow..."
mkdir -p .github/workflows

# Write the MainDistributionPipeline workflow
cat > .github/workflows/MainDistributionPipeline.yml << 'WORKFLOW'
name: Main Extension Distribution Pipeline

# Only runs on manual trigger or merge to main — NOT on every branch push.
# ~2,000 free CI minutes/month; each full matrix build uses ~30–40 min.
on:
  push:
    branches:
      - main
  workflow_dispatch:
    inputs:
      override_git_describe:
        type: string
        description: "Git describe override (leave blank to auto-detect)"
        required: false
        default: ""

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}-${{ github.head_ref || '' }}-${{ github.base_ref || '' }}-${{ github.ref != 'refs/heads/main' || github.sha }}
  cancel-in-progress: true

env:
  GH_TOKEN: ${{ secrets.GH_TOKEN }}
  DUCKDB_VERSION: "v1.5.5"

jobs:
  linux-x64:
    name: Linux x86-64
    runs-on: ubuntu-24.04
    container: ubuntu:22.04
    env:
      VCPKG_TARGET_TRIPLET: x64-linux
      GEN: Ninja
      DUCKDB_PLATFORM: linux_amd64
    steps:
      - name: Install dependencies
        run: |
          apt-get update -y
          apt-get install -y git curl zip unzip cmake ninja-build python3 python3-pip \
                             build-essential libssl-dev pkg-config
          pip3 install --break-system-packages httpx==0.26.0
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
          submodules: "recursive"
      - name: Setup vcpkg
        run: |
          cd /usr/local
          curl -L https://github.com/microsoft/vcpkg/archive/refs/tags/2024.08.23.tar.gz | tar xz
          mv vcpkg-2024.08.23 vcpkg
          /usr/local/vcpkg/bootstrap-vcpkg.sh -disableMetrics
      - name: Build extension
        env:
          VCPKG_ROOT: /usr/local/vcpkg
        run: make release
      - name: Test extension
        run: make test_release
      - name: Upload artifacts
        uses: actions/upload-artifact@v4
        with:
          name: linux-x64-extension
          path: build/release/**/*.duckdb_extension

  macos-arm64:
    name: macOS Apple Silicon
    runs-on: macos-14
    env:
      VCPKG_TARGET_TRIPLET: arm64-osx
      GEN: Ninja
      DUCKDB_PLATFORM: osx_arm64
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
          submodules: "recursive"
      - name: Install dependencies
        run: brew install ninja cmake
      - name: Setup vcpkg
        run: |
          git clone https://github.com/microsoft/vcpkg /usr/local/vcpkg || true
          /usr/local/vcpkg/bootstrap-vcpkg.sh -disableMetrics
      - name: Build extension
        env:
          VCPKG_ROOT: /usr/local/vcpkg
        run: make release
      - name: Test extension
        run: make test_release
      - name: Upload artifacts
        uses: actions/upload-artifact@v4
        with:
          name: macos-arm64-extension
          path: build/release/**/*.duckdb_extension

  windows-x64:
    name: Windows x64
    runs-on: windows-2022
    env:
      VCPKG_TARGET_TRIPLET: x64-windows-static
      DUCKDB_PLATFORM: windows_amd64
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
          submodules: "recursive"
      - name: Setup vcpkg
        uses: lukka/run-vcpkg@v11
        with:
          vcpkgGitCommitId: "a1a1cbc975abf909a6c8985a6a2b8fe20bbd9bd6"
      - name: Build extension
        run: make release
        shell: bash
      - name: Test extension
        run: make test_release
        shell: bash
      - name: Upload artifacts
        uses: actions/upload-artifact@v4
        with:
          name: windows-x64-extension
          path: build/release/**/*.duckdb_extension
WORKFLOW

echo "   ✓ .github/workflows/MainDistributionPipeline.yml written"

echo ""
echo "==> Step 2: Creating and pushing dev branch '${BRANCH_NAME}'..."

# Make sure we're on main first
git checkout main 2>/dev/null || git checkout -b main

# Create the dev branch
git checkout -b "${BRANCH_NAME}"

# Stage the workflow file
git add .github/workflows/MainDistributionPipeline.yml

git commit -m "ci: add GitHub Actions workflow (manual + main-only trigger)

- Adds MainDistributionPipeline.yml
- Triggers only on workflow_dispatch and push to main
- Builds for Linux x86-64, macOS ARM64, Windows x64
- Conserves CI minutes (~2,000/month free limit)"

# Push the branch to origin
git push -u origin "${BRANCH_NAME}"

echo ""
echo "==> Done!"
echo ""
echo "   Branch '${BRANCH_NAME}' is now on GitHub."
echo "   GitHub Actions will only run when:"
echo "     • You push/merge to main"
echo "     • You manually trigger it from Actions → MainDistributionPipeline → Run workflow"
echo ""
echo "   Next: open a PR from '${BRANCH_NAME}' → main when you're ready to merge."
