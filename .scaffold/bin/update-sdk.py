#!/usr/bin/env python3
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#######################################################################################
## Usage: [--tag CANTON_TAG] [--log-level debug|info|warn|error] [--create-pr]
##
## Updates the Canton and Daml versions by fetching the latest OCI manifests, and
## create a PR.
#######################################################################################

import argparse
import json
import logging
import os
import re
import random
import string
import subprocess
import sys
import urllib.error
import urllib.request 

args = None

def setup_cli_args(cli_args):
  arg_parser = argparse.ArgumentParser(description='Update canton component version')
  arg_parser.add_argument('--tag',
      dest='tag',
      type=str,
      help='A specific canton tag to use.'
    )
  arg_parser.add_argument('--base-branch',
      dest='base_branch',
      type=str,
      help='The base branch to create a PR against.',
      default='main'
    )
  arg_parser.add_argument('--create-pr',
      dest='create_pr',
      action='store_true',
      help='Create a PR with the changes.',
      default=False
    )
  arg_parser.add_argument('--log-level',
      dest='log_level',
      type=str,
      choices=['debug', 'info', 'warn', 'error'],
      help='The logging level for the application',
      default='info'
    )

  return arg_parser.parse_args(cli_args)


## Set logging Level
# Configure the logging level and output format for this application
# Output format will be {levelname}: {message}
def set_logging_level():
  level = getattr(logging, args.log_level.upper(), None)
  if not isinstance(level, int):
    raise ValueError(f'Invalid log level "{args.log_level}"')

  logging.basicConfig(level=level, filemode='w', format='%(levelname)s: %(message)s')


## Run Command
# Execute a shell command, capturing output. Raises on failure.
# Returns: subprocess.CompletedProcess
def run_command(command, env=None, capture=True):
  logging.debug(f'Running: {command}')
  result = subprocess.run(
    command,
    shell=isinstance(command, str), 
    capture_output=capture,
    text=True,
    env=env,
  )
  if result.returncode != 0:
    stderr = result.stderr if result.stderr else ''
    raise RuntimeError(f'Command failed (exit {result.returncode}): {command}\n{stderr}')
  return result



## Fetch OCI Manifest
# Fetch the OCI manifest for the given repo URL using oras
# Parse manifest JSON
# Returns: version number from manifest's annotations
def fetch_version(component, tag):
  repo_url = f'europe-docker.pkg.dev/da-images/public-unstable/components/{component}:{tag}'
  result = run_command(f'oras manifest fetch {repo_url}')
  manifest = json.loads(result.stdout)
  return manifest['annotations']['com.digitalasset.version']


## Write version in apps/mill-build/src/mill-build/package.scala
# Returns: bool - True if the file was actually changed
def write_version(variable, version):
  package_scala = 'apps/mill-build/src/millbuild/package.scala'
  logging.info(f'Writing {variable} version to {package_scala}')

  with open(package_scala, 'r') as f:
    contents = f.read()

  if not re.search(rf'val {variable} = "[^"]+"', contents):
    raise RuntimeError(f'Could not find val {variable} = "..." in {package_scala}')

  updated = re.sub(
    rf'(val {variable} = )"[^"]+"',
    rf'\1"{version}"',
    contents,
  )

  if updated == contents:
    logging.info(f'{variable} is already at {version}')
    return False

  with open(package_scala, 'w') as f:
    f.write(updated)
  return True


## Update versions in apps/compatibility/parameters.csv
# Only updates snapshot versions (matching X.Y.Z-snapshot.*), leaving pinned stable versions untouched.
# Returns: bool - True if the file was actually changed
def update_parameters_csv(canton_version, damlc_version):
  parameters_csv = 'apps/compatibility/parameters.csv'
  logging.info(f'Updating versions in {parameters_csv}')

  snapshot_pattern = re.compile(r'^\d+\.\d+\.\d+-snapshot\.')

  with open(parameters_csv, 'r') as f:
    lines = f.readlines()

  updated_lines = []
  for line in lines:
    # Skip comments and header
    stripped = line.strip()
    if stripped.startswith('#') or stripped.startswith('SCRIBE_') or not stripped:
      updated_lines.append(line)
      continue

    fields = line.rstrip('\n').split(',')
    if len(fields) >= 3:
      # Column 1: SCRIBE_DAMLSDKVERSION
      if snapshot_pattern.match(fields[1]):
        fields[1] = damlc_version
      # Column 2: SCRIBE_CANTONVERSION
      if snapshot_pattern.match(fields[2]):
        fields[2] = canton_version

    updated_lines.append(','.join(fields) + '\n')

  updated_content = ''.join(updated_lines)
  original_content = ''.join(lines)

  if updated_content == original_content:
    logging.info('parameters.csv is already up to date')
    return False

  with open(parameters_csv, 'w') as f:
    f.write(updated_content)
  return True


## Create a PR with the version update
# Commits changes, pushes a branch, and opens a PR via GitHub API
def create_pr(branch, pr_title, base_branch):
  github_token = os.getenv('GITHUB_TOKEN', '')
  if not github_token:
    raise RuntimeError('GITHUB_TOKEN is required to create a PR')

  # Create branch and commit
  subprocess.run(['git', 'branch', '-D', branch], capture_output=True)  # best-effort delete
  run_command(f'git checkout -b {branch}')
  run_command('git add -A')
  run_command([
    'git', '-c', 'user.name=CircleCI', '-c', 'user.email=support@digitalasset.com',
    'commit', '-m', f'{pr_title}',
  ], capture=False)
  run_command(f'git push --force origin {branch}:{branch}')

  # Create PR via GitHub API
  pr_payload = json.dumps({
    'title': pr_title,
    'head': branch,
    'base': base_branch,
  }).encode()

  req = urllib.request.Request(
    'https://api.github.com/repos/DACH-NY/scribe/pulls',
    data=pr_payload,
    headers={
      'Authorization': f'token {github_token}',
      'Content-Type': 'application/json',
      'Accept': 'application/vnd.github.v3+json',
    },
    method='POST',
  )

  try:
    with urllib.request.urlopen(req) as resp:
      pr_data = json.loads(resp.read().decode())
      logging.info(f'PR created: {pr_data["html_url"]}')
  except urllib.error.HTTPError as e:
    error_body = e.read().decode()
    # 422 likely means PR already exists
    if e.code == 422:
      logging.info(f'PR already exists for branch {branch}')
    else:
      raise RuntimeError(f'Failed to create PR: {e.code} {error_body}')


if __name__ == "__main__":
  args = setup_cli_args(sys.argv[1:])
  set_logging_level()

  # Change to the repo root
  script_dir = os.path.dirname(os.path.abspath(__file__))
  os.chdir(os.path.join(script_dir, '..', '..'))

  repo_url = f'europe-docker.pkg.dev/da-images/public-unstable/components/canton-open-source:{args.tag}'
  canton_version = fetch_version('canton-open-source', args.tag)
  logging.info(f'Latest Canton snapshot: {canton_version}')

  damlc_version = fetch_version('damlc', args.tag)
  logging.info(f'Latest damlc snapshot: {damlc_version}')

  canton_updated = write_version('canton', canton_version)
  damlc_updated = write_version('damlc', damlc_version)
  matrix_updated = update_parameters_csv(canton_version, damlc_version)

  # Build PR title
  parts = []
  if canton_updated:
    parts.append(f'Canton to {canton_version}')
  if damlc_updated:
    parts.append(f'Daml to {damlc_version}')
  if not canton_updated and not damlc_updated and matrix_updated:
    parts.append(f'compatibility matrix')

  if not parts:
    logging.info('Already up-to-date, nothing to do.')
    sys.exit(0) 
  
  if args.create_pr:
    suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    branch = f'sdk-update/{args.base_branch}-{args.tag}-{suffix}'
    pr_title = f'Update {" and ".join(parts)}'
    create_pr(branch, pr_title, args.base_branch)