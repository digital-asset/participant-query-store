#!/usr/bin/env python3
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

#######################################################################################
## Usage: --tag v2.10.5
## Validates that the provided tag is a valid release tag and belongs to the correct release branch
#######################################################################################

import argparse
import logging
import re
import subprocess
import sys

args = None

def setup_cli_args(cli_args):
  arg_parser = argparse.ArgumentParser(description='Workflow configuration tooling')
  arg_parser.add_argument('--tag',
      dest='tag',
      type=str,
      required=True,
      help='git tag to check'
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


## Call Git Command
# execute a git command to get details about the commit
# Returns: [String] - the results of the git command
def call_git_command(command):
  logging.debug('git command: {0}'.format(command))
  return subprocess.run(command.split(' '), capture_output=True) \
                  .stdout \
                  .decode('utf-8') \
                  .splitlines()


## Get all remote branches that contain the tag
# Returns: List[String] - the names of the branches without the 'origin/' prefix
def get_tag_branches():
  branches = call_git_command(f'git branch -r --contains {args.tag}')
  return [
    branch.strip().replace('origin/', '', 1)
    for branch in branches
    if branch.strip().startswith('origin/')
  ]

if __name__ == "__main__":
  args = setup_cli_args(sys.argv[1:])
  set_logging_level()

  #tag matches release tag format
  if not re.match(r'^v\d+\.\d+\.\d+(-rc\d+)?$', args.tag):
    logging.error(f'Tag {args.tag} is not a valid release tag')
    sys.exit(1)

  base_version = '.'.join(args.tag.lstrip('v').split('.')[:2])
  expected_release_branch = f'release-line-{base_version}'
  branches = get_tag_branches()

  # check that tag belongs to the correct release branch
  if not any(
    branch == expected_release_branch
    for branch in branches
  ):
    logging.error(f'Tag {args.tag} does not belong to {expected_release_branch}')
    sys.exit(1)

  sys.exit(0)
