#! /usr/bin/env python3
# Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

# Add copyright headers to source files.
#
# To check that all files have required headers:
# $ copyright-headers --action=check
#
# To add/update the copyright headers:
# $ copyright-headers --action=update

import argparse
import datetime
import logging
import json

import os
from os.path import join
from string import Template

import subprocess
import sys

import re


args = None
def setup_cli_args(cli_args):
  arg_parser = argparse.ArgumentParser(description='Copyright header tool')
  arg_parser.add_argument('--action',
      dest='action',
      type=str,
      choices=['check', 'update', 'remove'],
      help='The action to take - check, update or remove copyright headers',
      default='check'
    )
  arg_parser.add_argument('--folder',
      dest='folder',
      type=str,
      help='The folder containing the files to check/update the copyright header'
    )
  arg_parser.add_argument('--files',
      nargs='+',
      default=None,
      dest='files',
      help='The list of files to check/update the copyright header'
    )
  arg_parser.add_argument('--config-file',
      dest='config_file',
      type=str,
      help='Path to the configuration for the Copyright Header',
      default='./.scaffold/libs/python/copyright-headers.json'
    )
  arg_parser.add_argument('--force-update',
      default=False,
      dest="force_update",
      help="Force the update to overwrite the copyright in all matching files",
      action='store_true'
    )
  arg_parser.add_argument('--log-level',
      dest='log_level',
      type=str,
      choices=['debug', 'info', 'warn', 'error'],
      help='The logging level for the application',
      default='info'
    )
  arg_parser.add_argument('--log-file',
      dest='log_file',
      type=str,
      help='The file to save all output from the application'
    )

  return arg_parser.parse_args(cli_args)



## Set logging Level
# Configure the logging for this application, this includes setting the level
# and the output file
# Output format will be {levelname}: {message}
def set_logging_level():
  level = getattr(logging, args.log_level.upper(), None)
  if not isinstance(level, int):
    raise ValueError(f'Invalid log level "{args.log_level}"')

  if args.log_file is not None:
    logging.basicConfig(level=level, filemode='w', filename=args.log_file, format='%(levelname)s: %(message)s')
  else:
    logging.basicConfig(level=level, filemode='w', format='%(levelname)s: %(message)s')

#########################
# File-type configuration
# line: The prefix for the notice text.
# skip: Optional prefix for lines to skip when adding header initially.

filetypes = \
  { 'bat'        : { 'line' : '::' } ,
    'bats'       : { 'line' : '#'  , 'skip': '#!' } ,
    'bazel'      : { 'line' : '#'  , 'skip': '#!' } ,
    'bzl'        : { 'line' : '#'  , 'skip': '#!' } ,
    'c'          : { 'line' : '//' } ,
    'cc'         : { 'line' : '//' } ,
    'chs'        : { 'line' : '--' } ,
    'cmd'        : { 'line' : '::' } ,
    'cpp'        : { 'line' : '//' } ,
    'css'        : { 'line' : ''   } ,
    'daml'       : { 'line' : '--' } ,
    'groovy'     : { 'line' : '//' } ,
    'h'          : { 'line' : '//' } ,
    'hh'         : { 'line' : '//' } ,
    'hpp'        : { 'line' : '//' } ,
    'hs'         : { 'line' : '--' , 'skip' : '#!'  } ,
    'java'       : { 'line' : '//' } ,
    'js'         : { 'line' : '//' } ,
    'kt'         : { 'line' : '//' } ,
    'libsonnet'  : { 'line' : '//' } ,
    'lf'         : { 'line' : '//' } ,
    'lhs'        : { 'line' : '--' } ,
    'proto'      : { 'line' : '//' } ,
    'py'         : { 'line' : '#'  , 'skip' : '#!' } ,
    'rb'         : { 'line' : '#'  } ,
    'rst'        : { 'line' : '..' } ,
    'sc'         : { 'line' : '//' } ,
    'scala'      : { 'line' : '//' } ,
    'scss'       : { 'line' : ''   } ,
    'sh'         : { 'line' : '#'  , 'skip' : '#!' } ,
    # Files with no extension are usually shell scripts
    ''           : { 'line' : '#'  , 'skip' : '#!' } ,
    'sql'        : { 'line' : '--' } ,
    'tex'        : { 'line' : '%'  } ,
    'tf'         : { 'line' : '#'  } ,
    'ts'         : { 'line' : '//' } ,
    'tsx'        : { 'line' : '//' } ,
    'yaml'       : { 'line' : '#'  } ,
    'yml'        : { 'line' : '#'  } ,
    'Makefile'   : { 'line' : '#'  } ,
    'Dockerfile' : { 'line' : '#'  } ,
    'mk'         : { 'line' : '#'  } ,
  }

#########################

def has_attr(object, prop):
  if object.get(prop, None) == None:
    return False
  else:
    return True


## Get Full Path
# Take the path and replce the shorthands to create a full path
# Returns: String - the full path
def get_full_path(path):
  if path.startswith("./"):
    path = path.replace("./",f"{os.getcwd()}/", 1)

  if not path.startswith("/"):
    path = join(os.getcwd(), path)

  if path.endswith("*"):
    path = path[:-1]

  return path

## Read File Contents
# Read the file contents
# Returns: dict - the rules in the file
def read_file_contents(path, file = None):
  path = get_full_path(path)
  f = None
  contents = None
  try:
    if file is not None:
      path = join(path, file)

    f = open(path, "r")
    contents = f.read()
  finally:
    f.close()
  return contents


## Clean Regex
# take the text and prepare it for regex matching
# parse the regex to be compliant, following rules in order are:
# fix brackets
# fix unescaped forward slash
# fix unescaped .
def clean_regex(text):
  return text.replace(r"(", r"\(") \
              .replace(r")", r"\)") \
              .replace(r".", r"\.") \
              .replace(r"/", r"\/")

## Clean Regex
# take the text and prepare it for regex compiling
# parse the regex to be compliant, following rules in order are:
# fix groking
# fix unescaped forward slash
# fix unescaped .
# * should match everything until this point
def parse_regex(text):
  return text.replace(r"**/", r"*/") \
                .replace(r"/", r"\/") \
                .replace(r".", r"\.") \
                .replace(r"*", r".*")

## Combine Regexs
# Parse the individual regex options
# then combine them into a large meaningful regex
# Returns: String - the uber regex
def combine_regexs(regexs):
  output_re = ""
  if regexs is None or len(regexs) == 0:
    regexs = ["*"]

  for regex in regexs:
    regex = parse_regex(regex)

    if len(output_re) > 0 :
      output_re = output_re + "|"
    output_re = output_re + regex

  return output_re


## Create Header
# using the text template and the file extension create the
# comment aware copyright header
def create_header(text, file_ext):
  logging.debug("Create Header")
  format = filetypes[file_ext]

  today = datetime.date.today()
  template = Template(text).substitute(year=today.year)

  lines = [f"{format['line']} {line}" for line in template.splitlines()]
  header = "\n".join(lines) + "\n"

  logging.debug(f"copyright header: {header}")
  return header


# Switch the current working directory to the root of the repository.
def chdir_to_toplevel():
  toplevel = subprocess                                    \
    .check_output(["git", "rev-parse", "--show-toplevel"], universal_newlines=True) \
    .strip()
  os.chdir(toplevel)


## Filter files
# will filter the files based on the ignores list
def filter_files(files, ignores):

  logging.debug(f"ignore paths: {ignores}")

  # process ignores list to turn into regex's
  output_re = combine_regexs(ignores)
  compiled_re = re.compile(f"^{output_re}$")

  # Filter out files or folders from the ignores list
  unignored = list(filter(lambda item: (True if compiled_re.match(item) is None else False), files))
  logging.debug(f"Unignored files list: [{unignored}]")

  # Filter out symlinks
  return [ f for f in unignored if not os.path.islink(f) ]


## List Files
# Find all files from the provided directory (defaults to '.')
# using git ls-files, ignoring all prefixes specified in the config file
def list_files(dir, ignores):
  logging.debug("list_files")
  if dir is None:
    dir = "."

  all_files = subprocess                       \
    .check_output(["git", "ls-files", dir], universal_newlines=True)    \
    .strip()                                   \
    .split("\n")

  return filter_files(all_files, ignores)


## Build Header Regex
# Build a compiled regex matching the copyright header block for the given
# comment style.
def build_header_regex(header_template, format):
  block = [f"{format['line']} {line}" for line in header_template.split('\n')]
  pattern_lines = [Template(clean_regex(line)).substitute(year=r"\d{4}") for line in block]

  pattern = f"^{pattern_lines[0]}$"
  for line in pattern_lines[1:]:
    pattern += f"\\n^{line}$"

  return re.compile(pattern, re.MULTILINE)


## Get File Contents
# return either the file content without the header, or only the header
# boolean flag for if the header was found
# the entire header block is matched against the header_template in one go
def get_file_contents(contents, header_template, format, header_only = False):
  header_re = build_header_regex(header_template, format)

  stripped = [line.rstrip() for line in contents]

  # skip lines (e.g. a shebang) are allowed before the header, so match after them
  skip = format.get('skip', None)
  start = 0
  while skip is not None and start < len(stripped) and stripped[start].startswith(skip):
    start += 1

  match = header_re.match("\n".join(stripped[start:]))

  if match is None:
    return False, [] if header_only else stripped

  header = match.group(0).split("\n")
  if header_only:
    return True, header

  # drop the matched header lines, keeping the rest of the file intact
  return True, stripped[:start] + stripped[start + len(header):]


## Check Header
# construct a new regex template of the header and confirm that the header in the
# provided file exists and matches the regex
def check_header(config, contents, file_ext):
  logging.debug("Checking header")
  format = filetypes[file_ext]

  # if config limits the number of lines to check, then use only those lines
  if has_attr(config, "check-lines"):
    contents = contents[:config["check-lines"]]

  header_found, curr_header = get_file_contents(contents, config["header-text"], format, True)

  header_re = build_header_regex(config["header-text"], format)
  matches = header_re.match("\n".join(curr_header)) is not None

  return header_found, matches


## Remove Header
# remove the header from the file contents if a header is found
def remove_header(contents, header_template, format):
  logging.debug(f"Removing header")
  # iterate down the top few lines to remove the header
  header_found, output = get_file_contents(contents, header_template, format)

  # return the starting dex of the header and the file contents without any header in there
  return header_found, output


## Insert Header
# Insert the header at the required location into the file contents
def insert_header(contents, start_index, header, format):
  logging.debug(f"Inserting header at line {start_index}")
  output = []
  skip = format.get('skip', None)
  for idx, line in enumerate(contents):
    if skip is not None and line.startswith(skip):
      logging.debug("The skip is detected, moving insert to the new line")
      start_index = start_index + 1

    if idx == start_index:
      output = output + [header]
      if line.strip() != '':
        # ensure we have a nice clean line after the header
        output = output + ["\n"]

    output = output + [line]

  return output

## Remove
# remove the header from the provided file
def remove(config, file_path, file_ext):
  logging.debug(f"Processing {file_path} for header removal")

  # never modify files with a grandfathered legacy header
  if legacy_header_text(config, file_path) is not None:
    logging.info(f"Skipping legacy header file (not modified): {file_path}")
    return

  with open(file_path, 'r+', encoding="UTF-8") as fp:
    content = fp.readlines()
    format = filetypes[file_ext]
    header_found, contents = remove_header(content, config["header-text"], format)
  if header_found == True:
    logging.info(f"Removing header from file {file_path}")
    with open(file_path, 'w', encoding="UTF-8") as fp:
      # restore the trailing newline stripped by get_file_contents on each line
      fp.write("".join(f"{line}\n" for line in contents))


## Update
# will check if the file has a valid header.
# If no header is found or is not valid then a new
# header will be inserted into the file
def update(config, file_path, file_ext):
  logging.debug(f"updating file {file_path}")

  # never modify files with a grandfathered legacy header (e.g. Flyway migrations,
  # whose headers are locked by checksum validation - see PR #1033)
  if legacy_header_text(config, file_path) is not None:
    logging.info(f"Skipping legacy header file (not updated): {file_path}")
    return

  header = create_header(config["header-text"], file_ext)

  with open(file_path, 'r+', encoding="UTF-8") as fp:
    content = fp.readlines()
    start_index = 0
    insert_required = False
    found, valid = check_header(config, content, file_ext)

    format = filetypes[file_ext]

  if found == True:
    if valid == False or args.force_update == True:
      _, content = remove_header(content, config["header-text"], format)
      # get_file_contents returns stripped lines, restore newlines for re-insertion
      content = [f"{line}\n" for line in content]
      insert_required = True

  if found == False:
    insert_required = True

  if insert_required == True:
    content = insert_header(content, start_index, header, format)
    with open(file_path, 'w', encoding="UTF-8") as fp:
      fp.write("".join(content))
    logging.info(f"Copyright header updated in file {file_path}")


## Legacy Header Text
# Some existing files carry a legacy copyright notice that cannot be brought up to
# the standard header - e.g. Flyway migrations, whose headers are locked by checksum
# validation (see PR #1033). For those explicitly enumerated files the alternate
# 'header-text' from the config is accepted in addition to the standard one. New
# files are never listed, so they are always held to the standard header.
# Returns: str|None - the legacy header template for this file, or None
def legacy_header_text(config, file_path):
  for rule in config.get("legacy-headers", []):
    if file_path in rule.get("files", []):
      return rule["header-text"]
  return None


## Has Header
# confirm that the provided header template is present in the file contents
def has_header(config, contents, file_ext, header_text):
  format = filetypes[file_ext]

  # if config limits the number of lines to check, then use only those lines
  if has_attr(config, "check-lines"):
    contents = contents[:config["check-lines"]]

  found, _ = get_file_contents(contents, header_text, format, True)
  return found


## Check
# check that the file has a copyright header and its valid
def check(config, file_path, file_ext):
  logging.debug(f"Checking file {file_path}")
  with open(file_path, 'r', encoding="UTF-8") as fp:
    content = fp.readlines()

  found, valid = check_header(config, content, file_ext)

  if found == True and valid == True:
    return True

  # existing files with a grandfathered legacy header (e.g. Flyway migrations) are
  # allowed the alternate notice enumerated in the config
  legacy = legacy_header_text(config, file_path)
  if legacy is not None and has_header(config, content, file_ext, legacy):
    return True

  logging.info(F" file '{file_path}', header found: {found}, is valid: {valid}")
  return False

#########################
# Main

if __name__ == "__main__":
  exit_code = 0
  args = setup_cli_args(sys.argv[1:])
  set_logging_level()

  try:
    config = json.loads(read_file_contents(args.config_file))

    chdir_to_toplevel()
    files = args.files
    if files is None:
      files = list_files(args.folder, config["excludes"])
    else:
      files = filter_files(files, config["excludes"])

    msg = ""
    for file in files:
      filename = os.path.basename(file)
      if filename == 'Makefile' or filename.startswith('Dockerfile'):
        ext = filename
      else:
        (_, ext) = os.path.splitext(file)
        ext = ext[1:]

      if ext in filetypes:
        if args.action == "update":
          update(config, file, ext)
          msg = "Update headers complete"
        elif args.action == "check":
          if check(config, file, ext) == False:
            exit_code = 1
            msg = "Failed copyright check, run 'copyright-headers.py --action=update --force' to ensure the files are compliant"
        elif args.action == "remove":
          remove(config, file, ext)
      else:
        logging.info(f"Unsupported file type '{ext}', skipping file {file}")

    if len(msg) > 0:
      # make sure to print to the command line
      if args.log_file is not None:
        print(msg)
      logging.info(msg)


  except Exception as e:
    print(f"Error: {e}")
    logging.error('Unable to check or update the copyright headers', e)
    exit_code = 1
  finally:
    # close the logging and flush the output
    logging.shutdown()
    sys.exit(exit_code)
