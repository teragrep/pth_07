#!/usr/bin/python3

# Teragrep DPL Spark Integration PTH-07
# Copyright (C) 2022  Suomen Kanuuna Oy
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://github.com/teragrep/teragrep/blob/main/LICENSE>.
#
#
# Additional permission under GNU Affero General Public License version 3
# section 7
#
# If you modify this Program, or any covered work, by linking or combining it
# with other code, such other code is not for that reason alone subject to any
# of the requirements of the GNU Affero GPL version 3 as long as this Program
# is the same Program as licensed from Suomen Kanuuna Oy without any additional
# modifications.
#
# Supplemented terms under GNU Affero General Public License version 3
# section 7
#
# Origin of the software must be attributed to Suomen Kanuuna Oy. Any modified
# versions must be marked as "Modified version of" The Program.
#
# Names of the licensors and authors may not be used for publicity purposes.
#
# No rights are granted for use of trade names, trademarks, or service marks
# which are in The Program if any.
#
# Licensee must indemnify licensors and authors for any liability that these
# contractual assumptions impose on licensors and authors.
#
# To the extent this program is licensed as part of the Commercial versions of
# Teragrep, the applicable Commercial License may apply to this file if you as
# a licensee so wish it.

import json
import os
import sys
import shutil
import time

interpreter_settings_file = os.environ.get("INTERPRETER_SETTINGS_FILE", "/opt/teragrep/zep_01/interpreter/spark/interpreter-setting.json")
interpreter_file = os.environ.get("INTERPRETER_FILE", "/opt/teragrep/zep_01/conf/interpreter.json")
patch_file = os.environ.get("PATCH_FILE", "/opt/teragrep/pth_07/share/inject_config.json")

# Check if patch and interpreter-settings exist
for filename in [interpreter_settings_file, patch_file]:
    if not os.path.isfile(filename):
        print(f"Can't find {filename}, failing.")
        sys.exit(1)

# Read interpreter-settings
try:
    config = json.load(open(interpreter_settings_file, "r"))
except Exception as e:
    print(f"Can't read {interpreter_settings_file}: {e}")
    sys.exit(1)

# Read patch data
try:
    patch = json.load(open(patch_file, "r"))
except Exception as e:
    print(f"Can't read {patch_file}: {e}")
    sys.exit(1)

# Check if spark/dpl settings exist - remove the old one if it exists
for index, interpreter in enumerate(config):
    if interpreter["group"] == "spark" and interpreter["name"] == "dpl":
        print("Removing old settings-interpreter configuration")
        del(config[index])

# Inject
print("Injecting settings-interpreter configuration")
config.append(patch)
with open(interpreter_settings_file, "w") as fh:
    fh.write(json.dumps(config, indent=2))

# Patch interpreter.json if any
if os.path.isfile(interpreter_file):
    try:
        interpreter = json.load(open(interpreter_file, "r"))
    except Exception as e:
        print(f"Cna't read {interpreter_file}: {e}")
        sys.exit(1)
    # Check if configs should be patched and flag for rewrite if we do
    rewrite = False
    for key in patch["properties"]:
        if key not in interpreter["interpreterSettings"]["spark"]["properties"]:
            print(f"Adding {key} to {interpreter_file}")
            rewrite = True
            # interpreter.json has different format
            new_props = { "envName": patch["properties"][key]["envName"], "name": key, "value": patch["properties"][key]["defaultValue"], "type": patch["properties"][key]["type"], "description": patch["properties"][key]["description"] }
            interpreter["interpreterSettings"]["spark"]["properties"][key] = new_props
    if rewrite:
        # We dont care about decimals
        timestamp = int(time.time())
        print(f"Copying {interpreter_file} to {interpreter_file}.{timestamp}")
        shutil.copy(interpreter_file, f"{interpreter_file}.{timestamp}")
        print(f"Patching {interpreter_file}")
        with open(interpreter_file, "w") as fh:
            fh.write(json.dumps(interpreter, indent=2))
