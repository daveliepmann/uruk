#!/bin/bash

# Command that generates the HTML.
export AUTODOC_CMD="lein with-profile +codox codox"

# The directory where the result of $AUTODOC_CMD, the generated HTML, ends up. This
# is what gets committed to $AUTODOC_BRANCH.
export AUTODOC_DIR="gh-pages"

# You can optionally keep multiple versions of your documentation in a
# subdirectory of AUTODOC_DIR.  If you set this variable then the
# generated HTML is expected to go into this subdirectory, and the contents
# of any other subdirectories are preserved.
# export AUTODOC_SUBDIR="v0.1.0"

# The git remote to fetch from and push to.
export AUTODOC_REMOTE="origin"

# Branch name to commit and push to
export AUTODOC_BRANCH="gh-pages"

./autodoc.sh
