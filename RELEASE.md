# Managing Releases


## Handling patch releases

### When should I create a patch release?
Patch releases are exclusively for bugs and and security fixes. Patch releases can have minor _enhancements_ to 
existing features but really should not affect the API in any noticeable way (unless it is adding an argument). For any
functionality changes or new endpoints, please create a minor or major release.

A patch release should increment the third digit e.g. `0.6.9 -> 0.6.10`. This indicates to users that the release 


### Creating a patch release

When looking through the branches on the main repo, you will notice a series of `release` branches labeled
`release-<major version>.<minor version>`. So if we are currently working on the 0.6 release, the branch should be labeled 
`release-0.6`. Before creating a minor release please check the [milestones](https://github.com/astro-projects/astro/milestones)
page to ensure that all relevent bug fixes have been PRed and merged. 

Once you merge all expected fixes, the next step is to ensure that all fixes have been cherry-picked to the release branch from main (please use `git cherry-pick -x <commit id>` to retain
git commit hashes and messages). Depending on the fix there might be some git conflicts to resolve. If you run into conflicts, please 
resolve said conflicts, run `git add .`, and then `git cherry-pick --continue` to continue the merge.

After cherry-picking all needed fixes, follow the instructions to [create a release from the release branch](#creating-a-release-from-the-release-branch)

## Handling minor releases
### When should I create a minor release?

Minor release bumps should be for adding new features (as well as fixing any bugs or security issues). Notably, the difference between
a minor and major release is that a minor release _should not break existing functionality_. A notable exception to this is that while
Astro is in alpha we will allow breaking minor releases, however any release 1.0+ should only break functionality at major releases.

It is, however, ok to put deprecation warnings on existing features in minor releases to alert users that a feature will go away in a future
major release.

### Creating a minor release

To create a minor release, first create a new release branch based on main under the new minor release number. 
If we are currently releasing from `release-0.6` then you should create the branch `release-0.7`. Since this branch is
pulled directly from main, there is nothing for you to cherry-pick. Simply follow the following instructions to [create a release from the release branch](#creating-a-release-from-the-release-branch)

# Handling Major releases

### When should I create a major release?

A major release should be carefully considered, and we should make all attempts to not break functionality when possible.
However, there is a reality that we sometimes need to remove features to keep the project forward facing. Please take care to give users
ample time with deprecation warnings and migration steps before removing a feature.

### Creating a minor release

The instructions for creating a major release are identical for those of [creating a minor release](###creating-a-minor-release).

# Creating a release from the release branch

Once your release branch is ready to go there are a few simple steps to actually release the project to pypi.

The first step is to go to [a relative link](src/astro/__init__.py) and change the `__version__` variable to the new version. You can then
push this change to main or create a PR depending on your level of permission within the project.

The second step is to [create a release](https://docs.github.com/en/repositories/releasing-projects-on-github/managing-releases-in-a-repository)
in GitHub. Please take extra care to ensure that you a) create a tag with the new release version (e.g. `0.7.0`) and that you
target the release branch (e.g. `release-0.7`). Failure to do these steps properly could result in a release we will later need to yank.

Once you've created your release, that's it! The rest of the steps should be handled by the CI system.