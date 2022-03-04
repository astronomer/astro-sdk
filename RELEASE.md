# Managing Releases


## Handling patch releases

### When should I create a patch release?
Patch releases are exclusively for bugs and and security fixes. Patch releases can have minor _enhancements_ to 
existing features but really should not affect the API in any noticeable way (unless it is adding an argument). For any
functionality changes or new endpoints, please create a minor or major release.

A patch release should increment the third digit e.g. `0.6.9 -> 0.6.10`. This indicates to users that the release 


### Creating a patch release

#### Cherry-picking all necessary patches
When looking through the branches on the main repo, you will notice a series of `release` branches labeled
`release-<major version>.<minor version>`. So if we are currently working on the 0.6 release, the branch should be labeled 
`release-0.6`. Before creating a minor release please check the [milestones](https://github.com/astro-projects/astro/milestones)
page to ensure that all relevent bug fixes have been PRed and merged. 

Once you merge all expected fixes, the next step is to ensure that all fixes have been cherry-picked to the release branch from main (please use `git cherry-pick -x <commit id>` to retain
git commit hashes and messages). Depending on the fix there might be some git conflicts to resolve. If you run into conflicts, please 
resolve said conflicts, run `git add .`, and then `git cherry-pick --continue` to continue the merge.

#### Creating a release from the release branch

# Handling minor releases
### When should I create a minor release?

### Creating a minor release

To create a minor release, first create a new release branch based on main under the new minor release number. 
If we are currently releasing from `release-0.6` then you should create the branch `release-0.7`. 
You can then follow the steps laid out [here](####creating-a-release-from-the-release-branch)

# Handling Major releases

### When should I create a major release?
